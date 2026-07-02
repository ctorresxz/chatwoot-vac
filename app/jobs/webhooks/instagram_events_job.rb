class Webhooks::InstagramEventsJob < MutexApplicationJob
  queue_as :default
  # This lock is only a short race dampener for first-message conversation creation.
  # ContactInbox creation is already protected by a unique index, but conversation
  # lookup is `find active conversation || create`, so concurrent first messages from
  # the same IG contact can create duplicate conversations.
  #
  # ActiveJob retries are not FIFO, so a longer retry window does not preserve message
  # order. Use deterministic backoff so the final attempt happens after the 3s lock TTL,
  # then process without the lock instead of dropping the webhook.
  retry_on_lock_conflict wait: ->(executions) { executions.seconds }, attempts: 3, on_exhaustion: :process_without_lock

  # Only process real message events for now.
  # Instagram also sends read receipts, message edits and other events that may not
  # include sender/recipient, so processing them here can break conversation creation.
  SUPPORTED_EVENTS = [:message].freeze

  def perform(entries)
    @entries = entries

    key = format(::Redis::Alfred::IG_MESSAGE_MUTEX, sender_id: contact_instagram_id, ig_account_id: ig_account_id)
    # Keep the lock TTL just long enough for the first job to fetch profile data and
    # create the contact/conversation. A longer TTL would add user-visible latency for
    # hot contacts without giving us ordering guarantees.
    with_lock(key, 3.seconds) do
      process_entries(entries)
    end
  end

  def process_without_lock(entries)
    Rails.logger.warn("[#{self.class.name}] Processing without lock after lock retry exhaustion")
    process_entries(entries)
  end

  # https://developers.facebook.com/docs/messenger-platform/instagram/features/webhook
  def process_entries(entries)
    entries.each do |entry|
      process_single_entry(entry.with_indifferent_access)
    end
  end

  private

  def process_single_entry(entry)
    if test_event?(entry)
      process_test_event(entry)
      return
    end

    process_messages(entry)
  end

  def process_messages(entry)
    messages(entry).each do |messaging|
      messaging = messaging.with_indifferent_access

      Rails.logger.info("Instagram Events Job Messaging: #{messaging}")

      next unless processable_event?(messaging)

      instagram_id = instagram_id(messaging)
      next if instagram_id.blank?

      channel = find_channel(instagram_id)
      next if channel.blank?

      if (event_name = event_name(messaging))
        send(event_name, messaging, channel)
      end
    end
  end

  def processable_event?(messaging)
    return false if messaging.blank?

    # Ignore Instagram events that do not create or update a conversation.
    # Examples: read receipts, message edits, delivery receipts.
    return false if messaging[:read].present?
    return false if messaging[:message_edit].present?
    return false if messaging[:delivery].present?

    messaging[:message].present?
  end

  def agent_message_via_echo?(messaging)
    messaging[:message].present? && messaging[:message][:is_echo].present?
  end

  def test_event?(entry)
    entry[:changes].present?
  end

  def process_test_event(entry)
    messaging = extract_messaging_from_test_event(entry)

    Instagram::TestEventService.new(messaging).perform if messaging.present?
  end

  def extract_messaging_from_test_event(entry)
    entry[:changes].first&.dig(:value) if entry[:changes].present?
  end

  def instagram_id(messaging)
    if agent_message_via_echo?(messaging)
      messaging.dig(:sender, :id)
    else
      messaging.dig(:recipient, :id)
    end
  end

  def ig_account_id
    @entries&.first&.dig(:id)
  end

  def contact_instagram_id
    entry = @entries&.first
    return nil unless entry

    # Handle both messaging and standby arrays
    messaging = (entry[:messaging].presence || entry[:standby] || []).first
    return nil unless messaging

    messaging = messaging.with_indifferent_access

    # For echo messages (outgoing from our account), use recipient's ID (the contact)
    # For incoming messages (from contact), use sender's ID (the contact)
    if messaging.dig(:message, :is_echo)
      messaging.dig(:recipient, :id)
    else
      messaging.dig(:sender, :id)
    end
  end

  def sender_id
    @entries&.dig(0, :messaging, 0, :sender, :id)
  end

  def find_channel(instagram_id)
    # There will be chances for the instagram account to be connected to a facebook page,
    # so we need to check for both instagram and facebook page channels.
    # Priority is for instagram channel which was created via instagram login.
    channel = Channel::Instagram.find_by(instagram_id: instagram_id)
    # If not found, fallback to the facebook page channel.
    channel ||= Channel::FacebookPage.find_by(instagram_id: instagram_id)

    channel
  end

  def event_name(messaging)
    SUPPORTED_EVENTS.find { |key| messaging.key?(key) }
  end

  def message(messaging, channel)
    if channel.is_a?(Channel::Instagram)
      ::Instagram::MessageText.new(messaging, channel).perform
    else
      ::Instagram::Messenger::MessageText.new(messaging, channel).perform
    end
  end

  def read(messaging, channel)
    # Kept for compatibility if read events are re-enabled later.
    # Currently read events are intentionally ignored because Instagram may send
    # them without sender/recipient fields, which breaks instagram_id resolution.
    ::Instagram::ReadStatusService.new(params: messaging, channel: channel).perform
  end

  def messages(entry)
    (entry[:messaging].presence || entry[:standby] || [])
  end
end

# Actual response from Instagram webhook (both via Facebook page and Instagram direct)
# [
#   {
#     "time": <timestamp>,
#     "id": <INSTAGRAM_USER_ID>,
#     "messaging": [
#       {
#         "sender": {
#           "id": <INSTAGRAM_USER_ID>
#         },
#         "recipient": {
#           "id": <INSTAGRAM_USER_ID>
#         },
#         "timestamp": <timestamp>,
#         "message": {
#           "mid": <MESSAGE_ID>,
#           "text": <MESSAGE_TEXT>
#         }
#       }
#     ]
#   }
# ]

# Instagram's webhook via Instagram direct testing quirk: Test payloads vs Actual payloads
# When testing in Facebook's developer dashboard, you'll get a Page-style
# payload with a "changes" object. But don't be fooled! Real Instagram DMs
# arrive in the familiar Messenger format with a "messaging" array.
# This apparent inconsistency is actually by design - Instagram's webhooks
# use different formats for testing vs production to maintain compatibility
# with both Instagram Direct and Facebook Page integrations.
# See: https://developers.facebook.com/docs/instagram-platform/webhooks#event-notifications

# Test response from via Instagram direct
# [
#   {
#     "id": "0",
#     "time": <timestamp>,
#     "changes": [
#       {
#         "field": "messages",
#         "value": {
#           "sender": {
#             "id": "12334"
#           },
#           "recipient": {
#             "id": "23245"
#           },
#           "timestamp": "1527459824",
#           "message": {
#             "mid": "random_mid",
#             "text": "random_text"
#           }
#         }
#       }
#     ]
#   }
# ]

# Test response via Facebook page
# [
#   {
#     "time": <timestamp>,
#     "id": "0",
#     "messaging": [
#       {
#         "sender": {
#           "id": "12334"
#         },
#         "recipient": {
#           "id": "23245"
#         },
#         "timestamp": <timestamp>,
#         "message": {
#           "mid": "random_mid",
#           "text": "random_text"
#         }
#       }
#     ]
#   }
# ]
