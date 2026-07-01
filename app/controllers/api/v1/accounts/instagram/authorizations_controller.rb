class Api::V1::Accounts::Instagram::AuthorizationsController < Api::V1::Accounts::OauthAuthorizationController
  include InstagramConcern
  include Instagram::IntegrationHelper

  def create
    redirect_uri = "#{base_url}/instagram/callback"

    Rails.logger.info("[Instagram OAuth Start] redirect_uri=#{redirect_uri}")
    Rails.logger.info("[Instagram OAuth Start] base_url=#{base_url}")
    Rails.logger.info("[Instagram OAuth Start] scope=#{REQUIRED_SCOPES.join(',')}")

    redirect_url = instagram_client.auth_code.authorize_url(
      {
        redirect_uri: redirect_uri,
        scope: REQUIRED_SCOPES.join(','),
        force_reauth: 'true',
        response_type: 'code',
        state: generate_instagram_token(Current.account.id, params[:return_to])
      }
    )

    Rails.logger.info("[Instagram OAuth Start] redirect_url=#{redirect_url}")

    if redirect_url
      render json: { success: true, url: redirect_url }
    else
      render json: { success: false }, status: :unprocessable_entity
    end
  end
end
