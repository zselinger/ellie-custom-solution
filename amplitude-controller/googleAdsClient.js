const axios = require("axios");

const GADS_API_URL = "https://googleads.googleapis.com/v21/customers";
const OAUTH2_TOKEN_URL = "https://www.googleapis.com/oauth2/v4/token";

/**
 * Refreshes the OAuth2 access token using the refresh token.
 *
 * @returns {Promise<string>} The access token.
 */
async function refreshAccessToken() {
  try {
    const response = await axios.post(OAUTH2_TOKEN_URL, {
      client_id: process.env.GADS_CLIENT_ID,
      client_secret: process.env.GADS_CLIENT_SECRET,
      refresh_token: process.env.GADS_REFRESH_TOKEN,
      grant_type: "refresh_token",
    });
    return response.data.access_token;
  } catch (error) {
    console.error(
      "Error refreshing Google Ads API access token:",
      error.response ? error.response.data : error.message
    );
    throw new Error("Failed to refresh access token.");
  }
}

module.exports = { refreshAccessToken };
