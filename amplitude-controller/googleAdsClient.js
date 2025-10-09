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

/**
 * Retrieves the Google Ads account ID for a given GCLID using the REST API.
 *
 * @param {string} gclid The Google Click ID.
 * @returns {Promise<string|null>} The account ID or null if not found.
 */
async function getAccountFromGclid(gclid) {
  const loginCustomerId = process.env.GADS_LOGIN_CUSTOMER_ID;
  if (!loginCustomerId) {
    throw new Error("Missing GADS_LOGIN_CUSTOMER_ID environment variable.");
  }

  const accessToken = await refreshAccessToken();
  const developerToken = process.env.GADS_DEVELOPER_TOKEN;

  console.log(`AccessToken: ${accessToken}`);
  console.log(`Developer Token: ${developerToken}`);
  console.log(`Login Customer ID: ${loginCustomerId}`);

  const url = `${GADS_API_URL}/${loginCustomerId}/googleAds:searchStream`;

  const query = `
    SELECT customer.id
    FROM click_view
    WHERE click_view.gclid = '${gclid}'
  `;

  const config = {
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "developer-token": developerToken,
      "login-customer-id": loginCustomerId,
      "Content-Type": "application/json",
    },
  };

  const data = {
    query,
  };

  try {
    const response = await axios.post(url, data, config);
    if (
      response.data.results &&
      response.data.results.length > 0 &&
      response.data.results[0].customer
    ) {
      return response.data.results[0].customer.id.toString();
    }
    return null;
  } catch (error) {
    console.error(
      "Error fetching from Google Ads REST API:",
      error.response ? error.response.data : error.message
    );
    console.log(error.response.data[0].error.details[0].errors);
    // Optionally, inspect error.response.data for specific Google Ads API errors
    if (error.response && error.response.data && error.response.data.error) {
      console.error(
        "Google Ads API Error:",
        JSON.stringify(error.response.data.error, null, 2)
      );
    }
    throw new Error("Failed to fetch account from GCLID.");
  }
}

module.exports = { getAccountFromGclid };
