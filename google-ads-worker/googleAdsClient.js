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
 * Uploads a click conversion to Google Ads.
 * @param {string} customerId - The ID of the customer account.
 * @param {Array<object>} clicks - An array of click objects, each with a gclid and conversionActions.
 * @returns {Promise<object>} The response from the Google Ads API.
 */
async function uploadClickConversion(customerId, clicks) {
  if (!customerId || !clicks || clicks.length === 0) {
    throw new Error("Missing required parameters for conversion upload.");
  }

  const developerToken = process.env.GADS_DEVELOPER_TOKEN;
  const loginCustomerId = process.env.GADS_LOGIN_CUSTOMER_ID;

  if (!loginCustomerId || !developerToken) {
    throw new Error(
      "Missing Google Ads manager account credentials or developer token."
    );
  }

  try {
    const accessToken = await refreshAccessToken();
    const customerIdFormatted = customerId.replace(/-/g, "");
    const url = `${GADS_API_URL}/${customerIdFormatted}:uploadClickConversions`;

    const conversions = clicks.flatMap((click) => {
      const { gclid, conversion_actions } = click;
      if (!gclid || !conversion_actions || conversion_actions.length === 0) {
        // Skip invalid click objects
        return [];
      }
      return conversion_actions.map((conv) => {
        const dt = new Date();
        const conversionDateTime = `${dt.getFullYear()}-${String(
          dt.getMonth() + 1
        ).padStart(2, "0")}-${String(dt.getDate()).padStart(2, "0")} ${String(
          dt.getHours()
        ).padStart(2, "0")}:${String(dt.getMinutes()).padStart(
          2,
          "0"
        )}:${String(dt.getSeconds()).padStart(2, "0")}+00:00`;

        return {
          gclid,
          conversionAction: `customers/${customerIdFormatted}/conversionActions/${conv.conversion_action_id}`,
          conversionDateTime,
        };
      });
    });

    if (conversions.length === 0) {
      console.log(`No valid conversions to upload for customer ${customerId}.`);
      return;
    }

    const payload = {
      conversions,
      partialFailure: true,
      validateOnly: false, // TODO: Remove this after testing
    };

    const response = await axios.post(url, payload, {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${accessToken}`,
        "developer-token": developerToken,
        "login-customer-id": loginCustomerId.replace(/-/g, ""),
      },
    });

    console.log(
      `Upload response for customer ${customerId}:`,
      JSON.stringify(response.data, null, 2)
    );

    const { partialFailureError, results } = response.data;

    // If there's a partial failure error, log it as a warning but do not throw.
    // This is an expected outcome when some GCLIDs are invalid for a customer account.
    if (partialFailureError) {
      console.warn(
        `Partial failure for customer ${customerId}: ${JSON.stringify(
          partialFailureError
        )}`
      );
    }

    // A successful response for a valid gclid should contain results.
    // If no results, it means no conversions were successfully processed.
    if (!results || results.length === 0) {
      console.log(
        `No conversions were successfully uploaded for customer ${customerId}. This may be expected.`
      );
    } else {
      console.log(
        `Successfully uploaded ${results.length} click conversion(s) for customer ${customerId}.`
      );
    }

    return response.data;
  } catch (error) {
    console.error(
      `Failed to upload click conversion for customer ${customerId}:`,
      error.response
        ? JSON.stringify(error.response.data, null, 2)
        : error.message
    );
    throw error;
  }
}

module.exports = { refreshAccessToken, uploadClickConversion };
