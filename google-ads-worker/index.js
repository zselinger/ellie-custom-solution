require("dotenv").config({ path: "../.env" });
const functions = require("@google-cloud/functions-framework");
const { uploadClickConversion } = require("./googleAdsClient");

/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is triggered when a message is published to the topic.
 *
 * @param {object} message The Pub/Sub message.
 * @param {object} context The event metadata.
 */
functions.cloudEvent("googleAdsWorker", async (cloudEvent) => {
  console.log(
    JSON.stringify({
      message: "google-ads-worker execution started.",
      severity: "INFO",
    })
  );

  let event_type = "unknown";

  try {
    // The Pub/Sub message is passed as the cloudEvent.data.message property.
    const base64data = cloudEvent.data.message.data;
    const messageData = base64data
      ? JSON.parse(Buffer.from(base64data, "base64").toString())
      : {};

    const { gclid, conversion_actions } = messageData;
    event_type = messageData.event_type || "unknown";

    if (!conversion_actions || !gclid) {
      console.error(
        JSON.stringify({
          message: "Invalid message format received.",
          severity: "ERROR",
          payload: messageData,
        })
      );
      return;
    }

    // 1. Group conversion actions by customer_id for efficiency.
    const customerConversionsMap = new Map();
    for (const action of messageData.conversion_actions) {
      if (!customerConversionsMap.has(action.customer_id)) {
        customerConversionsMap.set(action.customer_id, []);
      }
      customerConversionsMap.get(action.customer_id).push(action);
    }

    // 2. Create an array of upload promises to be executed in parallel.
    const uploadPromises = [];
    for (const [customerId, actions] of customerConversionsMap.entries()) {
      uploadPromises.push(
        uploadClickConversion(customerId, actions, messageData.gclid)
      );
    }

    // 3. Execute all uploads concurrently and wait for the first success.
    await Promise.any(uploadPromises);

    // If Promise.any() resolves, it means at least one upload succeeded.
    // If all promises reject, Promise.any() will throw an AggregateError,
    // which will be caught by the main catch block.

    console.log(
      JSON.stringify({
        message:
          "google-ads-worker execution finished successfully (at least one upload succeeded).",
        severity: "INFO",
        gclid: messageData.gclid,
        event_type: event_type,
      })
    );
  } catch (error) {
    let errorMessage = "google-ads-worker execution failed.";
    // Check if it's an AggregateError from Promise.any(), which means all uploads failed.
    if (error instanceof AggregateError) {
      errorMessage =
        "All click conversion uploads failed for the provided GCLID.";
    }

    console.error(
      JSON.stringify({
        message: errorMessage,
        severity: "ERROR",
        error: error.message,
        stack: error.stack,
        event_type: event_type,
        // Optionally log individual errors if they are useful
        ...(error.errors && { individualErrors: error.errors }),
      })
    );

    // Re-throw the error to ensure the Cloud Function is marked as failed
    throw error;
  }
});
