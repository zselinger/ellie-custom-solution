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

  try {
    // The Pub/Sub message is passed as the cloudEvent.data.message property.
    const base64data = cloudEvent.data.message.data;
    const messageData = base64data
      ? JSON.parse(Buffer.from(base64data, "base64").toString())
      : {};

    if (!messageData.conversion_actions || !messageData.gclid) {
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

    // 3. Execute all uploads concurrently and wait for them to settle.
    const results = await Promise.allSettled(uploadPromises);

    // 4. Check if at least one upload was successful.
    const anyUploadSucceeded = results.some(
      (result) => result.status === "fulfilled"
    );

    const failedUploads = [];
    results.forEach((result, index) => {
      if (result.status === "rejected") {
        const customerId = Array.from(customerConversionsMap.keys())[index];
        const failure = {
          customerId,
          reason: result.reason.message,
        };
        failedUploads.push(failure);
        console.warn(
          JSON.stringify({
            message: `Failed to process conversions for customer ${customerId}`,
            severity: "WARNING",
            gclid: messageData.gclid,
            ...failure,
          })
        );
      }
    });

    if (!anyUploadSucceeded) {
      throw new Error(
        "Failed to upload click conversion for GCLID to any of the provided accounts."
      );
    }

    console.log(
      JSON.stringify({
        message: "google-ads-worker execution finished successfully.",
        severity: "INFO",
        gclid: messageData.gclid,
        processedConversions: messageData.conversion_actions.length,
        successfulUploads: results.filter((r) => r.status === "fulfilled")
          .length,
        failedUploads: failedUploads.length,
      })
    );
  } catch (error) {
    console.error(
      JSON.stringify({
        message: "google-ads-worker execution failed.",
        severity: "ERROR",
        error: error.message,
        stack: error.stack,
      })
    );
    // Re-throw the error to ensure the Cloud Function is marked as failed
    throw error;
  }
});
