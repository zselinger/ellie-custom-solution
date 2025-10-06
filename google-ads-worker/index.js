require("dotenv").config({ path: "../.env" });
const functions = require("@google-cloud/functions-framework");

/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is triggered when a message is published to the topic.
 *
 * @param {object} message The Pub/Sub message.
 * @param {object} context The event metadata.
 */
functions.cloudEvent("googleAdsWorker", (cloudEvent) => {
  // The Pub/Sub message is passed as the cloudEvent.data.message property.
  const base64data = cloudEvent.data.message.data;
  const messageData = base64data
    ? JSON.parse(Buffer.from(base64data, "base64").toString())
    : {};

  console.log("Received message from Pub/Sub:");
  console.log(JSON.stringify(messageData, null, 2));

  const { gclid, settings } = messageData;

  if (!gclid || !settings) {
    console.error("Missing gclid or settings in message.");
    return;
  }

  // Placeholder: In the future, you will use the google-ads-api library here
  console.log(`Simulating conversion upload for GCLID: ${gclid}`);
  console.log(`Using settings: ${JSON.stringify(settings)}`);
  console.log("Conversion upload successful (simulation).");

  // No return value is needed for background functions.
});
