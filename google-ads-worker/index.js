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
  // The Pub/Sub message is passed as the cloudEvent.data.message property.
  const base64data = cloudEvent.data.message.data;
  const messageData = base64data
    ? JSON.parse(Buffer.from(base64data, "base64").toString())
    : {};

  //let messageData = {
  //  gclid:
  //    "Cj0KCQjw6bfHBhDNARIsAIGsqLg25SvbEHHw5ZhMJNqRuWYz8shMN5DaapJa0UOJvCwXVzNEG7QL2v4aAt2eEALw_wcB",
  //  conversion_actions: [
  //    {
  //      timestamp: { value: "2025-10-15T01:22:04.358Z" },
  //      customer_id: "185-069-2321",
  //      conversion_action_id: "5464",
  //      events: "Irvin Yalom",
  //    },
  //    {
  //      timestamp: { value: "2025-10-15T01:22:04.358Z" },
  //      customer_id: "372-699-8268",
  //      conversion_action_id: "7217059813",
  //      events: "Sigmund Freud, Marsh Linehan, Irvin Yalom",
  //    },
  //    {
  //      timestamp: { value: "2025-10-15T01:22:04.358Z" },
  //      customer_id: "628-809-9044",
  //      conversion_action_id: "4645464",
  //      events: "Jean Piaget, Irvin Yalom",
  //    },
  //    {
  //      timestamp: { value: "2025-10-15T01:22:04.358Z" },
  //      customer_id: "571-852-3592",
  //      conversion_action_id: "114465545",
  //      events: "Virginia Satir, Irvin Yalom",
  //    },
  //    {
  //      timestamp: { value: "2025-10-15T01:22:04.358Z" },
  //      customer_id: "571-852-3592",
  //      conversion_action_id: "474644",
  //      events: "Irvin Yalom",
  //    },
  //  ],
  //};

  console.log("Received message from Pub/Sub:");
  console.log(JSON.stringify(messageData, null, 2));

  if (!messageData.conversion_actions || !messageData.gclid) {
    console.error("Invalid message format received:", messageData);
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

  console.log(
    "Processing conversions for customers:",
    Array.from(customerConversionsMap.keys())
  );

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

  results.forEach((result, index) => {
    if (result.status === "rejected") {
      const customerId = Array.from(customerConversionsMap.keys())[index];
      console.error(
        `Failed to process conversions for customer ${customerId}:`,
        result.reason.message
      );
    }
  });

  if (!anyUploadSucceeded) {
    throw new Error(
      "Failed to upload click conversion for GCLID to any of the provided accounts."
    );
  }
});
