require("dotenv").config({ path: "../.env" });
const functions = require("@google-cloud/functions-framework");
const {
  uploadClickConversion,
} = require("../amplitude-controller/googleAdsClient");

/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is triggered when a message is published to the topic.
 *
 * @param {object} message The Pub/Sub message.
 * @param {object} context The event metadata.
 */
functions.cloudEvent("googleAdsWorker", async (cloudEvent) => {
  // The Pub/Sub message is passed as the cloudEvent.data.message property.
  //const base64data = cloudEvent.data.message.data;
  //const messageData = base64data
  //  ? JSON.parse(Buffer.from(base64data, "base64").toString())
  //  : {};

  let messageData = {
    gclid:
      "Cj0KCQjw6bfHBhDNARIsAIGsqLg25SvbEHHw5ZhMJNqRuWYz8shMN5DaapJa0UOJvCwXVzNEG7QL2v4aAt2eEALw_wcB",
    conversion_actions: [
      {
        timestamp: { value: "2025-10-15T01:22:04.358Z" },
        customer_id: "185-069-2321",
        conversion_action_id: "5464",
        events: "Irvin Yalom",
      },
      {
        timestamp: { value: "2025-10-15T01:22:04.358Z" },
        customer_id: "372-699-8268",
        conversion_action_id: "7217059813",
        events: "Sigmund Freud, Marsh Linehan, Irvin Yalom",
      },
      {
        timestamp: { value: "2025-10-15T01:22:04.358Z" },
        customer_id: "628-809-9044",
        conversion_action_id: "4645464",
        events: "Jean Piaget, Irvin Yalom",
      },
      {
        timestamp: { value: "2025-10-15T01:22:04.358Z" },
        customer_id: "571-852-3592",
        conversion_action_id: "114465545",
        events: "Virginia Satir, Irvin Yalom",
      },
      {
        timestamp: { value: "2025-10-15T01:22:04.358Z" },
        customer_id: "571-852-3592",
        conversion_action_id: "474644",
        events: "Irvin Yalom",
      },
    ],
  };

  console.log("Received message from Pub/Sub:");
  console.log(JSON.stringify(messageData, null, 2));

  if (!messageData.conversion_actions || !messageData.gclid) {
    console.error("Invalid message format received:", messageData);
    return;
  }

  const customers = new Set(
    messageData.conversion_actions.map(
      (conversion_action) => conversion_action.customer_id
    )
  );

  console.log("Customers:", Array.from(customers));

  let anyUploadSucceeded = false;
  for (const customerId of customers) {
    const customerConversionActions = messageData.conversion_actions.filter(
      (action) => action.customer_id === customerId
    );
    try {
      await uploadClickConversion(
        customerId,
        customerConversionActions,
        messageData.gclid
      );
      // Mark as success if the API accepts the call for this customer.
      // We don't stop, in case other conversions need to be processed for other accounts.
      anyUploadSucceeded = true;
    } catch (error) {
      console.error(
        `Failed to process conversions for customer ${customerId}:`,
        error.message
      );
    }
  }

  if (!anyUploadSucceeded) {
    throw new Error(
      "Failed to upload click conversion for GCLID to any of the provided accounts."
    );
  }
});
