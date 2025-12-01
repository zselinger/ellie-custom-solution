const functions = require("@google-cloud/functions-framework");
const { SubscriberClient } = require("@google-cloud/pubsub").v1;
const { uploadClickConversion } = require("./googleAdsClient");

const subscriberClient = new SubscriberClient();
const MAX_MESSAGES = 50;

/**
 * HTTP Cloud Function to be triggered by Cloud Scheduler.
 * It pulls messages from a Pub/Sub subscription, batches them by customer,
 * and sends them to the Google Ads API.
 *
 * @param {object} req The request object.
 * @param {object} res The response object.
 */
functions.http("googleAdsWorker", async (req, res) => {
  console.log(
    JSON.stringify({
      message: "google-ads-worker execution started.",
      severity: "INFO",
      payload: req.body,
    })
  );

  const subscriptionName = process.env.PUBSUB_SUBSCRIPTION_ID;
  const projectId = process.env.GCP_PROJECT_ID;

  if (!subscriptionName || !projectId) {
    console.error(
      JSON.stringify({
        message:
          "PUBSUB_SUBSCRIPTION_ID or GCP_PROJECT_ID environment variables not set.",
        severity: "CRITICAL",
      })
    );
    res.status(500).send("Server configuration error.");
    return;
  }

  let ackIds = [];

  try {
    const formattedSubscription = subscriberClient.subscriptionPath(
      projectId,
      subscriptionName
    );
    const request = {
      subscription: formattedSubscription,
      maxMessages: MAX_MESSAGES,
    };

    const [response] = await subscriberClient.pull(request);
    const messages = response.receivedMessages;

    if (messages.length === 0) {
      console.log(
        JSON.stringify({
          message: "No messages to process.",
          severity: "INFO",
        })
      );
      res.status(200).send("No messages to process.");
      return;
    }

    ackIds = messages.map((message) => message.ackId);
    console.log(`Received ${messages.length} messages.`);

    // 1. Group clicks by customer_id for efficiency.
    const customerClicksMap = new Map();
    for (const message of messages) {
      const messageData = JSON.parse(message.message.data.toString());
      const { gclid, conversion_actions } = messageData;

      if (!conversion_actions || !gclid) {
        console.error(
          JSON.stringify({
            message: "Invalid message format received.",
            severity: "ERROR",
            payload: messageData,
          })
        );
        // This message will be acked without processing.
        // Alternatively, you could push it to a dead-letter queue.
        continue;
      }

      for (const action of conversion_actions) {
        if (!customerClicksMap.has(action.customer_id)) {
          customerClicksMap.set(action.customer_id, []);
        }
        // Group all clicks for a customer. A click is a gclid with its actions.
        const customerClicks = customerClicksMap.get(action.customer_id);
        const existingClick = customerClicks.find((c) => c.gclid === gclid);
        if (existingClick) {
          // Only add the action if it's not already in the list for this click.
          const actionExists = existingClick.conversion_actions.some(
            (existingAction) =>
              existingAction.conversion_action_id ===
              action.conversion_action_id
          );
          if (!actionExists) {
            existingClick.conversion_actions.push(action);
          }
        } else {
          customerClicks.push({ gclid, conversion_actions: [action] });
        }
      }
    }

    // 2. Create an array of upload promises to be executed in parallel.
    const uploadPromises = [];
    for (const [customerId, clicks] of customerClicksMap.entries()) {
      uploadPromises.push(uploadClickConversion(customerId, clicks));
    }

    // 3. Execute all uploads concurrently.
    await Promise.all(uploadPromises);

    console.log(
      JSON.stringify({
        message: "google-ads-worker batch processing finished.",
        severity: "INFO",
      })
    );
    res.status(200).send("Batch processing complete.");
  } catch (error) {
    console.error(
      JSON.stringify({
        message: "google-ads-worker execution failed during API call.",
        severity: "ERROR",
        error: error.message,
        stack: error.stack,
      })
    );

    const errorMessage = error.response?.data
      ? JSON.stringify(error.response.data, null, 2)
      : "An error occurred during processing.";
    const errorStatus = error.response?.status || 500;

    res.status(errorStatus).send(errorMessage);
  } finally {
    // 4. Acknowledge messages from Pub/Sub under all conditions to prevent replays.
    if (ackIds.length > 0) {
      try {
        const formattedSubscription = subscriberClient.subscriptionPath(
          projectId,
          subscriptionName
        );
        await subscriberClient.acknowledge({
          subscription: formattedSubscription,
          ackIds,
        });
        console.log(`Acknowledged ${ackIds.length} messages.`);
      } catch (ackError) {
        console.error(
          JSON.stringify({
            message: "Failed to acknowledge Pub/Sub messages.",
            severity: "CRITICAL",
            error: ackError.message,
            stack: ackError.stack,
          })
        );
      }
    }
  }
});
