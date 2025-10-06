require("dotenv").config({ path: "../.env" });
const functions = require("@google-cloud/functions-framework");
const { BigQuery } = require("@google-cloud/bigquery");
const { PubSub } = require("@google-cloud/pubsub");

const bigquery = new BigQuery();
const pubsub = new PubSub();

const TOPIC_NAME = process.env.PUBSUB_TOPIC_ID || "process-gclid";

/**
 * HTTP Cloud Function.
 * This function is triggered by an HTTP request.
 *
 * @param {Object} req Cloud Function request context.
 * @param {Object} res Cloud Function response context.
 */
functions.http("amplitudeController", async (req, res) => {
  if (req.method !== "POST") {
    return res.status(405).send("Method Not Allowed");
  }

  console.log("Received request from Amplitude:");
  console.log(JSON.stringify(req.body, null, 2));

  // Placeholder: In the future, you would extract a key from the payload.
  // const { gclid, accountId } = req.body;
  const accountId = "placeholder-account-123";

  try {
    // 1. Simulate querying BigQuery for settings
    console.log(`Simulating BQ query for account: ${accountId}`);
    // In the real implementation, you would query BQ like this:
    // const query = `SELECT * FROM \`my-project.my_dataset.my_table\` WHERE account_id = @accountId`;
    // const options = { query: query, params: { accountId: accountId } };
    // const [rows] = await bigquery.query(options);
    // console.log('BQ Settings:', rows);
    const settings = { conversion_action_id: "AW-12345/abcdef" }; // Dummy settings

    // 2. Prepare message for Pub/Sub
    const messagePayload = {
      gclid: req.body.gclid || "test-gclid",
      settings: settings,
    };
    const dataBuffer = Buffer.from(JSON.stringify(messagePayload));

    // 3. Publish message to Pub/Sub
    const messageId = await pubsub
      .topic(TOPIC_NAME)
      .publishMessage({ data: dataBuffer });
    console.log(`Message ${messageId} published to topic ${TOPIC_NAME}.`);

    res.status(200).send(`Successfully published message ID: ${messageId}`);
  } catch (error) {
    console.error("Error processing request:", error);
    res.status(500).send("Internal Server Error");
  }
});
