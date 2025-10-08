require("dotenv").config({ path: "../.env" });
const functions = require("@google-cloud/functions-framework");
const { BigQuery } = require("@google-cloud/bigquery");
const { PubSub } = require("@google-cloud/pubsub");

const projectId = process.env.GCP_PROJECT_ID;

const bigquery = new BigQuery({ projectId });
const pubsub = new PubSub({ projectId });

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

  const { gclid, accountId } = req.body;

  try {
    const datasetId = process.env.BIGQUERY_DATASET;
    const tableId = process.env.BIGQUERY_TABLE;

    if (!projectId || !datasetId || !tableId) {
      throw new Error(
        "Missing required environment variables for BigQuery connection."
      );
    }
    // 1. Query BigQuery for settings
    console.log(`Querying BigQuery for account: ${accountId}`);
    const query = `SELECT * FROM \`${projectId}.${datasetId}.${tableId}\` WHERE account_id = @accountId LIMIT 1`;
    const options = { query: query, params: { accountId: accountId } };
    const [rows] = await bigquery.query(options);

    if (rows.length === 0) {
      console.log(`No settings found for account: ${accountId}`);
      res.status(404).send(`No settings found for account: ${accountId}`);
      return;
    }
    const settings = rows[0];
    console.log("BQ Settings:", settings);

    // 2. Prepare message for Pub/Sub
    //const messagePayload = {
    //  gclid: req.body.gclid || "test-gclid",
    //  settings: settings,
    //};
    //const dataBuffer = Buffer.from(JSON.stringify(messagePayload));

    //// 3. Publish message to Pub/Sub
    //const messageId = await pubsub
    //  .topic(TOPIC_NAME)
    //  .publishMessage({ data: dataBuffer });
    //console.log(`Message ${messageId} published to topic ${TOPIC_NAME}.`);

    //res.status(200).send(`Successfully published message ID: ${messageId}`);
    res.status(200).send(`Successfully processed request`);
  } catch (error) {
    console.error("Error processing request:", error);
    res.status(500).send("Internal Server Error");
  }
});
