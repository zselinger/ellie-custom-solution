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

  const { event_type, event_properties } = req.body;

  if (!event_properties.gclid) {
    return res.status(400).send("Missing gclid in request body");
  }

  try {
    const datasetId = process.env.BIGQUERY_DATASET;
    const tableId = process.env.BIGQUERY_TABLE;

    if (!projectId || !datasetId || !tableId) {
      throw new Error(
        "Missing required environment variables for BigQuery connection."
      );
    }
    // 2. Query BigQuery for settings
    console.log(`Querying BigQuery for all settings for event: ${event_type}`);
    const query = `SELECT * FROM \`${projectId}.${datasetId}.${tableId}\` WHERE REGEXP_CONTAINS(events, CONCAT('\\\\b', @eventType, '\\\\b'))`;
    const options = { query, params: { eventType: event_type } };
    const [rows] = await bigquery.query(options);
    console.log("BQ Settings:", rows);

    if (rows.length === 0) {
      console.log(`No settings found for event: ${event_type}`);
      res.status(200).send(`No settings found for event: ${event_type}`);
      return;
    }

    // 3. Prepare message for Pub/Sub
    const dataBuffer = Buffer.from(
      JSON.stringify({ gclid: event_properties.gclid, conversion_action: rows })
    );

    // 4. Publish message to Pub/Sub
    const messageId = await pubsub
      .topic(TOPIC_NAME)
      .publishMessage({ data: dataBuffer });
    console.log(`Message ${messageId} published to topic ${TOPIC_NAME}.`);

    res.status(200).send(`Successfully published message ID: ${messageId}`);
  } catch (error) {
    console.error("ERROR:", error);
    res.status(500).send("Internal Server Error");
  }
});
