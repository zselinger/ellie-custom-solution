require('dotenv').config({ path: '../.env' });
const functions = require('@google-cloud/functions-framework');
const { BigQuery } = require('@google-cloud/bigquery');
const { PubSub } = require('@google-cloud/pubsub');

const projectId = process.env.GCP_PROJECT_ID;

const bigquery = new BigQuery({ projectId });
const pubsub = new PubSub({ projectId });

const TOPIC_NAME = process.env.PUBSUB_TOPIC_ID || 'process-gclid';

/**
 * HTTP Cloud Function.
 * This function is triggered by an HTTP request.
 *
 * @param {Object} req Cloud Function request context.
 * @param {Object} res Cloud Function response context.
 */
functions.http('amplitudeController', async (req, res) => {
  console.log(
    JSON.stringify({
      message: 'amplitude-controller execution started.',
      severity: 'INFO',
    })
  );

  if (req.method !== 'POST') {
    console.error(
      JSON.stringify({
        message: 'Method Not Allowed',
        severity: 'ERROR',
      })
    );
    return res.status(405).send('Method Not Allowed');
  }

  const { event_type, event_properties, user_properties } = req.body;

  // Check for gclid in both event_properties and user_properties
  const gclid = event_properties?.gclid || user_properties?.gclid;

  if (!gclid) {
    console.error(
      JSON.stringify({
        message:
          'Missing gclid in request body (checked both event_properties and user_properties)',
        severity: 'ERROR',
        body: req.body,
      })
    );
    return res.status(400).send('Missing gclid in request body');
  }

  try {
    const datasetId = process.env.BIGQUERY_DATASET;
    const tableId = process.env.BIGQUERY_TABLE;

    if (!projectId || !datasetId || !tableId) {
      throw new Error(
        'Missing required environment variables for BigQuery connection.'
      );
    }
    // 2. Query BigQuery for settings
    const query = `SELECT * FROM \`${projectId}.${datasetId}.${tableId}\` WHERE REGEXP_CONTAINS(events, CONCAT('\\\\b', @eventType, '\\\\b'))`;
    const options = { query, params: { eventType: event_type } };
    const [rows] = await bigquery.query(options);

    if (rows.length === 0) {
      const logMessage = `No settings found for event: ${event_type}`;
      console.log(
        JSON.stringify({
          message: logMessage,
          severity: 'INFO',
          event_type,
        })
      );
      res.status(200).send(logMessage);
      return;
    }

    // 3. Prepare message for Pub/Sub
    const messagePayload = {
      gclid: gclid,
      event_type: event_type,
      conversion_actions: rows,
    };
    const dataBuffer = Buffer.from(JSON.stringify(messagePayload));

    // 4. Publish message to Pub/Sub
    const messageId = await pubsub
      .topic(TOPIC_NAME)
      .publishMessage({ data: dataBuffer });

    console.log(
      JSON.stringify({
        message: `Message ${messageId} published to topic ${TOPIC_NAME}.`,
        severity: 'INFO',
        messageId,
        topic: TOPIC_NAME,
        gclid: gclid,
        event_type: event_type,
      })
    );

    res.status(200).send(`Successfully published message ID: ${messageId}`);
  } catch (error) {
    console.error(
      JSON.stringify({
        message: 'amplitude-controller execution failed.',
        severity: 'ERROR',
        error: error.message,
        stack: error.stack,
        event_type: event_type,
      })
    );
    res.status(500).send('Internal Server Error');
  }
});
