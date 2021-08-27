/* global require */
// Import the Google Cloud client libraries
const { BigQuery } = require('@google-cloud/bigquery');
const { Storage } = require('@google-cloud/storage');

// Instantiate clients
const bigquery = new BigQuery();
const storage = new Storage();

/**
 * This sample loads the json file at
 * https://storage.googleapis.com/cloud-samples-data/bigquery/us-states/us-states.json
 *
 * TODO(developer): Replace the following lines with the path to your file.
 */
const bucketName = 'slogfile-upload-5';
const filename = '2021-08-25T23:10:36.470Z-Vitconratran#7219.slog.gz';

// https://cloud.google.com/bigquery/docs/datasets
async function createDataset() {
  // Creates a new dataset named "my_dataset".

  /**
   * TODO(developer): Uncomment the following lines before running the sample.
   */
  const datasetId = 'slog45';

  // Specify the geographic location where the dataset should reside
  const options = {
    location: 'US',
  };

  // Create a new dataset
  const [dataset] = await bigquery.createDataset(datasetId, options);
  console.log(`Dataset ${dataset.id} created.`);
}

// https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json#node.js
async function loadJSONFromGCS() {
  // Imports a GCS file into a table with manually defined schema.

  /**
   * TODO(developer): Uncomment the following lines before running the sample.
   */
  const datasetId = 'slog45';
  const tableId = 'entry';

  // Configure the load job. For full list of options, see:
  // https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
  const metadata = {
    // sourceFormat: 'NEWLINE_DELIMITED_JSON',
    // "If you need to keep the entire JSON object, then it should be put into a string column, which can be queried using JSON functions."
    // -- https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json
    sourceFormat: 'CSV',
    // stackoverflow hack
    fieldDelimiter: '\u0001',
    // schema: {
    //   fields: [
    //     { name: 'time', type: 'STRING' },
    //     { name: 'post_abbr', type: 'STRING' },
    //   ],
    // },
    autodetect: true,
    location: 'US',
  };

  // Load data from a Google Cloud Storage file into the table
  const [job] = await bigquery
    .dataset(datasetId)
    .table(tableId)
    .load(storage.bucket(bucketName).file(filename), metadata);
  // load() waits for the job to finish
  console.log(`Job ${job.id} completed.`);

  // Check the job's status for errors
  const errors = job.status.errors;
  if (errors && errors.length > 0) {
    throw errors;
  }
}

/* global process */
if (process.argv.includes('--create')) {
  createDataset().catch(err => console.error(err));
} else {
  loadJSONFromGCS().catch(err => console.error(err));
}
