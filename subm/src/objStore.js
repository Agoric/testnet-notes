/**
 * TODO(developer): Uncomment the following lines before running the sample.
 */
// The ID of your GCS bucket
// const bucketName = 'your-unique-bucket-name';

// The ID of your GCS file
// const fileName = 'your-file-name';

// Imports the Google Cloud client library
const { Storage } = require('@google-cloud/storage');

// Creates a client
const storage = new Storage();

async function generateV4SignedPolicy() {
  const bucket = storage.bucket(bucketName);
  const file = bucket.file(fileName);

  // These options will allow temporary uploading of a file
  // through an HTML form.
  const expires = Date.now() + 10 * 60 * 1000; //  10 minutes
  const options = {
    expires,
    fields: { 'x-goog-meta-test': 'data' },
  };

  // Get a v4 signed policy for uploading file
  const [response] = await file.generateSignedPostPolicyV4(options);

  // Create an HTML form with the provided policy
  let output = `<form action='${response.url}' method='POST' enctype="multipart/form-data">\n`;
  // Include all fields returned in the HTML form as they're required
  for (const name of Object.keys(response.fields)) {
    const value = response.fields[name];
    output += `  <input name='${name}' value='${value}' type='hidden'/>\n`;
  }
  output += "  <input type='file' name='file'/><br />\n";
  output +=
    "  <input type='submit' value='Upload File' name='submit'/><br />\n";
  output += '</form>';

  console.log(output);
}

generateV4SignedPolicy().catch(console.error);
