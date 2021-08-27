// objStore - Upload an object to Google Cloud Storage with HTML forms

const { entries } = Object;

/**
 * Upload an object to Google Cloud Storage with HTML forms
 *
 * @param { string } bucketName The ID of your GCS bucket
 * @param { string } fileName The ID of your GCS file
 * @param { Date } now
 * @param {{
 *   storage: import('@google-cloud/storage').Storage,
 * }} io
 */
async function generateV4SignedPolicy(bucketName, fileName, now, { storage }) {
  const bucket = storage.bucket(bucketName);
  const file = bucket.file(fileName);

  // These options will allow temporary uploading of a file
  // through an HTML form.
  const expires = now.valueOf() + 10 * 60 * 1000; //  10 minutes
  const options = {
    expires,
    fields: {
      'x-goog-meta-test': 'data',
      // https://stackoverflow.com/a/61783141/7963
      bucket: bucketName,
    },
  };

  // Get a v4 signed policy for uploading file
  const [response] = await file.generateSignedPostPolicyV4(options);

  return response;
}

/** @param { import('@google-cloud/storage').SignedPostPolicyV4Output } response */
function uploadForm(response) {
  // Create an HTML form with the provided policy
  // Include all fields returned in the HTML form as they're required
  const markup = `
  <form action='${response.url}' method='POST' enctype="multipart/form-data">
  ${entries(response.fields)
    .map(
      ([name, value]) =>
        `  <input name='${name}' value='${value}' type='hidden'/>`,
    )
    .join('\n')}
  <input type='file' name='file'/><br />
  <input type='submit' value='Upload File'/><br />
  </form>`;

  return markup;
}

/* global module */
module.exports = { generateV4SignedPolicy, uploadForm };
