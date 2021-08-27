// objStore - Upload an object to Google Cloud Storage with HTML forms

/**
 * Upload an object to Google Cloud Storage with HTML forms
 *
 * @param { import('@google-cloud/storage').Storage } storage
 * @param { string } bucketName The ID of your GCS bucket
 * @param { string } fileName The ID of your GCS file
 * @param { Date } now
 * @param { string } there success redirect
 */
async function generateV4SignedPolicy(
  storage,
  bucketName,
  fileName,
  now,
  there,
) {
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
      success_action_redirect: there,
    },
  };

  // Get a v4 signed policy for uploading file
  const [response] = await file.generateSignedPostPolicyV4(options);

  return response;
}

/* global module */
module.exports = { generateV4SignedPolicy };
