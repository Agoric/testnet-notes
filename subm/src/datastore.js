/**
 * "... we recommend using Firestore in Datastore mode
 *  for databases that will be used primarily by App Engine apps."
 * -- https://cloud.google.com/appengine/docs/standard/nodejs/using-cloud-datastore
 *
 * Ref docs:
 * [Node\.js client library  \|  Google Cloud](https://cloud.google.com/nodejs/docs/reference/datastore/latest)
 *
 * TODO: try
 * [Running the Datastore Emulator](https://cloud.google.com/datastore/docs/tools/datastore-emulator)
 * Or maybe not: "The Datastore emulator does not emulate features introduced by Firestore in Datastore mode."
 */
// @ts-check

/**
 *
 * @param {{
 *   datastore: typeof import('@google-cloud/datastore')
 * }} io
 */
async function main({ datastore }) {
  // assumes $GOOGLE_APPLICATION_CREDENTIALS has name of service account key file
  // for local development, add a key for the App Engine default service account
  // and download it in .json format.
  const store = new datastore.Datastore();

  await store.save({
    key: store.key('visit'),
    data: { size: 10, color: 'blue' },
  });

  // https://cloud.google.com/datastore/docs/concepts/queries
  const q = store.createQuery('visit').filter('size', '=', 10);

  const visits = await store.runQuery(q);
  console.log(visits);
}

// ocap: Import powerful references only when invoked from CLI.
/* global require, module */
if (require.main === module) {
  /* eslint-disable global-require */
  // eslint-disable-next-line no-use-before-define
  main({
    datastore: require('@google-cloud/datastore'),
  }).catch(err => console.error(err));
}
