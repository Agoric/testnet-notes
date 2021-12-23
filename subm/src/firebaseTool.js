/* global Buffer */
// @ts-check
const { makeConfig } = require('./config.js');

const { freeze } = Object; // IOU ses / harden

/**
 * @param { TemplateTag } config
 */
function getFirebaseConfig(config) {
  return {
    apiKey: config`FIREBASE_API_KEY`,
    authDomain: config`FIREBASE_AUTH_DOMAIN`,
    databaseURL: config`FIREBASE_DATABASE_URL`,
    projectId: config`FIREBASE_PROJECT_ID`,
    appId: config`FIREBASE_APP_ID`,
  };
}

/**
 * @param { typeof import('firebase-admin') } admin
 * @param {ReturnType<typeof getFirebaseConfig>} firebaseConfig
 */
function makeFirebaseAdmin(admin, firebaseConfig) {
  return freeze({
    init: () => {
      // "If your code is deployed in the App Engine standard environment for Java, Python or Go, the Admin SDK can use the App Identity service present in that environment to sign custom tokens."
      // -- https://firebase.google.com/docs/auth/admin/create-custom-tokens#letting_the_admin_sdk_discover_a_service_account
      admin.initializeApp();
    },

    /**
     * @param { string } uid
     */
    async generateCustomToken(uid) {
      const jwt = await admin.auth().createCustomToken(uid);

      const encodedConfig = Buffer.from(
        JSON.stringify(firebaseConfig),
      ).toString('base64');

      const combinedToken = [encodedConfig, jwt].join('.');

      return combinedToken;
    },
  });
}

/**
 * @param {string[]} args
 * @param {Record<string, string | undefined>} env
 * @param {{ admin: typeof import('firebase-admin') }} io
 */
async function main(args, env, { admin }) {
  const [uid] = args;

  const config = makeConfig(env);
  const firebaseConfig = getFirebaseConfig(config);

  const anAdmin = makeFirebaseAdmin(admin, firebaseConfig);
  anAdmin.init();
  const combinedToken = await anAdmin.generateCustomToken(uid);
  console.log(
    'Please execute:',
    `curl -s -X PUT --data '${combinedToken}' http://127.0.0.1:3352/push-config`,
  );
}

/* global process, require, module */
module.exports = { makeFirebaseAdmin, getFirebaseConfig, main };

if (require.main === module) {
  main(
    process.argv.slice(2),
    { ...process.env },
    {
      // eslint-disable-next-line global-require
      admin: require('firebase-admin'),
    },
  ).catch(err => console.error(err));
}
