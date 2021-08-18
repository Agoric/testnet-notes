/* global Buffer */
// @ts-check

async function generateCustomToken(uid, admin, firebaseConfig) {
  const jwt = await admin.auth().createCustomToken(uid);

  const encodedConfig = Buffer.from(JSON.stringify(firebaseConfig)).toString(
    'base64',
  );

  const combinedToken = [encodedConfig, jwt].join('.');

  console.log(
    'Please execute:',
    `curl -s -X PUT --data '${combinedToken}' http://127.0.0.1:3352/push-config`,
  );

  return combinedToken;
}

/**
 * @param { NodeJS.ProcessEnv } env
 * @returns { TemplateTag }
 * @typedef { (parts: TemplateStringsArray, ...args: unknown[]) => string } TemplateTag
 */
const makeConfig = env => {
  return ([name], ..._args) => {
    const value = env[name];
    if (value === undefined) {
      throw Error(`${name} not configured`);
    }
    return value;
  };
};

async function main(args, env, { admin }) {
  const [uid] = args;
  // "If your code is deployed in the App Engine standard environment for Java, Python or Go, the Admin SDK can use the App Identity service present in that environment to sign custom tokens."
  // -- https://firebase.google.com/docs/auth/admin/create-custom-tokens#letting_the_admin_sdk_discover_a_service_account
  admin.initializeApp();

  const config = makeConfig(env);

  const firebaseConfig = {
    apiKey: config`FIREBASE_API_KEY`,
    authDomain: config`FIREBASE_AUTH_DOMAIN`,
    databaseURL: config`FIREBASE_DATABASE_URL`,
    projectId: config`FIREBASE_PROJECT_ID`,
    appId: config`FIREBASE_APP_ID`,
  };

  generateCustomToken(uid, admin, firebaseConfig);
}

/* global process, require, module */
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
