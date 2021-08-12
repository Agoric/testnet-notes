/* global require, Buffer */
// @ts-check

const discord = require('passport-discord');
const session = require('express-session');

const { freeze } = Object;

/**
 * @param { T | null | undefined } it
 * @returns { T }
 * @template T
 */
const the = it => {
  if (!it) throw Error('must not be falsy');
  return it;
};

/**
 * @param {string} token
 * @param {{ get: typeof import('https').get }} io
 */
function DiscordAPI(token, { get }) {
  // cribbed from rchain-dbr/o2r/gateway/server/main.js
  const host = 'discordapp.com';
  const api = '/api/v6';
  const headers = { Authorization: `Bot ${token}` };

  /** @param { string } path */
  function getJSON(path) {
    console.log('calling Discord API', { host, path, headers });
    return new Promise((resolve, reject) => {
      const req = get({ host, path, headers }, res => {
        /** @type { Buffer[] } */
        const chunks = [];
        // console.log({ status: res.statusCode,
        //               headers: res.headers });
        res
          .on('data', data => {
            chunks.push(data);
          })
          .on('end', () => {
            const body = Buffer.concat(chunks).toString();
            const data = JSON.parse(body);
            console.log('@@Discord done:', Object.keys(data));
            resolve(data);
          });
      });
      req.on('error', err => {
        console.error('Discord API error:', err);
        reject(err);
      });
    });
  }

  return freeze({
    /** @param { string } guildID */
    guilds(guildID) {
      return freeze({
        /** @returns { Promise<unknown> } */
        info() {
          return getJSON(`${api}/guilds/${guildID}`);
        },
        /**
         * @param { string } userID
         * @returns { Promise<unknown> }
         */
        members(userID) {
          return getJSON(`${api}/guilds/${guildID}/members/${userID}`);
        },
      });
    },
  });
}

const Pages = freeze({
  start: `
<!doctype html>
<title>Agoric Testnet Submission</title>

<a href="/auth/discord">login to discord and upload</a>

<hr />
`,
  /**
   * WARNING: caller is responsible to see that values are html-injection-safe
   *
   * @param {{
   *  GoogleAccessId: string,
   *  key: string,
   *  bucket: string,
   *  policy: string,
   *  signature: string,
   * } & Record<string, string> } info
   */
  upload: ({ GoogleAccessId, key, bucket, policy, signature, ...headers }) => `
<!doctype html>
<title>Agoric Testnet Submission</title>
<form action="http://${bucket}.storage.googleapis.com" method="post" enctype="multipart/form-data">
	<input type="text" name="key" value="${key}">
	<input type="hidden" name="bucket" value="${bucket}">
	<input type="hidden" name="GoogleAccessId" value="${GoogleAccessId}">
	<input type="hidden" name="policy" value="${policy}">
	<input type="hidden" name="signature" value="${signature}">
  <input type="hidden" name="Content-Type" value="${headers['Content-Type']}">
  <input type="hidden" name="Content-Disposition" value="${headers['Content-Disposition']}">

	<input name="file" type="file">
	<input type="submit" value="Upload">
</form>


<hr />

<a href="https://${bucket}.storage.googleapis.com/${key}" target="_new">View https://${bucket}.storage.googleapis.com/${key}</a>

  `,
});

/**
 * @param {ReturnType<ReturnType<typeof DiscordAPI>['guilds']>} guild
 * @param {ReturnType<typeof import('gcs-signed-urls')>} storage
 */
function makeUploader(guild, storage) {
  const self = freeze({
    uploadPath: '/uploadForm',
    callbackPath: '/auth/discord/callback',
    callbackURL: base => new URL(self.callbackPath, base).toString(),
    serializeUser: (user, done) => done(null, user),
    deserializeUser: (obj, done) => done(null, obj),
    /** @param { unknown } profile */
    async login(profile) {
      if (typeof profile !== 'object' || !profile) throw TypeError();
      // @ts-ignore ????
      const { id } = profile;
      if (typeof id !== 'string') throw TypeError();
      const member = await guild.members(id);
      console.log('@@@member', member);
      if (typeof member !== 'object' || !member) throw TypeError();
      // @ts-ignore
      const { user } = member;
      if (typeof user !== 'object' || !user) throw TypeError();
      const { username, discriminator } = user;
      if (typeof username !== 'string' || typeof discriminator !== 'string')
        throw TypeError();
      return `${username}#${discriminator}`;
    },
    /**
     * @param { string } fileName
     * @param key
     */
    formData: (fileName, key) => storage.uploadRequest(fileName, key, true, {}),
  });

  return self;
}

/**
 *
 * @param {Record<string, string | undefined>} env
 * @param {{
 *   clock: () => number,
 *   get: typeof import('https').get,
 *   express: typeof import('express'),
 *   passport: typeof import('passport'),
 *   gcs: typeof import('gcs-signed-urls'),
 * }} io
 */
async function main(env, { clock, get, express, passport, gcs }) {
  const host = env.HOST || 'localhost';
  const port = parseInt(env.PORT || '3000', 10);
  const base = `http://${host}:${port}`;

  const storage = gcs(
    env.GCS_PRIVATE_KEY,
    env.GOOGLE_SERICES_EMAIL,
    env.GCS_STORAGE_BUCKET,
  );
  const guild = DiscordAPI(the(env.TOKEN), { get }).guilds(the(env.GUILD_ID));
  const site = makeUploader(guild, storage);

  passport.serializeUser(site.serializeUser);
  passport.deserializeUser(site.deserializeUser);

  const strategy = new discord.Strategy(
    {
      clientID: the(env.CLIENT_ID),
      clientSecret: the(env.CLIENT_SECRET),
      callbackURL: site.callbackURL(base),
      scope: ['identify', 'email', 'guilds', 'guilds.join'],
    },
    async (_accessToken, _refreshToken, profile, cb) => {
      try {
        const user = await site.login(profile);
        cb(null, user);
      } catch (err) {
        cb(err);
      }
    },
  );
  passport.use(strategy);

  const app = express();
  app.use(
    session({
      secret: the(env.SESSION_SECRET),
      resave: false,
      saveUninitialized: false,
    }),
  );
  app.use(passport.initialize());
  app.use(passport.session());

  app.get('/', (_req, res) => res.send(Pages.start));
  app.get('/auth/discord', passport.authenticate('discord'));
  app.get(
    site.callbackPath,
    passport.authenticate('discord', { failureRedirect: '/' }),
    (_req, res) => res.redirect(site.uploadPath), // Successful auth
  );
  function checkAuth(req, res, next) {
    if (req.isAuthenticated()) return next();
    res.send('not logged in :(');
    return undefined;
  }
  app.get(site.uploadPath, checkAuth, (req, res) => {
    const fileName = `${the(req.user)}.slog.gz`;
    const ts = new Date(clock()).toISOString();
    const freshKey = `${ts}-${fileName}`;
    const formData = site.formData(fileName, freshKey);
    console.log({ formData }); // @@@
    res.send(Pages.upload(formData));
  });

  console.log(base);
  app.listen(port);
}

/* global module */
if (require.main === module) {
  // ocap: Import powerful references only when invoked as a main module.
  /* eslint-disable global-require */
  /* global process */
  main(
    { ...process.env },
    {
      clock: () => Date.now(),
      express: require('express'),
      passport: require('passport'),
      gcs: require('gcs-signed-urls'),
      get: require('https').get,
    },
  ).catch(err => console.error(err));
}
