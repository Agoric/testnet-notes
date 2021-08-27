#!/usr/bin/env node
/**
 * subm - submit Agoric Testnet Swingset logs (slogfiles)
 *
 * Access to upload files is granted based on Discord OAuth credentials.
 * Data is stored in Google Cloud Storage.
 */

/* global require */
// @ts-check

const discord = require('passport-discord'); // please excuse CJS
const session = require('express-session');
const passport = require('passport'); // Our usage is pure; we ignore the default singleton.
const gcs = require('gcs-signed-urls'); // we use only pure parts

const { DiscordAPI, avatar } = require('./discordGuild.js');
/** @typedef { import('./discordGuild.js').GuildMember } GuildMember */
const { makeFirebaseAdmin, getFirebaseConfig } = require('./firebaseTool.js');

const { freeze, keys, values } = Object; // please excuse freeze vs. harden

const AgoricStyle = freeze({
  top: `
  <!doctype html>
  <head>
  <title>Agoric Testnet Submission</title>
  <style>
    label { display: block; padding: .5em }
    .avatar {
      border-radius: 10%; padding: 5x; border 1px solid #ddd;
    }
  </style>
  </head>

  <div class="container">
  <nav>
  <a href="https://agoric.com/"
  ><img alt="Agoric" align="bottom"
     src="https://agoric.com/wp-content/themes/agoric_2021_theme/assets/img/logo.svg"
      /></a> &middot; <a href="https://validate.agoric.com/">Incentivized Testnet</a>
  </nav>
  <hr />
  `,
});

/** @type {(n: number, lp?: number) => string} */
const numeral = (n, lp = 0) =>
  `${' '.repeat(lp)}${n.toLocaleString()}`.slice(-lp);

const Site = freeze({
  loginPath: '/auth/discord',
  callbackPath: '/auth/discord/callback',
  badLoginPath: '/loginRefused',
  uploadSlogPath: '/participant/slogForm',
  loadGenKeyPath: '/participant/loadGenKey',

  /**
   * @param {string | undefined} project
   * @param {string | undefined} hostConfig
   * @param {string | undefined} portConfig
   */
  base: (project, hostConfig, portConfig) => {
    const port = parseInt(portConfig || '3000', 10);
    const base = project
      ? `https://${project}.wl.r.appspot.com`
      : `http://${hostConfig || 'localhost'}:${port}`;
    return { base, port };
  },

  start: () => `${AgoricStyle.top}
<h1>Incentivized Testnet Participants</h1>

<form action="/auth/discord">
<fieldset>
<legend>To Submit materials</legend>
<large>
<button type="submit">Login via Discord</button>
</large>
</fieldset>
</form>
</div>
`,

  testnetRoles: {
    'testnet-participant': '819067161371738173',
    // TODO: support for many-to-one discord to moniker
    // 'testnet-teammate': '825108158744756226',
    team: '754111409645682700',
  },

  badLogin: () => `${AgoricStyle.top}
  <p><strong>Login refused.</strong> Only Incentivized Testnet participants are allowed.</p>
  <form action="/"><button type="submit">Try again</button></form>
  `,

  /** @param { GuildMember } member */
  welcome: member => `
    <figure>
    <img class="avatar" src="${avatar(member.user)}" />
    <figcaption>Welcome <b>${member.nick || 'participant'}</b>.</figcaption>
    </figure>
  `,

  /**
   * @param { GuildMember } member
   * @param { string } combinedToken
   */
  loadGenKey: (member, combinedToken) => `${AgoricStyle.top}

    <h1>LoadGen Key</h1>

    ${Site.welcome(member)}

    <p>Please execute:</p>
    <textarea readonly cols='80' rows='24'
    >curl -s -X PUT --data '${combinedToken}' http://127.0.0.1:3352/push-config
    </textarea>
      `,

  /**
   * Construct slogfile upload form.
   *
   * WARNING: caller is responsible to see that values are html-injection-safe
   *
   * @param { GuildMember } member
   * @param {{
   *  GoogleAccessId: string,
   *  key: string,
   *  bucket: string,
   *  policy: string,
   *  signature: string,
   * } & Record<string, string> } info
   * @param { { name: string}[] } files
   */
  uploadSlog: (
    member,
    { GoogleAccessId, key, bucket, policy, signature, ...headers },
    files,
  ) => `
${AgoricStyle.top}

<h1>Testnet Admin</h1>

${Site.welcome(member)}

<form action="https://${bucket}.storage.googleapis.com"
      method="post" enctype="multipart/form-data">
      <fieldset><legend>slogfile</legend>

  <p><em><strong>NOTE:</strong> this form lacks feedback on when your upload finishes.</em><br />

  You can check this list to see if your file arrived:</p>
  <textarea readonly rows=8 cols=100>
md5Hash                      size       name
${files
  .sort((a, b) => (a.name < b.name ? -1 : 1))
  .map(
    ({ name, metadata: { md5Hash, size } }) =>
      `${md5Hash} ${numeral(size | 0, 14)} ${name}`,
  )
  .join('\n')}
  </textarea>
  <input type="hidden" name="bucket" value="${bucket}">
	<label><em>storage key:</em> <input type="text" readonly name="key" value="${key}" />
  <br />
  <strong>NOTE:</strong> if you get an <code>Access denied. ... does not have storage.objects.delete access ...</code>
  error, your storage key has already been used. Reload the page to get a new one.
  </label>
	<input type="hidden" name="GoogleAccessId" value="${GoogleAccessId}">
	<input type="hidden" name="policy" value="${policy}">
	<input type="hidden" name="signature" value="${signature}">
  <input type="hidden" name="Content-Type" value="${headers['Content-Type']}">
  <input type="hidden" name="Content-Disposition"
         value="${headers['Content-Disposition']}">
  <label>
    Suggested name: <code><em>moniker</em>-agorictest-<em>NN</em></code>.slog.gz</code><br />
	  <input name="file" type="file">
    <input type="submit" value="Upload">
  </label>
  </fieldset>
</form>

<h2>Load Generator Key</h1>
<p>See: <a href='${Site.loadGenKeyPath}'>load generator key</a>.</p>
`,
});

/**
 * A Discord bot has authority to endow passport sessions
 * with discord user rights.
 *
 * @param {ReturnType<ReturnType<typeof DiscordAPI>['guilds']>} guild
 * @param {Record<string, Snowflake>} authorizedRoles
 * @param {discord.StrategyOptions} opts
 * @typedef {import('./discordGuild.js').Snowflake} Snowflake
 */
function makeDiscordBot(guild, authorizedRoles, opts) {
  /** @param { GuildMember } mem */
  const checkParticipant = mem => {
    const roles = values(authorizedRoles).filter(r => mem.roles.includes(r));
    if (roles.length < 1) {
      return `${mem.nick} lacks roles ${JSON.stringify(keys(authorizedRoles))}`;
    }
    return null;
  };

  const self = freeze({
    /** @param {string} failureRedirect */
    passport: failureRedirect => {
      const aPassport = new passport.Passport();

      aPassport.serializeUser((member, done) =>
        done(null, JSON.stringify(member)),
      );
      aPassport.deserializeUser((data, done) => done(null, JSON.parse(data)));

      const strategy = new discord.Strategy(
        opts,
        // TODO: refreshToken handling
        async (_accessToken, _refreshToken, profile, cb) => {
          try {
            const { id } = profile;
            const member = await guild.members(id);

            const message = checkParticipant(member);
            if (message) {
              console.warn(message);
              return cb(null, false, { message });
            }
            if (!member.user) {
              const noUser = `undefined user in GuildMember ${id}`;
              console.warn(noUser);
              cb(null, false, { message: noUser });
            }
            console.info('login', member);
            return cb(null, member);
          } catch (err) {
            return cb(err);
          }
        },
      );
      aPassport.use(strategy);
      const loginHandler = aPassport.authenticate('discord');
      const callbackHandler = aPassport.authenticate('discord', {
        failureRedirect,
      });
      return { aPassport, loginHandler, callbackHandler };
    },
  });

  return self;
}

/**
 * @param {GuildMember} member
 * @param {ReturnType<typeof import('gcs-signed-urls')>} storage
 * @param { ReturnType<import('./firebaseTool.js').makeFirebaseAdmin> } loadGenAdmin
 */
function makeTestnetParticipant(member, storage, loadGenAdmin) {
  if (!member.user) throw RangeError();
  const { user } = member;
  const userID = `${user.username}#${user.discriminator}`;

  return freeze({
    user: member.user,
    /**
     * @param { number } freshTime
     */
    slogFormData: freshTime => {
      // ISSUE: # in filename is asking for trouble
      const fileName = `${userID}.slog.gz`;
      const dt = new Date(freshTime).toISOString();
      const freshKey = `${dt}-${fileName}`;
      return storage.uploadRequest(fileName, freshKey, true, {});
    },

    loadGenKey: () => loadGenAdmin.generateCustomToken(user.id),
  });
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

/**
 * @param { NodeJS.ProcessEnv } env
 * @param {{
 *   clock: () => number,
 *   get: typeof import('https').get,
 *   express: typeof import('express'),
 *   Storage: typeof import('@google-cloud/storage').Storage,
 *   admin: typeof import('firebase-admin')
 * }} io
 */
async function main(env, { clock, get, express, Storage, admin }) {
  const app = express();
  app.enable('trust proxy'); // trust X-Forwarded-* headers
  app.get('/', (_req, res) => res.send(Site.start()));
  const { base, port } = Site.base(
    env.GOOGLE_CLOUD_PROJECT,
    env.HOST,
    env.PORT,
  );

  const config = makeConfig(env);
  app.use(
    session({
      secret: config`SUBM_SESSION_SECRET`,
      resave: false,
      saveUninitialized: false,
    }),
  );

  const cloudStore = new Storage();

  const storage = gcs(
    config`GCS_PRIVATE_KEY`,
    config`GOOGLE_SERVICES_EMAIL`,
    config`GCS_STORAGE_BUCKET`,
  );
  const discordAPI = DiscordAPI(config`DISCORD_API_TOKEN`, { get });
  const guild = discordAPI.guilds(config`DISCORD_GUILD_ID`);
  const bot = makeDiscordBot(guild, Site.testnetRoles, {
    clientID: config`DISCORD_CLIENT_ID`,
    clientSecret: config`DISCORD_CLIENT_SECRET`,
    callbackURL: Site.callbackPath,
    scope: ['identify', 'email'],
  });
  const loadGenAdmin = makeFirebaseAdmin(admin, getFirebaseConfig(config));
  loadGenAdmin.init();

  // OAuth 2 flow
  const { aPassport, loginHandler, callbackHandler } = bot.passport(
    Site.badLoginPath,
  );
  app.use(aPassport.initialize());
  app.use(aPassport.session());
  app.get(Site.loginPath, loginHandler);
  app.get(
    Site.callbackPath,
    callbackHandler,
    (_req, res) => res.redirect(Site.uploadSlogPath), // Successful auth
  );
  app.get(Site.badLoginPath, (_r, res) => res.send(Site.badLogin()));

  // Upload form
  // Note the actual upload request goes directly to Google Cloud Storage.
  app.get(
    Site.uploadSlogPath,
    (req, res, next) =>
      req.isAuthenticated() ? next() : res.send(Site.badLogin()),
    async (req, res) => {
      try {
        const member = /** @type { GuildMember } */ (req.user);
        const participant = makeTestnetParticipant(
          member,
          storage,
          loadGenAdmin,
        );
        const formData = participant.slogFormData(clock());
        // console.log({ formData });
        const [files] = await cloudStore
          .bucket(config`GCS_STORAGE_BUCKET`)
          .getFiles();
        const page = Site.uploadSlog(member, formData, files);
        res.send(page);
      } catch (err) {
        res.status(err.status || 500);
        res.render('error', {
          message: err.message,
          error: app.get('env') === 'development' ? err : {},
        });
      }
    },
  );

  app.get(
    Site.loadGenKeyPath,
    (req, res, next) =>
      req.isAuthenticated() ? next() : res.send(Site.badLogin()),
    async (req, res) => {
      try {
        const member = /** @type { GuildMember } */ (req.user);
        const participant = makeTestnetParticipant(
          member,
          storage,
          loadGenAdmin,
        );
        const token = await participant.loadGenKey();
        res.send(Site.loadGenKey(member, token));
      } catch (err) {
        res.status(err.status || 500);
        res.render('error', {
          message: err.message,
          error: app.get('env') === 'development' ? err : {},
        });
      }
    },
  );

  // const testerID = '358096357862408195';
  // const tester = await guild.members(testerID);
  // app.get('/test', (_r, res) => res.send(Site.upload(tester, {})));

  console.log(base);
  app.listen(port);
}

// ocap: Import powerful references only when invoked from CLI.
/* global module */
if (require.main === module) {
  /* eslint-disable global-require */
  /* global process */
  // eslint-disable-next-line no-use-before-define
  main(
    { ...process.env },
    {
      clock: () => Date.now(),
      express: require('express'),
      get: require('https').get,
      Storage: require('@google-cloud/storage').Storage,
      admin: require('firebase-admin'),
    },
  ).catch(err => console.error(err));
}
