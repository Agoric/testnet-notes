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

const { DiscordAPI, avatar } = require('./discordGuild.js');
/** @typedef { import('./discordGuild.js').GuildMember } GuildMember */
const { makeFirebaseAdmin, getFirebaseConfig } = require('./firebaseTool.js');
const { generateV4SignedPolicy } = require('./objStore.js');

const { freeze, keys, values, entries } = Object; // please excuse freeze vs. harden

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
  <br />
  <hr />
  <address>
  <a href="https://github.com/Agoric/testnet-notes/tree/main/subm">source code and issues on github</a>
  </address>
  </nav>
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
  uploadSuccessPath: '/participant/slogOK',
  /** @param { string } base */
  uploadSuccessURL: base => `${new URL(Site.uploadSuccessPath, base)}`,
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

  /** @param { FileInfo[] } files */
  fileList: files => `
    <textarea readonly rows=8 cols=100
    >md5Hash                      size       name
    \n${files
      .sort((a, b) => (a.name < b.name ? -1 : 1))
      .map(
        ({ name, metadata: { md5Hash, size } }) =>
          `${md5Hash} ${numeral(parseInt(size, 10), 14)} ${name}`,
      )
      .join('\n')}
    </textarea>`,

  /** @param { Record<string, string> } fields */
  hiddenFields: fields =>
    entries(fields)
      .map(
        ([name, value]) =>
          `  <input name='${name}' value='${value}' type='hidden'/>`,
      )
      .join('\n'),

  /**
   * Construct slogfile upload form.
   *
   * WARNING: caller is responsible to see that values are html-injection-safe
   *
   * @param { GuildMember } member
   * @param { import('@google-cloud/storage').SignedPostPolicyV4Output } policy
   * @param { FileInfo[] } files
   * @typedef {{ name: string, metadata: { size: string, md5Hash: string }}} FileInfo
   */
  uploadSlog: (member, policy, files) => `
${AgoricStyle.top}

<h1>Testnet Admin</h1>

${Site.welcome(member)}

<form action="${policy.url}"
      method="post" enctype="multipart/form-data">
      <fieldset><legend>slogfile</legend>

  You can check this list to see if your file arrived:</p>
  ${Site.fileList(files)}

	<div>
  <em>storage key:</em> <code>${policy.fields.key}</code>
  <p>
  <strong>NOTE:</strong> if you get an <code>Access denied. ... does not have storage.objects.delete access ...</code>
  error, your storage key has already been used. Reload the page to get a new one.
  </p>
  </div>
  ${Site.hiddenFields(policy.fields)}
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
 * @param {GuildMember} member
 * @param { string } bucket
 * @param { StorageT } objectStore
 * @param { FirebaseAdmin } loadGenAdmin
 * @typedef { ReturnType<import('./firebaseTool.js').makeFirebaseAdmin> } FirebaseAdmin
 */
function makeTestnetParticipant(member, bucket, objectStore, loadGenAdmin) {
  if (!member.user) throw RangeError();
  const { user } = member;
  const userID = `${user.username}#${user.discriminator}`;

  return freeze({
    member,
    user: member.user,

    allFiles: () => objectStore.bucket(bucket).getFiles(),

    /**
     * @param { Date } freshTime
     * @param { string } redirect full URL
     */
    uploadPolicy: (freshTime, redirect) => {
      // ISSUE: # in filename is asking for trouble
      const fileName = `${userID}.slog.gz`;
      const freshKey = `${freshTime.toISOString()}-${fileName}`;
      return generateV4SignedPolicy(
        objectStore,
        bucket,
        freshKey,
        freshTime,
        redirect,
      );
    },
    loadGenKey: () => loadGenAdmin.generateCustomToken(user.id),
  });
}

/**
 * A Discord bot has authority to endow passport sessions
 * with discord user rights.
 *
 * @param {ReturnType<ReturnType<typeof DiscordAPI>['guilds']>} guild
 * @param {Record<string, Snowflake>} authorizedRoles
 * @param {discord.StrategyOptions} opts
 * @param { Object } powers
 * @param { StorageT } powers.objectStore
 * @param { string } powers.bucketName
 * @param { FirebaseAdmin } powers.loadGenAdmin
 * @typedef {import('./discordGuild.js').Snowflake} Snowflake
 */
function makeDiscordBot(guild, authorizedRoles, opts, powers) {
  /** @param { GuildMember } mem */
  const checkParticipant = mem => {
    const roles = values(authorizedRoles).filter(r => mem.roles.includes(r));
    if (roles.length < 1) {
      return `${mem.nick} lacks roles ${JSON.stringify(keys(authorizedRoles))}`;
    }
    return null;
  };

  /** @param { GuildMember } member */
  const reviveMember = member =>
    makeTestnetParticipant(
      member,
      powers.bucketName,
      powers.objectStore,
      powers.loadGenAdmin,
    );

  const self = freeze({
    /** @param {string} failureRedirect */
    passport: failureRedirect => {
      const aPassport = new passport.Passport();

      aPassport.serializeUser((member, done) =>
        done(null, JSON.stringify(member)),
      );
      // once authenticated, grant session capabilities
      aPassport.deserializeUser((data, done) =>
        done(null, reviveMember(JSON.parse(data))),
      );

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
 *   makeStorage: (...args: unknown[]) => StorageT,
 *   admin: typeof import('firebase-admin')
 * }} io
 * @typedef { import('@google-cloud/storage').Storage } StorageT
 */
async function main(env, { clock, get, express, makeStorage, admin }) {
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
    // @ts-ignore Argument of type 'Function' is not assignable to parameter of type 'PathParams'
    session({
      secret: config`SUBM_SESSION_SECRET`,
      resave: false,
      saveUninitialized: false,
    }),
  );

  const discordAPI = DiscordAPI(config`DISCORD_API_TOKEN`, { get });
  const guild = discordAPI.guilds(config`DISCORD_GUILD_ID`);
  const loadGenAdmin = makeFirebaseAdmin(admin, getFirebaseConfig(config));
  loadGenAdmin.init();
  const bot = makeDiscordBot(
    guild,
    Site.testnetRoles,
    {
      clientID: config`DISCORD_CLIENT_ID`,
      clientSecret: config`DISCORD_CLIENT_SECRET`,
      callbackURL: Site.callbackPath,
      scope: ['identify', 'email'],
    },
    {
      bucketName: config`GCS_STORAGE_BUCKET`,
      // ASSUME we are running in an environment which supports Application Default Credentials
      objectStore: makeStorage(),
      loadGenAdmin,
    },
  );

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

  const loginCheck = (req, res, next) =>
    req.isAuthenticated() ? next() : res.send(Site.badLogin());
  const handleError = (res, baseUrl, err) => {
    console.warn(baseUrl, err);
    res.status(err.status || 500);
    res.render('error', {
      message: err.message,
      error: app.get('env') === 'development' ? err : {},
    });
  };
  // Upload form
  // Note the actual upload request goes directly to Google Cloud Storage.
  app.get(Site.uploadSlogPath, loginCheck, async (req, res) => {
    try {
      const participant =
        /** @type { ReturnType<typeof makeTestnetParticipant> } */ (req.user);
      const policy = await participant.uploadPolicy(
        new Date(clock()),
        Site.uploadSuccessURL(base),
      );
      const [files] = await participant.allFiles();
      const page = Site.uploadSlog(participant.member, policy, files);
      res.send(page);
    } catch (err) {
      handleError(res, req.baseUrl, err);
    }
  });
  app.get(Site.uploadSuccessPath, loginCheck, async (req, res) => {
    const participant =
      /** @type { ReturnType<typeof makeTestnetParticipant> } */ (req.user);
    const [files] = await participant.allFiles();
    res.send(
      JSON.stringify(
        files.map(f => ({
          name: f.name,
          hash: f.metadata.md5Hash,
          size: f.metadata.size,
        })),
        null,
        2,
      ),
    );
  });

  app.get(Site.loadGenKeyPath, loginCheck, async (req, res) => {
    try {
      const participant =
        /** @type { ReturnType<typeof makeTestnetParticipant> } */ (req.user);

      const token = await participant.loadGenKey();
      res.send(Site.loadGenKey(participant.member, token));
    } catch (err) {
      handleError(res, req.baseUrl, err);
    }
  });

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
      makeStorage: (C => (...args) => new C(...args))(
        require('@google-cloud/storage').Storage,
      ),
      admin: require('firebase-admin'),
    },
  ).catch(err => console.error(err));
}
