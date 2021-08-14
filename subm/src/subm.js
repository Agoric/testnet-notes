#!/usr/bin/env node
/**
 * subm - submit Agoric Testnet Swingset logs (slogfiles)
 *
 * Access to upload files is granted based on Discord OAuth credentials.
 * Data is stored in Google Cloud Storage.
 */

/* global require, Buffer */
// @ts-check

const discord = require('passport-discord'); // please excuse CJS
const session = require('express-session');
const passport = require('passport'); // Our usage is pure; we ignore the default singleton.
const gcs = require('gcs-signed-urls'); // we use only pure parts

const { freeze, keys, values } = Object; // please excuse freeze vs. harden

/**
 * @param {string} host
 * @param {string} path
 * @param {Record<string, string>} headers
 * @param {{ get: typeof import('https').get }} io
 * @returns { Promise<string> }
 */
function getContent(host, path, headers, { get }) {
  // console.log('calling Discord API', { host, path, headers });
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
          resolve(body);
        });
    });
    req.on('error', err => {
      console.error('Discord API error:', err);
      reject(err);
    });
  });
}

/**
 * Discord API (a small slice of it, anyway)
 *
 * @param {string} token
 * @param {{ get: typeof import('https').get }} io
 *
 * // https://discordapp.com/developers/docs/resources/user
 * @typedef {{
 *  id: Snowflake,
 *  username: string,
 *  discriminator: string, // 4 digit discord-tag
 *  avatar: ?string,
 *  bot?: boolean,
 *  mfa_enabled?: boolean,
 *  locale?: string,
 *  verified?: boolean,
 *  email?: string
 * }} DiscordUser
 *
 * https://discord.com/developers/docs/resources/guild#guild-member-object
 * @typedef {{
 *   user?: DiscordUser,
 *   nick?: string,
 *   roles: Snowflake[],
 *   joined_at: TimeStamp,
 *   deaf: boolean,
 *   mute: boolean,
 *   pending?: boolean,
 *   permissions?: string,
 * }} GuildMember
 * @typedef { string } Snowflake 64 bit numeral
 * @typedef { string } TimeStamp ISO8601 format
 */
function DiscordAPI(token, { get }) {
  // cribbed from rchain-dbr/o2r/gateway/server/main.js
  const host = 'discordapp.com';
  const api = '/api/v6';
  const headers = { Authorization: `Bot ${token}` };

  const getJSON = async path => {
    const body = await getContent(host, path, headers, { get });
    const data = JSON.parse(body);
    // console.log('Discord done:', Object.keys(data));
    return data;
  };

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
         * @returns { Promise<GuildMember> }
         */
        members(userID) {
          return getJSON(`${api}/guilds/${guildID}/members/${userID}`);
        },
      });
    },
  });
}

const Site = freeze({
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
  start: () => `${Site.top}
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
  authPath: '/auth/discord',

  testnetRoles: {
    'testnet-participant': '819067161371738173',
    'testnet-teammate': '825108158744756226',
    team: '754111409645682700',
  },

  /** @param { GuildMember } mem */
  checkParticipant(mem) {
    const needed = Site.testnetRoles;
    const roles = values(needed).filter(r => mem.roles.includes(r));
    if (roles.length < 1) {
      return `${mem.nick} lacks roles ${JSON.stringify(keys(needed))}`;
    }
    return null;
  },

  checkAuth: (req, res, next) => {
    if (req.isAuthenticated()) return next();
    res.send('not logged in :(');
    return undefined;
  },

  badLoginPath: '/loginRefused',

  badLogin: () => `${Site.top}
  <p><strong>Login refused.</strong> Only Incentivized Testnet participants are allowed.</p>
  <form action="/"><button type="submit">Try again</button></form>
  `,

  avatars: 'https://cdn.discordapp.com/avatars',

  /**
   * Construct upload form.
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
   */
  upload: (
    member,
    { GoogleAccessId, key, bucket, policy, signature, ...headers },
  ) => `
${Site.top}

<h1>Swingset log (slogfile) submission</h1>

<figure>
<img class="avatar"
     src="${Site.avatars}/${member.user?.id}/${member.user?.avatar}.png" />
<figcaption>Welcome <b>${member.nick || 'participant'}</b>.</figcaption>
</figure>

<form action="https://${bucket}.storage.googleapis.com"
      method="post" enctype="multipart/form-data">
      <fieldset><legend>slogfile</legend>
	<input type="hidden" name="bucket" value="${bucket}">
	<input type="hidden" name="GoogleAccessId" value="${GoogleAccessId}">
	<input type="hidden" name="policy" value="${policy}">
	<input type="hidden" name="signature" value="${signature}">
  <input type="hidden" name="Content-Type" value="${headers['Content-Type']}">
  <input type="hidden" name="Content-Disposition" value="${
    headers['Content-Disposition']
  }">
  <label>
    Suggested name: <code><em>moniker</em>-agorictest-<em>NN</em></code>.slog.gz</code><br />
	  <input name="file" type="file">
    <input type="submit" value="Upload">
  </label>
  <p><em><strong>NOTE:</strong> this page lacks feedback on when your upload finishes.</em></p>
	<label><em>storage key:</em> <input type="text" readonly name="key" value="${key}" /></label>
  </fieldset>
</form>
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
    /** @param { string } base */
    callbackURL: base => new URL(self.callbackPath, base).toString(),

    /**
     * @param {discord.StrategyOptions} opts
     */
    strategy: opts =>
      new discord.Strategy(
        opts,
        // TODO: refreshToken handling
        async (_accessToken, _refreshToken, profile, cb) => {
          const { id } = profile;
          const member = await guild.members(id);

          const message = Site.checkParticipant(member);
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
        },
      ),

    /**
     * @param {Express.User} user
     * @param {(e: Error | null, s: string) => string} done
     */
    serializeUser: (user, done) => {
      const member = /** @type { GuildMember } */ (user);
      done(null, JSON.stringify(member));
    },
    /**
     * @param { string } obj
     * @param {(e: any, u: Express.User | false | null | undefined ) => void} done
     */
    deserializeUser: (obj, done) => {
      const member = JSON.parse(obj);
      done(null, member);
    },

    /**
     * @param { GuildMember } member
     * @param { number } freshTime
     */
    formData: (member, freshTime) => {
      const { user } = member;
      if (!user) throw TypeError('corrupted session');

      // ISSUE: # in filename is asking for trouble
      const userID = `${user.username}#${user.discriminator}`;
      const fileName = `${userID}.slog.gz`;
      const dt = new Date(freshTime).toISOString();
      const freshKey = `${dt}-${fileName}`;
      return storage.uploadRequest(fileName, freshKey, true, {});
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
 * }} io
 */
async function main(env, { clock, get, express }) {
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
    // @ts-ignore ???
    session({
      secret: config`SUBM_SESSION_SECRET`,
      resave: false,
      saveUninitialized: false,
    }),
  );

  const storage = gcs(
    config`GCS_PRIVATE_KEY`,
    config`GOOGLE_SERVICES_EMAIL`,
    config`GCS_STORAGE_BUCKET`,
  );
  const discordAPI = DiscordAPI(config`DISCORD_API_TOKEN`, { get });
  const guild = discordAPI.guilds(config`DISCORD_GUILD_ID`);
  const site = makeUploader(guild, storage);

  const aPassport = new passport.Passport();
  aPassport.serializeUser(site.serializeUser);
  aPassport.deserializeUser(site.deserializeUser);
  aPassport.use(
    site.strategy({
      clientID: config`DISCORD_CLIENT_ID`,
      clientSecret: config`DISCORD_CLIENT_SECRET`,
      callbackURL: site.callbackPath,
      scope: ['identify', 'email'],
    }),
  );
  app.use(aPassport.initialize());
  app.use(aPassport.session());
  app.get(Site.authPath, aPassport.authenticate('discord'));
  app.get(
    site.callbackPath,
    aPassport.authenticate('discord', { failureRedirect: Site.badLoginPath }),
    (_req, res) => res.redirect(site.uploadPath), // Successful auth
  );
  app.get(Site.badLoginPath, (_r, res) => res.send(Site.badLogin()));

  app.get(site.uploadPath, Site.checkAuth, (req, res) => {
    const member = /** @type { GuildMember } */ (req.user);
    const formData = site.formData(member, clock());
    // console.log({ formData });
    res.send(Site.upload(member, formData));
  });
  const testerID = '358096357862408195';
  const tester = await guild.members(testerID);
  app.get('/test', (_r, res) => res.send(Site.upload(tester, {})));

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
    },
  ).catch(err => console.error(err));
}
