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
const { lookup, upsert } = require('./sheetAccess.js');
const { makeConfig } = require('./config.js');
const { requestStatus } = require('./request1bld.js');

const { freeze, keys, values, entries } = Object; // please excuse freeze vs. harden

/** @type {(digits: string) => string} */
const fmtNum = digits => parseInt(digits, 10).toLocaleString();

const AgoricStyle = freeze({
  top: `
  <!doctype html>
  <head>
  <title>Agoric Submission Tool</title>
  <style>
    label { display: block; padding: .5em }
    .avatar {
      border-radius: 10%; padding: 5x; border 1px solid #ddd;
    }
    table {
      border-collapse: collapse;
    }
    .success {
      color: #4F8A10;
      background-color: #DFF2BF;
      font-size: x-large;
    }
    .warning {
      color: #9F6000;
      background-color: #FEEFB3;
      font-size: x-large;
    }
  </style>
  </head>

  <div class="container">
  <nav>
  <a href="https://agoric.com/"
  ><img alt="Agoric" align="bottom"
     src="https://agoric.com/wp-content/themes/agoric_2021_theme/assets/img/logo.svg"
      /></a>
  <br />
  <hr />
  <address>
  <a href="https://github.com/Agoric/testnet-notes/tree/main/subm">source code and issues on github</a>
  </address>
  </nav>
  `,
});

const Site = freeze({
  path: {
    login: '/auth/discord',
    callback: '/auth/discord/callback',
    badLogin: '/loginRefused',
    contactForm: '/community/contact',
    request1bld: '/community/request1bld',
    uploadSlog: '/participant/slogForm',
    uploadSuccess: '/participant/slogOK',
    loadGenKey: '/participant/loadGenKey',
  },
  /**
   * @param { string } base
   * @param { string } protocol
   * @param { string } host
   * @returns
   */
  uploadSuccessURL: (base, protocol, host) =>
    `${new URL(
      Site.path.uploadSuccess,
      host === 'localhost' ? base : `${protocol}://${host}`,
    )}`,

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
<h2>Community Participants</h1>

<form action="/auth/discord">
<fieldset>
<legend>To Submit Contact Info</legend>
<large>
<button type="submit">Login via Discord</button>
</large>
</fieldset>
</form>
</div>

<h2>Incentivized Testnet Participants</h1>

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
    Community: '757035496198111262',
    // TODO: support for many-to-one discord to moniker
    // 'testnet-teammate': '825108158744756226',
    team: '754111409645682700',
  },

  badLogin: () => `${AgoricStyle.top}
  <p><strong>Login refused.</strong> Only ${keys(Site.testnetRoles).join(
    '/',
  )} members are allowed.</p>
  <form action="/"><button type="submit">Try again</button></form>
  `,

  /** @param { GuildMember } member */
  welcome: member => `
    <figure>
    <img class="avatar" src="${avatar(member.user)}" />
    <figcaption>Welcome <b>${member.nick ||
      (member.user || {}).username ||
      'participant'}</b>.</figcaption>
    </figure>
  `,

  /**
   * @param { string } name
   * @param { 'yes' | 'no' | undefined } value
   */
  yesNo: (name, value) => `
  <ul style="list-style: none;">
    <li><label><input type="radio" name="${name}"
                  value="yes" ${value === 'yes' ? 'checked' : ''}
                /> Yes</label></li>
    <li><label><input type="radio" name="${name}"
                value="no" ${value === 'no' ? 'checked' : ''}
              /> No</label></li>
 </ul>
  `,
  /**
   * @param { GuildMember } member
   * @param { ContactInfo } contactInfo
   * @param { 'ok' | 'incomplete' | undefined } ack
   */
  contactForm: (
    member,
    {
      email,
      fullName,
      countryOfResidence,
      interestInAgoric,
      agreeToBeContacted,
      agreeToReceiveNewsletter,
    },
    ack,
  ) => `${AgoricStyle.top}

<h1>Contact Info</h1>

${Site.welcome(member)}

<form method="POST" >
  <fieldset><legend>Contact Info</legend>
    ${
      ack === 'ok'
        ? `<p class="success"><b>Contact info updated.</b> Thank you.</p>`
        : ''
    }
    ${
      ack === 'incomplete'
        ? `<p class="warning">Contact info <b>incomplete. Please provide all fields.</b></p>`
        : ''
    }
    <label>*Email: <input name="email" type="email"
     value="${email || ''}"/></label>
    <label>*Full Name: <input name="fullName"
     value="${fullName || ''}"/></label>
    <label>Briefly describe your interest in Agoric:<br />
     <textarea name="interestInAgoric" rows="6" columns="60">${interestInAgoric ||
       ''}</textarea>
      </label>
    <label>*Country of residence: <input name="countryOfResidence"
      value="${countryOfResidence || ''}"/></label>
    <label>*I agree to be contacted by Agoric regarding opportunities:
     ${Site.yesNo('agreeToBeContacted', agreeToBeContacted)}</label>
    <label>*I agree to receive the Agoric Newsletter to keep me updated on Agoric progress and news:
    ${Site.yesNo('agreeToReceiveNewsletter', agreeToReceiveNewsletter)}</label>
    <input type="submit" value="Submit">
  </fieldset>
</form>
  `,

  exploreAddr: addr =>
    `<small><code><a href="https://agoric.bigdipper.live/accounts/${addr}">${addr}</a></code></small>`,
  exploreTx: hash =>
    hash
      ? `<small><code><a href="https://agoric.bigdipper.live/transactions/${hash}"
      >${hash.slice(0, 16)}...</a></code></small>`
      : '',

  /** @param {Awaited<ReturnType<typeof requestStatus>>} requests */
  requestStatus: requests => `${AgoricStyle.top}
  <h1>1 BLD Request Status</h1>
  <div>background: <a href="https://github.com/Agoric/validator-profiles/wiki/Request-1-BLD">Request 1 BLD</a></div>
  <table border=1>
  <thead><th>Request</th><th>To</th><th>Tx?</th></thead>
  <tbody>
    ${requests
      .map(
        ({ message: { id, author, timestamp }, address, hash }) => `
      <tr>
        <td>
          <a href="https://discord.com/channels/585576150827532298/946137891023777802/${id}">
          ${timestamp.slice(0, '1999-01-01T12:59'.length).replace('T', ' ')}
            <b title='${author.id}'>${(author || {}).username || 'author'}</b>
        </td>
        <td>${Site.exploreAddr(address)}</td>
        <td>${Site.exploreTx(hash)}</td>
      </tr>
    `,
      )
      .join('\n')}
  </tbody>
  </table>
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
    <div><h3>Your Existing Files</h3>
    <table border>
    <thead><tr><th>Name</th><th>Size</th><th>md5Hash</th></tr></thead>
    <tbody>
    ${files
      .sort((a, b) => (a.name < b.name ? -1 : 1))
      .map(
        ({ name, metadata: { md5Hash, size } }) =>
          `<tr>
          <td>${name}</td>
          <td align="right">${fmtNum(size)}</td>
          <td><code>${md5Hash}</code></td>
          </tr>`,
      )
      .join('\n')}
    </tbody>
    </table>
    </div>
    `,

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

${Site.fileList(files)}

<h2>Load Generator Key</h1>
<p>See: <a href='${Site.path.loadGenKey}'>load generator key</a>.</p>
`,
});

/**
 * @param {GoogleSpreadsheetWorksheet} sheet
 * @param {GuildMember} member
 * @typedef {{
 *   email?: string,
 *   fullName?: string,
 *   countryOfResidence?: string,
 *   interestInAgoric?: string,
 *   agreeToBeContacted?: 'yes' | 'no',
 *   agreeToReceiveNewsletter?: 'yes' | 'no',
 * }} ContactInfo
 */
function makeContact(sheet, member) {
  const { user } = member;
  if (!user) throw TypeError('user undefined');
  return freeze({
    member,
    /**
     * @returns { Promise<ContactInfo> }
     * @throws on not found
     */
    getContactInfo: async () => {
      const row = await lookup(sheet, user.id);
      const {
        email,
        fullName,
        countryOfResidence,
        interestInAgoric,
        agreeToBeContacted,
        agreeToReceiveNewsletter,
      } = row;
      return {
        email,
        fullName,
        countryOfResidence,
        interestInAgoric,
        agreeToBeContacted,
        agreeToReceiveNewsletter,
      };
    },

    /**
     * @param {ContactInfo} contactInfo
     */
    setContactInfo: contactInfo =>
      upsert(sheet, user.id, {
        userID: user.id,
        joined_at: member.joined_at,
        nick: member.nick || (member.user || {}).username || '<nick???>',
        detail: JSON.stringify(member, null, 2),
        ...contactInfo,
      }),
  });
}

/** @param {ContactInfo} contactInfo */
const contactInfoComplete = ({
  email,
  fullName,
  countryOfResidence,
  agreeToBeContacted,
  agreeToReceiveNewsletter,
}) =>
  email &&
  fullName &&
  countryOfResidence &&
  agreeToBeContacted &&
  agreeToReceiveNewsletter;

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

    myFiles: async () => {
      const [files] = await objectStore.bucket(bucket).getFiles();
      return files.filter(f => f.name.includes(userID));
    },

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
 * @param { GoogleSpreadsheetWorksheet } powers.contactSheet
 * @typedef {import('./discordGuild.js').Snowflake} Snowflake
 * @typedef { import('google-spreadsheet').GoogleSpreadsheetWorksheet} GoogleSpreadsheetWorksheet
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
  const reviveMember = member => {
    // ISSUE: make testnet participant powers available based on role.
    // eslint-disable-next-line no-constant-condition
    const tester = false
      ? makeTestnetParticipant(
          member,
          powers.bucketName,
          powers.objectStore,
          powers.loadGenAdmin,
        )
      : {};
    return freeze({
      ...makeContact(powers.contactSheet, member),
      ...tester,
    });
  };

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
 * @param {{
 *   clock: () => number,
 *   get: typeof import('https').get,
 *   setTimeout: typeof setTimeout,
 *   express: typeof import('express'),
 *   GoogleSpreadsheet: typeof import('google-spreadsheet').GoogleSpreadsheet,
 *   makeStorage: (...args: unknown[]) => StorageT,
 *   admin: typeof import('firebase-admin')
 * }} io
 * @typedef { import('@google-cloud/storage').Storage } StorageT
 */
async function main(
  env,
  { clock, get, setTimeout, express, makeStorage, admin, GoogleSpreadsheet },
) {
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

  const doc = new GoogleSpreadsheet(config`SHEET1_ID`);

  await doc.useServiceAccountAuth({
    client_email: config`GOOGLE_SERVICES_EMAIL`,
    private_key: config`GCS_PRIVATE_KEY`,
  });
  await doc.loadInfo();

  const discordAPI = DiscordAPI(config`DISCORD_API_TOKEN`, { get, setTimeout });
  const guild = discordAPI.guilds(config`DISCORD_GUILD_ID`);
  const loadGenAdmin = makeFirebaseAdmin(admin, getFirebaseConfig(config));
  loadGenAdmin.init();
  const bot = makeDiscordBot(
    guild,
    Site.testnetRoles,
    {
      clientID: config`DISCORD_CLIENT_ID`,
      clientSecret: config`DISCORD_CLIENT_SECRET`,
      callbackURL: Site.path.callback,
      scope: ['identify', 'email'],
    },
    {
      bucketName: config`GCS_STORAGE_BUCKET`,
      // ASSUME we are running in an environment which supports Application Default Credentials
      objectStore: makeStorage(),
      loadGenAdmin,
      contactSheet: doc.sheetsByIndex[0],
    },
  );

  // OAuth 2 flow
  const { aPassport, loginHandler, callbackHandler } = bot.passport(
    Site.path.badLogin,
  );
  app.use(aPassport.initialize());
  app.use(aPassport.session());
  app.use(express.urlencoded({ extended: true }));
  app.get(Site.path.login, loginHandler);
  app.get(
    Site.path.callback,
    callbackHandler,
    (_req, res) => res.redirect(Site.path.contactForm), // Successful auth. ISSUE: slogForm
  );
  app.get(Site.path.badLogin, (_r, res) => res.send(Site.badLogin()));

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

  app.get(Site.path.contactForm, loginCheck, async (req, res) => {
    const contact = /** @type { ReturnType<typeof makeContact> } */ (req.user);
    const { user } = contact.member;
    if (!user) throw Error('no user');
    let contactInfo = {};
    try {
      contactInfo = await contact.getContactInfo();
    } catch (_notFound) {
      // never mind;
    }
    const page = Site.contactForm(contact.member, contactInfo, req.query.ack);
    res.send(page);
  });
  app.post(Site.path.contactForm, loginCheck, async (req, res) => {
    const contact = /** @type { ReturnType<typeof makeContact> } */ (req.user);
    const {
      email,
      fullName,
      countryOfResidence,
      interestInAgoric,
      agreeToBeContacted,
      agreeToReceiveNewsletter,
    } = req.body;
    if (typeof email !== 'string') throw TypeError(email);
    const contactInfo = {
      email,
      fullName,
      countryOfResidence,
      interestInAgoric,
      agreeToBeContacted,
      agreeToReceiveNewsletter,
    };
    await contact.setContactInfo(contactInfo);
    res.redirect(
      `${Site.path.contactForm}?ack=${
        contactInfoComplete(contactInfo) ? 'ok' : 'incomplete'
      }`,
    );
  });

  const channel = discordAPI.channels(config`CHANNEL_ID`);
  const roleID = config`REVIEWER_ROLE_ID`;
  app.get(Site.path.request1bld, async (req, res) => {
    try {
      const status = await requestStatus(channel, guild, roleID, { get });
      res.send(Site.requestStatus(status));
    } catch (err) {
      handleError(res, req.baseUrl, err);
    }
  });

  // Upload form
  // Note the actual upload request goes directly to Google Cloud Storage.
  app.get(
    `${Site.path.uploadSlog}disabled@@@`,
    loginCheck,
    async (req, res) => {
      try {
        const participant =
          /** @type { ReturnType<typeof makeTestnetParticipant> } */ (req.user);
        const policy = await participant.uploadPolicy(
          new Date(clock()),
          Site.uploadSuccessURL(base, req.protocol, req.hostname),
        );
        const files = await participant.myFiles();
        const page = Site.uploadSlog(participant.member, policy, files);
        res.send(page);
      } catch (err) {
        handleError(res, req.baseUrl, err);
      }
    },
  );
  app.get(Site.path.uploadSuccess, loginCheck, async (req, res) => {
    const participant =
      /** @type { ReturnType<typeof makeTestnetParticipant> } */ (req.user);
    const files = await participant.myFiles();
    res.send(Site.fileList(files));
  });

  app.get(Site.path.loadGenKey, loginCheck, async (req, res) => {
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
      setTimeout,
      makeStorage: (C => (...args) => new C(...args))(
        require('@google-cloud/storage').Storage,
      ),
      admin: require('firebase-admin'),
      // eslint-disable-next-line global-require
      GoogleSpreadsheet: require('google-spreadsheet').GoogleSpreadsheet,
    },
  ).catch(err => console.error(err));
}
