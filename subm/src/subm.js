/* global require, Buffer */
// @ts-check

const { assert } = require('console');
const discord = require('passport-discord');

const { freeze } = Object;

/**
 * @param {string} token
 * @param {{ get: typeof import('https').get }} io
 */
function DiscordAPI(token, { get }) {
  // cribbed from rchain-dbr/o2r/gateway/server/main.js
  const host = 'discordapp.com';
  const api = '/api/v6';
  const headers = { Authorization: `Bot ${token}` };

  function getJSON(path) {
    console.log('calling Discord API', { host, path, headers });
    return new Promise((resolve, reject) => {
      const req = get({ host, path, headers }, res => {
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
            console.log('Discord done:', Object.keys(data));
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
    guilds(guildID) {
      return freeze({
        info() {
          return getJSON(`${api}/guilds/${guildID}`);
        },
        members(userID) {
          return getJSON(`${api}/guilds/${guildID}/members/${userID}`);
        },
      });
    },
  });
}

const Pages = {
  start: `
<!doctype html>
<title>Agoric Testnet Submission</title>

<a href="/auth/discord">login to discord and upload</a>

<hr />
<a href="dckc">dckc discord details</a>
`,
};

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
 *
 * @param {Record<string, string | undefined>} env
 * @param {{
 *   get: typeof import('https').get,
 *   express: typeof import('express'),
 *   passport: typeof import('passport'),
 * }} io
 */
async function main(env, { get, express, passport }) {
  const host = env.HOST || 'localhost';
  const port = parseInt(env.PORT || '3000', 10);
  const base = `http://localhost:${port}`;

  const app = express();
  app.use(passport.initialize());

  const api = DiscordAPI(the(env.TOKEN), { get });
  const guild = api.guilds(the(env.GUILD_ID));

  const scopes = ['identify', 'email', 'guilds', 'guilds.join'];
  const callbackPath = '/auth/discord/callback';
  const callbackURL = new URL(callbackPath, base).toString();

  passport.serializeUser((user, done) => done(null, user));
  passport.deserializeUser((obj, done) => done(null, obj));

  passport.use(
    new discord.Strategy(
      {
        clientID: the(env.CLIENT_ID),
        clientSecret: the(env.CLIENT_SECRET),
        callbackURL,
        scope: scopes,
      },
      async (accessToken, refreshToken, profile, cb) => {
        try {
          const user = await guild.members(profile.id);
          console.log('@@@user', user);
          cb(null, `${user.username}#${user.discriminator}`);
        } catch (err) {
          cb(err);
        }
      },
    ),
  );

  app.get('/', (_req, res) => res.send(Pages.start));
  app.get('/auth/discord', passport.authenticate('discord'));
  app.get(
    callbackPath,
    passport.authenticate('discord', {
      failureRedirect: '/',
    }),
    (_req, res) => {
      res.redirect('/uploadForm'); // Successful auth
    },
  );

  app.get('/dckc', (req, res) => {
    DiscordAPI(env.TOKEN || '', { get })
      .guilds(env.GUILD_ID || '')
      .members(env.USER_ID || '')
      .then(member => {
        // res.type('text/plain');
        res.set('Content-Type', 'text/plain');
        res.send(JSON.stringify(member, null, 2));
      })
      .catch(oops => {
        console.log(oops);
      });
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
      express: require('express'),
      passport: require('passport'),
      get: require('https').get,
    },
  ).catch(err => console.error(err));
}
