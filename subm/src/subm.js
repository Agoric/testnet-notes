/* global require, Buffer */

const discord = require('passport-discord');

const { freeze } = Object;

// cribbed from rchain-dbr/o2r/gateway/server/main.js
function DiscordAPI(get, token) {
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

/* global module */
if (require.main === module) {
  // ocap: Import powerful references only when invoked as a main module.
  /* eslint-disable global-require */
  /* global process */
  const get = require('https').get;
  const env = process.env;
  DiscordAPI(get, env.TOKEN || '')
    .guilds(env.GUILD_ID || '')
    .members(env.USER_ID || '')
    .then(member => {
      console.log({ member });
    })
    .catch(oops => {
      console.log(oops);
    });
}
