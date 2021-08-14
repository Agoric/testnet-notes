/* global Buffer */
// @ts-check

const { freeze } = Object;

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

const avatarBase = 'https://cdn.discordapp.com/avatars';

/** @param { DiscordUser | undefined } user */
function avatar(user) {
  return `${avatarBase}/${user?.id}/${user?.avatar}.png`;
}

/* global module */
module.exports = { DiscordAPI, avatar };
