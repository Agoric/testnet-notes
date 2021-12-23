/* global Buffer */
// @ts-check

const { entries, freeze } = Object;

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

const query = params =>
  entries(params)
    .filter(([_n, value]) => value !== undefined)
    .map(([name, value]) => `${name}=${encodeURI(value)}`)
    .join('&');

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
 *
 * https://discord.com/developers/docs/resources/user#user-object
 * @typedef {{
 *   id: Snowflake,
 *   username: string,
 *   discriminator: string,
 *   avatar?: string,
 *   email?: string, // ... etc.
 * }} UserObject
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
    /**
     * @param { string } userID
     * @returns { Promise<UserObject> }
     */
    users: userID => getJSON(`${api}/users/${userID}`),
    /** @param { string } guildID */
    guilds(guildID) {
      return freeze({
        /** @returns { Promise<unknown> } */
        info() {
          return getJSON(`${api}/guilds/${guildID}`);
        },
        roles: () => getJSON(`${api}/guilds/${guildID}/roles`),
        /**
         * @param { string } userID
         * @returns { Promise<GuildMember> }
         */
        members(userID) {
          return getJSON(`${api}/guilds/${guildID}/members/${userID}`);
        },
        /**
         * @param {{ limit?: number, after?: string}=} opts
         * @returns { Promise<GuildMember[]> }
         */
        membersList({ limit, after } = {}) {
          return getJSON(
            `${api}/guilds/${guildID}/members?${query({ limit, after })}`,
          );
        },
      });
    },
  });
}

const avatarBase = 'https://cdn.discordapp.com/avatars';

/** @param { DiscordUser | undefined } user */
function avatar(user) {
  if (!user) return '/no-avatar???';
  return `${avatarBase}/${user.id}/${user.avatar}.png`;
}

/**
 * @param {ReturnType<ReturnType<DiscordAPI>['guilds']>} guild
 */
async function pagedMembers(guild) {
  /** @type {GuildMember[][]} */
  const pages = [];
  const limit = 1000;
  let after;
  do {
    console.error('getting page', pages.length, after);
    // eslint-disable-next-line no-await-in-loop
    const page = await guild.membersList({ limit, after });
    if (!page.length) break;
    after = page.slice(-1)[0].user.id;
    pages.push(page);
  } while (after);
  return pages.flat();
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
 * @param {Record<string, string | undefined>} env
 * @param {{
 *   get: typeof import('https').get,
 *   stdout: typeof import('process').stdout
 * }} io
 */
async function main(env, { stdout, get }) {
  const config = makeConfig(env);
  const discordAPI = DiscordAPI(config`DISCORD_API_TOKEN`, { get });
  const guild = discordAPI.guilds(config`DISCORD_GUILD_ID`);

  const roles = await guild.roles();
  stdout.write(JSON.stringify(roles, null, 2));

  const members = await pagedMembers(guild);
  stdout.write(JSON.stringify(members, null, 2));
}

/* global require, process */
if (require.main === module) {
  main(process.env, {
    stdout: process.stdout,
    // eslint-disable-next-line global-require
    get: require('https').get,
  }).catch(err => console.error(err));
}

/* global module */
module.exports = { DiscordAPI, avatar };
