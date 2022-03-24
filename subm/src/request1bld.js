/* eslint-disable no-await-in-loop */
// See https://github.com/Agoric/validator-profiles/wiki/Request-1-BLD

const { DiscordAPI } = require('./discordGuild');

const fail = () => {
  throw Error();
};

/**
 * @param {ReturnType<ReturnType<typeof DiscordAPI>['channels']>} channel
 * @param {ReturnType<ReturnType<typeof DiscordAPI>['guilds']>} guild
 * @param {Snowflake} role
 * @param {number} quorum
 * @yields {{ message: Message, endorsers: User[] }}
 * @typedef {import('./discordGuild').Snowflake} Snowflake
 */
async function* authorizedRequests(channel, guild, role, quorum) {
  /** @type {Map<Snowflake, import('./discordGuild').GuildMember>} */
  const memberDetail = new Map();
  /** @param {Snowflake} id */
  const getMemberDetail = async id => {
    if (memberDetail.has(id)) {
      return memberDetail.get(id) || fail();
    }
    const detail = await guild.members(id);
    // console.log(detail);
    memberDetail.set(id, detail);
    return detail;
  };

  const messages = await channel.getMessages({ limit: 100 });
  const hasAddr = messages.filter(msg => msg.content.match(/agoric1/));
  if (!hasAddr) return;
  const hasChecks = hasAddr.filter(msg => {
    const [checks] = (msg.reactions || []).filter(r => r.emoji.name === '✅');
    return (checks || {}).count >= quorum;
  });
  if (!hasChecks) return;

  for (const msg of hasChecks) {
    const endorsements = await channel.messages(msg.id).reactions('✅');
    const endorsers = [];
    for (const endorsement of endorsements) {
      const detail = await getMemberDetail(endorsement.id);
      if (detail.roles.includes(role)) {
        endorsers.push(detail);
      }
    }
    if (endorsers.length >= quorum) {
      yield { message: msg, endorsers };
    }
  }
}

/**
 * @param {Record<string, string | undefined>} env
 * @param {{
 *   get: typeof import('https').get,
 *   setTimeout: typeof setTimeout,
 * }} io
 */
async function main(env, { get, setTimeout }) {
  const discordAPI = DiscordAPI(env.DISCORD_API_TOKEN, { get, setTimeout });
  const guild = discordAPI.guilds(env.DISCORD_GUILD_ID);

  // to get mod-1-bld role id:
  // console.log(await guild.roles());

  const channel = discordAPI.channels(env.CHANNEL_ID);

  const header = ['timestamp', 'msgID', 'requestor', 'address', 'endorsers'];
  console.log(header.join(','));
  for await (const { message: msg, endorsers } of authorizedRequests(
    channel,
    guild,
    env.REVIEWER_ROLE_ID,
    2,
  )) {
    const [_, address] = msg.content.match(/(agoric1\S+)/);
    const label = user => `${user.username}#${user.discriminator}`;
    const ok = endorsers.map(u => label(u.user)).join(' ');
    console.log(
      `${msg.timestamp},${msg.id},${label(msg.author)},${address},${ok}`,
    );
  }
}

/* global require, process, module */
if (require.main === module) {
  main(process.env, {
    // eslint-disable-next-line global-require
    get: require('https').get,
    setTimeout,
  }).catch(err => console.error(err));
}
