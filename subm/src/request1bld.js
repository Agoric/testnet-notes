/* eslint-disable no-await-in-loop */
// See https://github.com/Agoric/validator-profiles/wiki/Request-1-BLD

const { DiscordAPI } = require('./discordGuild');

const ADMIN_ROLE_ID = '412648251196702741';

async function* authorizedRequests(channel, guild, role, quorum) {
  const memberDetail = new Map();
  const getMemberDetail = async id => {
    if (memberDetail.has(id)) {
      return memberDetail.get(id);
    }
    const detail = await guild.members(id);
    memberDetail.set(id, detail);
    return detail;
  };

  const messages = await channel.getMessages();
  const hasAddr = messages.filter(msg => msg.content.match(/agoric1/));
  const hasChecks = hasAddr.filter(
    msg => msg.reactions.filter(r => r.emoji.name === '✅').length >= quorum,
  );

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
 *   stdout: typeof import('process').stdout
 * }} io
 */
async function main(env, { stdout, get }) {
  const discordAPI = DiscordAPI(env.DISCORD_API_TOKEN, { get });
  const guild = discordAPI.guilds(env.DISCORD_GUILD_ID);

  const channel = discordAPI.channels(env.CHANNEL_ID);

  for await (const x of authorizedRequests(channel, guild, ADMIN_ROLE_ID, 1)) {
    console.log(x);
  }
}

/* global require, process, module */
if (require.main === module) {
  main(process.env, {
    stdout: process.stdout,
    // eslint-disable-next-line global-require
    get: require('https').get,
  }).catch(err => console.error(err));
}
