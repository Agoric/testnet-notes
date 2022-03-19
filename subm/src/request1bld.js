/* eslint-disable no-await-in-loop */
// See https://github.com/Agoric/validator-profiles/wiki/Request-1-BLD

const { DiscordAPI } = require('./discordGuild');

const ADMIN_ROLE_ID = '412648251196702741';

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
  const messages = await channel.getMessages();
  for (const message of messages) {
    if (message.content.match(/agoric1/)) {
      console.log(message);
      console.log(message.reactions);
      for (const reaction of message.reactions) {
        if (reaction.emoji.name === '✅') {
          console.log('endorsement!!');
          const endorsements = await channel
            .messages(message.id)
            .reactions(encodeURIComponent('✅'));
          console.log(endorsements);
          for (const endorsement of endorsements) {
            console.log(
              `${message.content} endorsed by ${endorsement.username}`,
            );
            const detail = await guild.members(endorsement.id);
            console.log({ detail });
            if (detail.roles.includes(ADMIN_ROLE_ID)) {
              console.log('endorsed by admin!');
            }
          }
        }
      }
    }
  }

  // const roles = await guild.roles();
  // console.log(roles);
}

/* global require, process */
if (require.main === module) {
  main(process.env, {
    stdout: process.stdout,
    // eslint-disable-next-line global-require
    get: require('https').get,
  }).catch(err => console.error(err));
}
