/* eslint-disable no-await-in-loop */
// See https://github.com/Agoric/validator-profiles/wiki/Request-1-BLD

const { DiscordAPI, getContent, paged } = require('./discordGuild.js');
const { upsert } = require('./sheetAccess');
const { searchBySender, transfers } = require('./tendermintRPC');

const config = {
  host: 'rpc-agoric.nodes.guru',
  address: 'agoric15qxmfufeyj4zm9zwnsczp72elxsjsvd0vm4q8h',
  quorum: 2,
};

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

  const messages = await paged(channel.getMessages);
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
      if (detail && detail.roles && detail.roles.includes(role)) {
        endorsers.push(detail);
      }
    }
    if (endorsers.length >= quorum) {
      const [_, address] = msg.content.match(/(agoric1\S+)/);
      if (typeof address !== 'string') throw TypeError(address);
      yield { message: msg, address, endorsers };
    }
  }
}

/**
 * @param {ReturnType<ReturnType<typeof DiscordAPI>['channels']>} channel
 * @param {ReturnType<ReturnType<typeof DiscordAPI>['guilds']>} guild
 * @param {string} roleID
 * @param {{
 *   get: typeof import('https').get,
 * }} io
 */
async function requestStatus(channel, guild, roleID, { get }) {
  const txs = await getContent(
    config.host,
    searchBySender(config.address),
    {},
    { get },
  ).then(txt => JSON.parse(txt).result.txs);

  const txfrs = transfers(txs);
  // console.log(txfrs);
  const byRecipient = new Map(txfrs.map(txfr => [txfr.recipient, txfr]));
  // console.log(byRecipient.keys());

  const result = [];
  for await (const {
    message: { id, timestamp, author },
    address,
    endorsers,
  } of authorizedRequests(channel, guild, roleID, config.quorum)) {
    const hash = byRecipient.has(address)
      ? byRecipient.get(address).hash
      : undefined;
    result.push({
      message: { id, timestamp, author },
      address,
      endorsers,
      hash,
    });
  }
  return result;
}

const label = user => `${user.username}#${user.discriminator}`;

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

  const txs = await getContent(
    config.host,
    searchBySender(config.address),
    {},
    { get },
  ).then(txt => JSON.parse(txt).result.txs);

  const txfrs = transfers(txs);
  // console.log(txfrs);
  const byRecipient = new Map(txfrs.map(txfr => [txfr.recipient, txfr]));
  // console.log(byRecipient.keys());

  const header = [
    'timestamp',
    'msgID',
    'requestor',
    'address',
    'endorsers',
    'hash',
  ];
  console.log(header.join(','));
  for await (const { message: msg, address, endorsers } of authorizedRequests(
    channel,
    guild,
    env.REVIEWER_ROLE_ID,
    2,
  )) {
    const ok = endorsers.map(u => label(u.user)).join(' ');
    const hash = byRecipient.has(address) ? byRecipient.get(address).hash : '';
    console.log(
      `${msg.timestamp},${msg.id},${label(
        msg.author,
      )},${address},${ok},${hash}`,
    );
  }
}

/**
 * @param {string[]} args
 * @param {Record<string, string | undefined>} env
 * @param {Object} io
 * @param {typeof import('google-spreadsheet').GoogleSpreadsheet} io.GoogleSpreadsheet
 * @param {typeof import('https').get} io.get
 * @param {typeof setTimeout} io.setTimeout
 */
const main2 = async (args, env, { get, setTimeout, GoogleSpreadsheet }) => {
  const discordAPI = DiscordAPI(env.DISCORD_API_TOKEN, { get, setTimeout });
  const guild = discordAPI.guilds(env.DISCORD_GUILD_ID);

  // to get mod-1-bld role id:
  // console.log(await guild.roles());

  const channel = discordAPI.channels(env.CHANNEL_ID);

  const creds = {
    client_email: env.GOOGLE_SERVICES_EMAIL,
    private_key: env.GCS_PRIVATE_KEY,
  };
  // Initialize the sheet - doc ID is the long id in the sheets URL
  const doc = new GoogleSpreadsheet(env.SHEET_1BLD_ID);
  // Initialize Auth - see https://theoephraim.github.io/node-google-spreadsheet/#/getting-started/authentication
  await doc.useServiceAccountAuth(creds);
  await doc.loadInfo(); // loads document properties and worksheets
  console.log(doc.title);
  const sheet = doc.sheetsByIndex[0];

  const memberLabel = mem => mem.nick || label(mem.user);
  for await (const { message: msg, address, endorsers } of authorizedRequests(
    channel,
    guild,
    env.REVIEWER_ROLE_ID,
    2,
  )) {
    upsert(sheet, address, {
      Request: `https://discord.com/channels/585576150827532298/946137891023777802/${msg.id}`,
      At: msg.timestamp.slice(0, '1999-01-01T12:59'.length).replace('T', ' '),
      By: label(msg.author),
      To: address,
      Reviewers: endorsers.map(memberLabel).join(','),
    });
  }
};

/* global require, process, module */
if (require.main === module) {
  main2(
    process.argv.slice(2),
    { ...process.env },
    {
      // eslint-disable-next-line global-require
      get: require('https').get,
      setTimeout,
      // eslint-disable-next-line global-require
      GoogleSpreadsheet: require('google-spreadsheet').GoogleSpreadsheet, // please excuse CJS
    },
  ).catch(err => console.error(err));
}

module.exports = { requestStatus };
