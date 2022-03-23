const { getContent } = require('./discordGuild.js');

const searchBySender = address =>
  `/tx_search?query="transfer.sender='${address}'"`;

const config = {
  host: 'rpc-agoric.nodes.guru',
  address: 'agoric15qxmfufeyj4zm9zwnsczp72elxsjsvd0vm4q8h',
};

/**
 * @param {{
 *   get: typeof import('https').get,
 * }} io
 */
const main = async ({ get }) => {
  const txt = await getContent(
    config.host,
    searchBySender(config.address),
    {},
    { get },
  );
  const data = JSON.parse(txt);
  console.log(data);
};

/* global require, module */
if (require.main === module) {
  main({
    // eslint-disable-next-line global-require
    get: require('https').get,
  }).catch(err => console.error(err));
}
