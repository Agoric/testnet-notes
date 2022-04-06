const { getContent } = require('./discordGuild.js');

const { fromEntries } = Object;

const config = {
  host: 'rpc-agoric.nodes.guru',
  address: 'agoric15qxmfufeyj4zm9zwnsczp72elxsjsvd0vm4q8h',
};

const searchBySender = address =>
  `/tx_search?query="transfer.sender='${address}'"&per_page=100`;

/**
 * @param {{ hash: string, tx_result: { log: string }}[]} txs
 * @returns {{ hash: string, recipient: string, sender: string, amount: string}[]}
 */
const transfers = txs =>
  txs
    .map(({ hash, tx_result: { log: logText } }) => {
      const [{ events }] = JSON.parse(logText);
      if (!events) return [];
      return events
        .filter(({ type }) => type === 'transfer')
        .map(({ attributes }) => ({
          hash,
          ...fromEntries(attributes.map(({ key, value }) => [key, value])),
        }));
    })
    .flat();

/**
 * @param {{
 *   get: typeof import('https').get,
 * }} io
 */
const main = async ({ get }) => {
  const txs = await getContent(
    config.host,
    searchBySender(config.address),
    {},
    { get },
  ).then(txt => JSON.parse(txt).result.txs);

  console.log(transfers(txs.slice(0, 3)));
};

/* global require, module */
if (require.main === module) {
  main({
    // eslint-disable-next-line global-require
    get: require('https').get,
  }).catch(err => console.error(err));
}

module.exports = { searchBySender, transfers };
