const range = n => [...Array(n).keys()];

/**
 * @param {GoogleSpreadsheetWorksheet} sheet
 * @param {string | number} key
 */
const lookup = async (sheet, key) => {
  // load primary key column
  await sheet.loadCells({
    startColumnIndex: 0,
    endColumnIndex: 1,
  });

  const destIndex = range(sheet.rowCount).find(rowIndex => {
    if (rowIndex === 0) return false; // header row
    const { value } = sheet.getCell(rowIndex, 0);
    return key === value || value === null;
  });
  if (destIndex < 0) throw Error('key not found and no available rows');
  const rows = await sheet.getRows({ offset: destIndex - 1, limit: 1 });
  return rows.length ? rows[0] : undefined;
};

/**
 * @param {GoogleSpreadsheetWorksheet} sheet
 * @param {string | number} key
 * @param {Record<string, string | number>} record
 * @typedef {import('google-spreadsheet').GoogleSpreadsheetWorksheet} GoogleSpreadsheetWorksheet
 */
const upsert = async (sheet, key, record) => {
  let row = await lookup(sheet, key);
  if (row) {
    Object.assign(row, record);
    await row.save({ raw: true });
  } else {
    row = await sheet.addRow(record);
  }
  return row;
};

/**
 * @param {string[]} argv
 * @param {Record<string, string | undefined>} env
 * @param {Object} io
 * @param {typeof import('google-spreadsheet').GoogleSpreadsheet} io.GoogleSpreadsheet
 */
const main = async (argv, env, { GoogleSpreadsheet }) => {
  // Initialize the sheet - doc ID is the long id in the sheets URL
  const doc = new GoogleSpreadsheet(env.SHEET1_ID);

  const creds = {
    client_email: env.GOOGLE_SERVICES_EMAIL,
    private_key: env.GCS_PRIVATE_KEY,
  };
  // Initialize Auth - see https://theoephraim.github.io/node-google-spreadsheet/#/getting-started/authentication
  await doc.useServiceAccountAuth(creds);

  await doc.loadInfo(); // loads document properties and worksheets
  console.log(doc.title);

  const sheet = doc.sheetsByIndex[0]; // or use doc.sheetsById[id] or doc.sheetsByTitle[title]
  console.log(sheet.title);
  console.log(sheet.rowCount);

  await upsert(sheet, '358096357862408195', {
    userID: '358096357862408195',
    email: 'dckc@agoric.com',
  });
};

/* global require, process */
if (require.main === module) {
  main(
    process.argv.slice(2),
    { ...process.env },
    {
      // eslint-disable-next-line global-require
      GoogleSpreadsheet: require('google-spreadsheet').GoogleSpreadsheet, // please excuse CJS
    },
  ).catch(err => console.error(err));
}

/* global module */
module.exports = { lookup, upsert };
