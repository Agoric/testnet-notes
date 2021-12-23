/**
 *
 * @param {string[]} argv
 * @param {Record<string, string | undefined>} env
 * @param {Object} io
 * @param {typeof import('google-spreadsheet').GoogleSpreadsheet} io.GoogleSpreadsheet
 */
const main = async (argv, env, { GoogleSpreadsheet }) => {
  // Initialize the sheet - doc ID is the long id in the sheets URL
  const doc = new GoogleSpreadsheet(env.SHEET1_ID); // ISSUE: AMBIENT

  const creds = JSON.parse(env.GCS_SERVICE_ACCOUNT);
  // Initialize Auth - see https://theoephraim.github.io/node-google-spreadsheet/#/getting-started/authentication
  await doc.useServiceAccountAuth(creds);

  await doc.loadInfo(); // loads document properties and worksheets
  console.log(doc.title);
  //   await doc.updateProperties({ title: 'renamed doc' });

  //   const sheet = doc.sheetsByIndex[0]; // or use doc.sheetsById[id] or doc.sheetsByTitle[title]
  //   console.log(sheet.title);
  //   console.log(sheet.rowCount);

  //   // adding / removing sheets
  //   const newSheet = await doc.addSheet({ title: 'hot new sheet!' });
  //   await newSheet.delete();
};

/* global require, process, module */
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
