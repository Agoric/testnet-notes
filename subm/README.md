# subm - Agoric Testnet Admin based on Discord Auth

Features:
  - upload slogfiles
  - generate loadgen credentials

## Deployment, Configuration: Google App Engine

`yarn start` starts the app in local development mode,
presuming you have done `yarn install`.

`app.yaml` lists several groups of credentials needed for
access to discord, Google Cloud Storage, and Firebase.
Plus, the app assumes a bunch of stuff is configured,
for example, using the Google Cloud console.

`yarn deploy` will prompt you to login the first time.

## Related work

  - https://github.com/conzorkingkong/cloud-storage
  - https://www.npmjs.com/package/gcs-signed-urls

## Contributing: Coding Style

Please follow [Agoric SDK coding style](https://github.com/Agoric/agoric-sdk/wiki/Coding-Style)
including `@agoric/eslint-config`.

### Road Not Taken: Cloudflare Workers

discord auth in the browser doesn't seem to be a thing.
