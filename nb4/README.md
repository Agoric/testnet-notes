# nb4 -- Agoric Incentivized Testnet Data Analysis

## Phase 4.5

We evaluate [phase 4.5 tasks](https://validate.agoric.com/3080879a304847cca5af9d96c04d2bd3) and issues using the code below. The main product, in progress, is:
 - [Phase 4.5 Analysis](https://docs.google.com/spreadsheets/d/1DjKQuBzHLw6slGuXumaYHBjtUEHhWcv1IP_g7-jp7_U/edit?usp=sharing) in Google sheets, **open for comment**.

 - **gentx_clean.py** - clean up **Create and submit your gentx** submissions
   - remove dups
   - fix some JSON parsing problems introduced by the portal
   - extract moniker
 - **slogfile-node-task** - looks at two of tasks:
   - **Create and submit your gentx** using data from `gentx_clean`
   - **Capture and submit a slog file** using files collected from https://submit.agoric.app/
 - **block-explore-uptime** - looks at two tasks using data from the [testnet explorer](https://testnet.explorer.agoric.net/) mongodb:
   - **Start your validator as part of the genesis ceremony**
   - **Maintain uptime during phase!**
 - **Makefile** - extract `.py` code from `.ipynb` for version control


### Google Cloud Infrastructure

In this phase, we take advantage of Google Cloud infrastructure: Cloud Storage, [Notebooks], and (https://cloud.google.com/notebooks/docs/introduction), Big Query. _Details are not documented for public consumption._

## Phase 4

One of the [Phase 4 Tasks](https://validate.agoric.com/6146e366ad6149818b6f45982b883578) was to create and submit a slogfile. So we have **172 slogfiles, ~500MB ea**:

![image](https://user-images.githubusercontent.com/150986/125867744-5b143aa6-5c0e-4ae6-8616-d3e9100298d6.png)

  | st_size | lines
-- | -- | --
count | 172 | 172
mean | 4.8E+08 | 9969421
std | 4.6E+08 | 9925292
min | 1839335 | 2665
25.00% | 3.1E+07 | 1632114
50.00% | 4.9E+08 | 4733944
75.00% | 8.9E+08 | 1.8E+07
max | 1.8E+09 | 3.5E+07

One goal is to:

 - [build model of computron\-to\-wallclock relationship · Issue \#3459 · Agoric/agoric\-sdk](https://github.com/Agoric/agoric-sdk/issues/3459)
   - See `run_policy_simulation.py`

 - **slogdata** provides `SlogAccess` which uses [gztool](https://github.com/circulosmeos/gztool)
   for random access of `.slog.gz` files
 - **shred1** was used to load a slogfile into mysql. It's largely obsolete in favor of Big Query.
 - **shred4** was a notebook to explore shredded data in support of #3459
 - **slogfiles** is a notebook to explore the computron model for #3459
 - **validator-speed** is a notebook to explore relative speed of validators
 - `slogq.sql` was an experimental SQL approach to finding swingset-level differences between validators

### Local Infrastructure: nix-shell, jupyter, mysql

To install `jupyter` and other dependencies, we use a [nix development
environment](https://nixos.wiki/wiki/Development_environment_with_nix-shell)
specified in the conventional [shell.nix](shell.nix):

```sh
nix-shell
jupyter notebook
```

_TODO: document how to generate slogfiles.ipynb from slogfiles.py._

