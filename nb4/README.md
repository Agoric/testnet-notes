# nb4 -- Agoric Incentivized Testnet Phase 4 Notebooks

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

## Getting Started: nix-shell, jupyter

To install `jupyter` and other dependencies, we use a [nix development
environment](https://nixos.wiki/wiki/Development_environment_with_nix-shell)
specified in the conventional [shell.nix](shell.nix):

```sh
nix-shell
jupyter notebook
```

_TODO: document how to generate slogfiles.ipynb from slogfiles.py._

