# https://nixos.wiki/wiki/Development_environment_with_nix-shell
# https://nixos.wiki/wiki/Packaging/Python
{ pkgs ? import <nixpkgs> {} }:
  pkgs.mkShell {
    # nativeBuildInputs is usually what you want -- tools you need to run
    nativeBuildInputs = [
      # pkgs.python38Packages.pyspark
      pkgs.readline
      pkgs.jupyter
      pkgs.python38Packages.pip
      pkgs.python38Packages.virtualenv
      pkgs.python38Packages.pymysql
      pkgs.python38Packages.statsmodels
      pkgs.python38Packages.jupytext
      pkgs.python38Packages.pandas pkgs.python38Packages.matplotlib
      pkgs.python38Packages.dask
      pkgs.python38Packages.distributed
      # python38Packages.pymongo
      pkgs.rclone
      pkgs.zlib
    ];
}

