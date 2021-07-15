# https://nixos.wiki/wiki/Development_environment_with_nix-shell
{ pkgs ? import <nixpkgs> {} }:
  pkgs.mkShell {
    # nativeBuildInputs is usually what you want -- tools you need to run
    nativeBuildInputs = [
      # pkgs.python38Packages.pyspark
      pkgs.jupyter
      pkgs.python38Packages.pandas pkgs.python38Packages.matplotlib
      pkgs.python38Packages.dask
      pkgs.python38Packages.distributed
      # python38Packages.pymongo
      pkgs.rclone
      pkgs.zlib
    ];
}

