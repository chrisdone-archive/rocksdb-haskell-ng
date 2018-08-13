{ pkgs ? import <nixpkgs> {} }:
  pkgs.haskellPackages.callPackage ./rocksdb-haskell-ng.nix {}
