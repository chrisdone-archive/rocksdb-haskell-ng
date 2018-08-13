{ mkDerivation, base, bytestring, criterion, deepseq, directory
, filepath, hspec, QuickCheck, rocksdb, stdenv, temporary
}:
mkDerivation {
  pname = "rocksdb-haskell-ng";
  version = "0.0.0";
  src = ./.;
  libraryHaskellDepends = [ base bytestring directory ];
  librarySystemDepends = [ rocksdb ];
  testHaskellDepends = [
    base deepseq directory filepath hspec QuickCheck temporary
  ];
  benchmarkHaskellDepends = [
    base criterion deepseq directory filepath hspec QuickCheck
    temporary
  ];
  license = stdenv.lib.licenses.bsd3;
}
