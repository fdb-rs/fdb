{ stdenv
, autoPatchelfHook
, fetchurl
}:

stdenv.mkDerivation rec {
  pname = "cargo-llvm-cov";

  version = "0.2.3";

  src = fetchurl {
    url = "https://github.com/taiki-e/cargo-llvm-cov/releases/download/v${version}/cargo-llvm-cov-x86_64-unknown-linux-gnu.tar.gz";
    sha256 = "1zr6vsdhgd768ihfx1a3k12y4cli1cwy7bhqs5p437kw03prsnwg";
  };

  unpackPhase = ":";

  nativeBuildInputs = [
    autoPatchelfHook
  ];

  installPhase = ''
    tar zxvf $src

    mkdir -p $out/bin

    cp cargo-llvm-cov $out/bin/cargo-llvm-cov
  '';
}
