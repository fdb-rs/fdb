{ stdenv
, autoPatchelfHook
, fetchurl
}:

stdenv.mkDerivation rec {
  pname = "cargo-llvm-cov";

  version = "0.4.9";

  src = fetchurl {
    url = "https://github.com/taiki-e/cargo-llvm-cov/releases/download/v${version}/cargo-llvm-cov-x86_64-unknown-linux-gnu.tar.gz";
    sha256 = "sha256-Gg24R33D1IrG0jX35FyD4sz5vJlpEyrY7Zzo9FKEW+A=";
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
