{ stdenv
, autoPatchelfHook
, fetchurl
}:

stdenv.mkDerivation rec {
  pname = "cargo-llvm-cov";

  version = "0.3.1";

  src = fetchurl {
    url = "https://github.com/taiki-e/cargo-llvm-cov/releases/download/v${version}/cargo-llvm-cov-x86_64-unknown-linux-gnu.tar.gz";
    sha256 = "sha256-IeAHM2le6cjGbU13Jk4OkuCDDZ7wPxmjIZh5ko7vlis=";
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
