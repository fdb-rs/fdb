# This derivation can be used to extract out
#   - `fdbbackup`
#   - `fdbcli`
#   - `fdbmonitor`
#   - `fdbserver`
# from GitHub and patch the app using  `autoPatchelfHook`.
#
# In this derivation, we don't care where the app will eventually get
# placed in the file system.
{ stdenv
, autoPatchelfHook
, fetchurl
, name
, sha256
, version
}:

stdenv.mkDerivation {
  pname = "fdb-${name}";

  inherit version;

  src = fetchurl {
    url = "https://github.com/apple/foundationdb/releases/download/${version}/fdb${name}.x86_64";
    inherit sha256;
  };

  nativeBuildInputs = [
    autoPatchelfHook
  ];

  unpackPhase = ":";

  installPhase = ''
    mkdir $out
    cp $src $out/fdb${name}
    chmod 755 $out/fdb${name}
  '';
}
