# This derivation just extracts out `libfdb_c_X.Y.Z.so` from GitHub
# and patches the library using `autoPatchelfHook`.
#
# In this derivation we don't care where `libfdb_c_X.Y.Z.so` will
# eventually get placed in the file system.
#
# If `isDir` is `false`, then we don't setup symlink from
# `libfdb_c.so` to `libfdb_c_X.Y.Z.so`. We would not want the symlink
# in case we are building `EXTERNAL_CLIENT_DIRECTORY`.
{ stdenv
, autoPatchelfHook
, fetchurl
, isDir
, lib
, sha256
, version
}:

stdenv.mkDerivation {
  pname = with lib; concatStrings [ "fdb-client-lib" (if isDir then "-dir" else "") ];

  inherit version;

  src = fetchurl {
    url = "https://github.com/apple/foundationdb/releases/download/${version}/libfdb_c.x86_64.so";
    inherit sha256;
  };

  nativeBuildInputs = [
    autoPatchelfHook
  ];

  unpackPhase = ":";

  installPhase = with lib; concatStrings [
    ''
      mkdir $out
      cp $src $out/libfdb_c.so.${version}
      chmod 555 $out/libfdb_c.so.${version}
    ''
    (
      if isDir then "" else ''
        ln -s $out/libfdb_c.so.${version} $out/libfdb_c.so    
      ''
    )
  ];
}
