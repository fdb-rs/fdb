{ pkgs }:
let
  opt-fdb-client-lib = { version, fdb-client-lib }:
    pkgs.runCommand "opt-lib-client-lib-${version}" { } ''
      mkdir -p $out/opt/fdb/client-lib
      ln -s ${fdb-client-lib}/libfdb_c.so $out/opt/fdb/client-lib/libfdb_c.so
      ln -s ${fdb-client-lib}/libfdb_c.so.${version} $out/opt/fdb/client-lib/libfdb_c.so.${version}
    '';

  opt-fdb-client-lib-dir = { version, fdb-client-lib-dir }:
    pkgs.runCommand "opt-lib-client-lib-dir-${version}" { } ''
      mkdir -p $out/opt/fdb/client-lib-dir
      ln -s ${fdb-client-lib-dir}/libfdb_c.so.${version} $out/opt/fdb/client-lib-dir/libfdb_c.so.${version}
    '';

  opt-fdb-monitor = { version, fdb-monitor }:
    pkgs.runCommand "opt-fdb-monitor-${version}" { } ''
      mkdir -p $out/opt/fdb/monitor
      ln -s ${fdb-monitor}/fdbmonitor $out/opt/fdb/monitor/fdbmonitor
    '';

  opt-fdb-server = { version, fdb-server }:
    pkgs.runCommand "opt-fdb-server-${version}" { } ''
      mkdir -p $out/opt/fdb/server/${version}
      ln -s ${fdb-server}/fdbserver $out/opt/fdb/server/${version}/fdbserver
    '';

  opt-fdb-cli = { version, fdb-cli }:
    pkgs.runCommand "opt-fdb-cli-${version}" { } ''
      mkdir -p $out/opt/fdb/cli/${version}
      ln -s ${fdb-cli}/fdbcli $out/opt/fdb/cli/${version}/fdbcli
    '';
in
[
  (
    let
      version = "7.1.12";
      sha256 = "sha256-5KeYLcy22eYWuQVUMIlrAP90h0crAzvkrcl/ADZ5yCE=";
      isDir = false;

      fdb-client-lib = pkgs.callPackage ./client-lib { inherit version sha256 isDir; };
    in
    opt-fdb-client-lib { inherit version fdb-client-lib; }
  )

  (
    let
      version = "7.1.12";
      sha256 = "sha256-5KeYLcy22eYWuQVUMIlrAP90h0crAzvkrcl/ADZ5yCE=";
      isDir = true;

      fdb-client-lib-dir = pkgs.callPackage ./client-lib { inherit version sha256 isDir; };
    in
    opt-fdb-client-lib-dir { inherit version fdb-client-lib-dir; }
  )

  (
    let
      name = "monitor";
      version = "7.1.12";
      sha256 = "sha256-meuNIjt6xhkuTM1AiF8fvtFM2SnM16MutNPsHH58gz8=";

      fdb-monitor = pkgs.callPackage ./app { inherit name sha256 version; };
    in
    opt-fdb-monitor { inherit version fdb-monitor; }
  )

  (
    let
      name = "server";
      version = "7.1.12";
      sha256 = "sha256-FQzCcIAeFLfGszkJ61BJqYRlq2ev/fMxA93Lz6qkRJg=";

      fdb-server = pkgs.callPackage ./app { inherit name sha256 version; };
    in
    opt-fdb-server { inherit version fdb-server; }
  )

  (
    let
      name = "cli";
      version = "7.1.12";
      sha256 = "sha256-JGHdRcAXii+hiOamrmy6GA5LG4a4aCfbF4o1LoHC4p0=";

      fdb-cli = pkgs.callPackage ./app { inherit name sha256 version; };
    in
    opt-fdb-cli { inherit version fdb-cli; }
  )
]
