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
      version = "6.3.23";
      sha256 = "172f74gn9gn0igiq9qsfbif2gha1f5jiicw14d638j9j1lfqbwsg";
      isDir = false;

      fdb-client-lib = pkgs.callPackage ./client-lib { inherit version sha256 isDir; };
    in
    opt-fdb-client-lib { inherit version fdb-client-lib; }
  )

  (
    let
      version = "6.3.23";
      sha256 = "172f74gn9gn0igiq9qsfbif2gha1f5jiicw14d638j9j1lfqbwsg";
      isDir = true;

      fdb-client-lib-dir = pkgs.callPackage ./client-lib { inherit version sha256 isDir; };
    in
    opt-fdb-client-lib-dir { inherit version fdb-client-lib-dir; }
  )

  (
    let
      name = "monitor";
      sha256 = "1p4fz322qg8wjlj8www6yd7wgx7yr5j0cygmsp29par0wzv94qw6";
      version = "6.3.23";

      fdb-monitor = pkgs.callPackage ./app { inherit name sha256 version; };
    in
    opt-fdb-monitor { inherit version fdb-monitor; }
  )

  (
    let
      name = "server";
      sha256 = "17sr5lwihy2p73kwb3v7biwziyrqhsbmya8506l2jbfmz7pk3jwm";
      version = "6.3.23";

      fdb-server = pkgs.callPackage ./app { inherit name sha256 version; };
    in
    opt-fdb-server { inherit version fdb-server; }
  )

  (
    let
      name = "cli";
      sha256 = "0c8ss64570jsiih2mbakq2knxxlwwwh89vnnpg8bqjpf6fy47avb";
      version = "6.3.23";

      fdb-cli = pkgs.callPackage ./app { inherit name sha256 version; };
    in
    opt-fdb-cli { inherit version fdb-cli; }
  )
]
