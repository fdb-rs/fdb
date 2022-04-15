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
      version = "7.1.3";
      sha256 = "sha256-kXCqh5vinx7EwpWKEvQ77lmy1OVODcOd23GPD/RY2jk=";
      isDir = false;

      fdb-client-lib = pkgs.callPackage ./client-lib { inherit version sha256 isDir; };
    in
    opt-fdb-client-lib { inherit version fdb-client-lib; }
  )

  (
    let
      version = "7.1.3";
      sha256 = "sha256-kXCqh5vinx7EwpWKEvQ77lmy1OVODcOd23GPD/RY2jk=";
      isDir = true;

      fdb-client-lib-dir = pkgs.callPackage ./client-lib { inherit version sha256 isDir; };
    in
    opt-fdb-client-lib-dir { inherit version fdb-client-lib-dir; }
  )

  (
    let
      name = "monitor";
      sha256 = "sha256-67qgMHUa0giGs8KvahvKHtgk0ZQz7QpoDtiRPMM+JD8=";
      version = "7.1.3";

      fdb-monitor = pkgs.callPackage ./app { inherit name sha256 version; };
    in
    opt-fdb-monitor { inherit version fdb-monitor; }
  )

  (
    let
      name = "server";
      sha256 = "sha256-SmlBVss1hPyr1NHIyW1ld6W6JCvVMsc6AWvYUydGUwA=";
      version = "7.1.3";

      fdb-server = pkgs.callPackage ./app { inherit name sha256 version; };
    in
    opt-fdb-server { inherit version fdb-server; }
  )

  (
    let
      name = "cli";
      sha256 = "sha256-6ZZvcZlRTC6yWZW3gbncChYzxC+iRHGCgLovIGeBcvo=";
      version = "7.1.3";

      fdb-cli = pkgs.callPackage ./app { inherit name sha256 version; };
    in
    opt-fdb-cli { inherit version fdb-cli; }
  )
]
