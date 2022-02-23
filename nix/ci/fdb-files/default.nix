{ pkgs, version }:
{
  conf = [
    (
      pkgs.writeTextDir "opt/fdb/conf/foundationdb.conf"
        (
          builtins.readFile
            (
              pkgs.substituteAll
                {
                  src = ./foundationdb.conf;
                  inherit version;
                }
            )
        )
    )
    (
      pkgs.writeTextDir "opt/fdb/conf/fdb.cluster"
        (
          builtins.readFile ./fdb.cluster
        )
    )
  ];

  systemd_units = {
    foundationdb_service = pkgs.writeTextDir "etc/systemd/system/foundationdb.service"
      (
        builtins.readFile ./foundationdb.service
      );

    fdbcli_service = pkgs.writeTextDir "etc/systemd/system/fdbcli.service"
      (
        builtins.readFile
          (
            pkgs.substituteAll
              {
                src = ./fdbcli.service;
                inherit version;
              }
          )
      );
  };
}
