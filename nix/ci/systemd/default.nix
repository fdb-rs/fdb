{ pkgs }:
rec {
  default_target = pkgs.writeTextDir "etc/systemd/system/default.target"
    (
      builtins.readFile
        (
          pkgs.substituteAll
            {
              src = ./unit-files/default.target;
              sysinit_target_name = sysinit_target.name;
              systemd_journald_service_name = systemd_journald_service.name;
            }
        )
    );

  halt_target = pkgs.writeTextDir "etc/systemd/system/halt.target"
    (
      builtins.readFile
        (
          pkgs.substituteAll
            {
              src = ./unit-files/halt.target;
              halt_service_name = halt_service.name;
            }
        )
    );

  halt_service = pkgs.writeTextDir "etc/systemd/system/halt.service"
    (
      builtins.readFile
        (
          pkgs.substituteAll
            {
              src = ./unit-files/halt.service;
              systemd = pkgs.systemd;
            }
        )
    );

  sysinit_target = pkgs.writeTextDir "etc/systemd/system/sysinit.target"
    (
      builtins.readFile ./unit-files/sysinit.target
    );

  systemd_journald_socket = pkgs.writeTextDir "etc/systemd/system/systemd-journald.socket"
    (
      builtins.readFile ./unit-files/systemd-journald.socket
    );

  systemd_journald_service = pkgs.writeTextDir "etc/systemd/system/systemd-journald.service"
    (
      builtins.readFile
        (
          pkgs.substituteAll
            {
              src = ./unit-files/systemd-journald.service;
              systemd = pkgs.systemd;
              systemd_journald_socket_name = systemd_journald_socket.name;
            }
        )
    );

  nix_daemon_socket = pkgs.writeTextDir "etc/systemd/system/nix-daemon.socket"
    (
      builtins.readFile ./unit-files/nix-daemon.socket
    );

  nix_daemon_service = pkgs.writeTextDir "etc/systemd/system/nix-daemon.service"
    (
      builtins.readFile
        (
          pkgs.substituteAll
            {
              src = ./unit-files/nix-daemon.service;
              nix = pkgs.nixUnstable;
              nix_daemon_socket_name = nix_daemon_socket.name;
              cacert = pkgs.cacert;
            }
        )
    );
}
