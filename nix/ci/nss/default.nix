{ pkgs }:
[
  # For FoundationDB it looks [1] like the recommended uid/gid is `4059`
  # and username/groupname is `fdb`.
  #
  # GitHub runs jobs in a ubuntu virtual machine using the user
  # `runner` whose `uid` is `1001`. The user `runner` primary group is
  # `docker`, whose `gid` is `121`. The path where the repository is
  # checked out is given by the environment variable
  # `GITHUB_WORKSPACE` [2] . We will assume the container is started
  # by bind-mounting `GITHUB_WORKSPACE` to `/home/runner/fdb`.
  #
  # `nixbld` group is required in order to prevent warning from `nix`
  # command. `nixbldX` users for `nix` to run in daemon mode.
  #
  # Also see `DEVELOP.md`.
  #
  # [1]: https://github.com/FoundationDB/fdb-kubernetes-operator/blob/v0.51.0/Dockerfile#L61-L66
  # [2]: https://docs.github.com/en/actions/learn-github-actions/environment-variables#default-environment-variables
  (
    pkgs.writeTextDir "etc/passwd" ''
      root:x:0:0:root user:/var/empty:/bin/sh
      runner:x:1001:121:runner user:/home/runner:/bin/sh
      fdb:x:4059:4059:fdb user:/var/empty:/bin/sh
      nixbld1:x:30001:30000:Nix build user 1:/var/empty:/bin/nologin
      nixbld2:x:30002:30000:Nix build user 2:/var/empty:/bin/nologin
      nixbld3:x:30003:30000:Nix build user 3:/var/empty:/bin/nologin
      nixbld4:x:30004:30000:Nix build user 4:/var/empty:/bin/nologin
      nixbld5:x:30005:30000:Nix build user 5:/var/empty:/bin/nologin
      nixbld6:x:30006:30000:Nix build user 6:/var/empty:/bin/nologin
      nixbld7:x:30007:30000:Nix build user 7:/var/empty:/bin/nologin
      nixbld8:x:30008:30000:Nix build user 8:/var/empty:/bin/nologin
      nobody:x:65534:65534:nobody:/var/empty:/bin/nologin
    ''
  )
  (
    pkgs.writeTextDir "etc/group" ''
      root:x:0:
      docker:x:121:
      fdb:x:4059:
      nixbld:x:30000:nixbld1,nixbld2,nixbld3,nixbld4,nixbld5,nixbld6,nixbld7,nixbld8
      nobody:x:65534:
    ''
  )
  (
    pkgs.writeTextDir "etc/nsswitch.conf" ''
      hosts: files dns
    ''
  )
  (
    pkgs.runCommand "var-empty" { } ''
      mkdir -p $out/var/empty
    ''
  )
]

