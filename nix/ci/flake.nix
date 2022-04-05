{
  description = "CI Nix flakes";

  inputs.nixpkgs.url = "nixpkgs/nixos-21.11";

  inputs.rust-overlay.url = "github:oxalica/rust-overlay";

  outputs = { self, nixpkgs, rust-overlay }: {
    fdb-6_3_23 =
      let
        pkgs = import nixpkgs {
          system = "x86_64-linux";
        };

        nix-conf = pkgs.writeTextDir "etc/nix/nix.conf" ''
          sandbox = false
          max-jobs = auto
          cores = 0
          trusted-users = root runner
          experimental-features = nix-command flakes
        '';

        systemd-units = builtins.attrValues (import ./systemd { inherit pkgs; });

        nss-files = import ./nss { inherit pkgs; };

        fdb = import ./fdb { inherit pkgs; };

        fdb-files = pkgs.callPackage ./fdb-files { version = "6.3.23"; };

        fdb-systemd-units = builtins.attrValues fdb-files.systemd_units;
      in
      with pkgs;
      dockerTools.buildImageWithNixDb {
        name = "fdb-6_3_23";
        tag = "latest";

        contents = [
          (symlinkJoin {
            name = "container-symlinks";
            paths = [
              bashInteractive
              cacert
              coreutils
              curl
              findutils
              git
              glibc.bin
              gnugrep
              gnutar
              gzip
              iproute2
              iputils
              nix-conf
              nixUnstable
              shadow
              systemd
              utillinux
              vim
              which
            ]
            ++ systemd-units
            ++ nss-files
            ++ fdb
            ++ fdb-systemd-units;
          })
        ]
        ++ fdb-files.conf;

        runAsRoot = ''
          mkdir -p -m 1777 /tmp

          mkdir -p /usr/bin
          ln -s ${coreutils}/bin/env /usr/bin/env

          touch /etc/machine-id
          mkdir -p /var
          ln -s /run /var/run

          mkdir -p /home/runner/fdb
          chown -R runner:docker /home/runner

          mkdir -p /opt/fdb/log
          mkdir -p /opt/fdb/data

          chown -R fdb:fdb /opt/fdb/conf
          chmod 644 /opt/fdb/conf/fdb.cluster

          chown fdb:fdb /opt/fdb/data
          chown fdb:fdb /opt/fdb/log

          systemctl enable fdbcli.service
          systemctl enable foundationdb.service
          systemctl enable nix-daemon.socket
        '';

        config = {
          Cmd = [ "/lib/systemd/systemd" ];

          Env = [
            "NIX_SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt"
          ];
        };
      };

    # We need to have multiple `mkShell` because of `rust-overlay`
    # limitation around `+nightly`. `+nightly` is needed by
    # `cargo-llvm-cov`.

    pull_request-6_3_23 =
      let
        overlays = [ (import rust-overlay) ];

        pkgs = import nixpkgs {
          inherit overlays;
          system = "x86_64-linux";
        };
      in
      with pkgs;
      mkShell {
        buildInputs = [
          clang
          libffi
          llvmPackages.libclang
          llvmPackages.libcxxClang
          openssl
          pkgconfig
          python37
          python37Packages.pip
          python37Packages.setuptools
          rust-bin.stable.latest.default
        ];

        LD_LIBRARY_PATH = "/opt/fdb/client-lib";
        FDB_CLUSTER_FILE = "/home/runner/fdb.cluster";

        # https://github.com/NixOS/nixpkgs/issues/52447#issuecomment-853429315
        BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${llvmPackages.libclang.lib}/lib/clang/${lib.getVersion clang}/include";
        LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
        RUSTC_LINK_SEARCH_FDB_CLIENT_LIB = "/opt/fdb/client-lib";

        # https://nixos.wiki/wiki/Python#Emulating_virtualenv_with_nix-shell
        shellHook = ''
          # Tells pip to put packages into $PIP_PREFIX instead of the usual locations.
          # See https://pip.pypa.io/en/stable/user_guide/#environment-variables.
          export PIP_PREFIX=/home/runner/_build/pip_packages
          export PYTHONPATH="$PIP_PREFIX/${pkgs.python37.sitePackages}:$PYTHONPATH"
          export PATH="$PIP_PREFIX/bin:$PATH"
          unset SOURCE_DATE_EPOCH
        '';
      };

    pull_request-nightly-6_3_23 =
      let
        overlays = [ (import rust-overlay) ];

        pkgs = import nixpkgs {
          inherit overlays;
          system = "x86_64-linux";
        };

        cargo-llvm-cov = pkgs.callPackage ./cargo-llvm-cov { };
      in
      with pkgs;
      mkShell {
        buildInputs = [
          cargo-llvm-cov
          clang
          lcov
          llvmPackages.libclang
          llvmPackages.libcxxClang
          openssl
          pkgconfig
          (rust-bin.nightly."2022-04-03".default.override {
            extensions = [
              "llvm-tools-preview"
            ];
          })
        ];

        LD_LIBRARY_PATH = "/opt/fdb/client-lib";
        FDB_CLUSTER_FILE = "/home/runner/fdb.cluster";

        # https://github.com/NixOS/nixpkgs/issues/52447#issuecomment-853429315
        BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${llvmPackages.libclang.lib}/lib/clang/${lib.getVersion clang}/include";
        LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
        RUSTC_LINK_SEARCH_FDB_CLIENT_LIB = "/opt/fdb/client-lib";
      };

    push-6_3_23 =
      let
        overlays = [ (import rust-overlay) ];

        pkgs = import nixpkgs {
          inherit overlays;
          system = "x86_64-linux";
        };
      in
      with pkgs;
      mkShell {
        buildInputs = [
          clang
          libffi
          llvmPackages.libclang
          llvmPackages.libcxxClang
          openssl
          pkgconfig
          python37
          python37Packages.pip
          python37Packages.setuptools
          rust-bin.stable.latest.default
        ];

        LD_LIBRARY_PATH = "/opt/fdb/client-lib";
        FDB_CLUSTER_FILE = "/home/runner/fdb.cluster";

        # https://github.com/NixOS/nixpkgs/issues/52447#issuecomment-853429315
        BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${llvmPackages.libclang.lib}/lib/clang/${lib.getVersion clang}/include";
        LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
        RUSTC_LINK_SEARCH_FDB_CLIENT_LIB = "/opt/fdb/client-lib";

        # https://nixos.wiki/wiki/Python#Emulating_virtualenv_with_nix-shell
        shellHook = ''
          # Tells pip to put packages into $PIP_PREFIX instead of the usual locations.
          # See https://pip.pypa.io/en/stable/user_guide/#environment-variables.
          export PIP_PREFIX=/home/runner/_build/pip_packages
          export PYTHONPATH="$PIP_PREFIX/${pkgs.python37.sitePackages}:$PYTHONPATH"
          export PATH="$PIP_PREFIX/bin:$PATH"
          unset SOURCE_DATE_EPOCH
        '';
      };

    push-nightly-6_3_23 =
      let
        overlays = [ (import rust-overlay) ];

        pkgs = import nixpkgs {
          inherit overlays;
          system = "x86_64-linux";
        };

        cargo-llvm-cov = pkgs.callPackage ./cargo-llvm-cov { };
      in
      with pkgs;
      mkShell {
        buildInputs = [
          cargo-llvm-cov
          clang
          lcov
          llvmPackages.libclang
          llvmPackages.libcxxClang
          openssl
          pkgconfig
          (rust-bin.nightly."2022-04-03".default.override {
            extensions = [
              "llvm-tools-preview"
            ];
          })
        ];

        LD_LIBRARY_PATH = "/opt/fdb/client-lib";
        FDB_CLUSTER_FILE = "/home/runner/fdb.cluster";

        # https://github.com/NixOS/nixpkgs/issues/52447#issuecomment-853429315
        BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${llvmPackages.libclang.lib}/lib/clang/${lib.getVersion clang}/include";
        LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
        RUSTC_LINK_SEARCH_FDB_CLIENT_LIB = "/opt/fdb/client-lib";
      };

    push_rustdoc-6_3_23 =
      let
        overlays = [ (import rust-overlay) ];

        pkgs = import nixpkgs {
          inherit overlays;
          system = "x86_64-linux";
        };
      in
      with pkgs;
      mkShell {
        buildInputs = [
          clang
          llvmPackages.libclang
          llvmPackages.libcxxClang
          openssl
          pkgconfig
          rust-bin.stable.latest.default
        ];

        LD_LIBRARY_PATH = "/opt/fdb/client-lib";
        FDB_CLUSTER_FILE = "/home/runner/fdb.cluster";

        # https://github.com/NixOS/nixpkgs/issues/52447#issuecomment-853429315
        BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${llvmPackages.libclang.lib}/lib/clang/${lib.getVersion clang}/include";
        LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
        RUSTC_LINK_SEARCH_FDB_CLIENT_LIB = "/opt/fdb/client-lib";
      };

    schedule-6_3_23 =
      let
        overlays = [ (import rust-overlay) ];

        pkgs = import nixpkgs {
          inherit overlays;
          system = "x86_64-linux";
        };
      in
      with pkgs;
      mkShell {
        buildInputs = [
          clang
          libffi
          llvmPackages.libclang
          llvmPackages.libcxxClang
          openssl
          pkgconfig
          python37
          python37Packages.pip
          python37Packages.setuptools
          rust-bin.stable.latest.default
        ];

        LD_LIBRARY_PATH = "/opt/fdb/client-lib";
        FDB_CLUSTER_FILE = "/home/runner/fdb.cluster";

        # https://github.com/NixOS/nixpkgs/issues/52447#issuecomment-853429315
        BINDGEN_EXTRA_CLANG_ARGS = "-isystem ${llvmPackages.libclang.lib}/lib/clang/${lib.getVersion clang}/include";
        LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
        RUSTC_LINK_SEARCH_FDB_CLIENT_LIB = "/opt/fdb/client-lib";

        # https://nixos.wiki/wiki/Python#Emulating_virtualenv_with_nix-shell
        shellHook = ''
          # Tells pip to put packages into $PIP_PREFIX instead of the usual locations.
          # See https://pip.pypa.io/en/stable/user_guide/#environment-variables.
          export PIP_PREFIX=/home/runner/_build/pip_packages
          export PYTHONPATH="$PIP_PREFIX/${pkgs.python37.sitePackages}:$PYTHONPATH"
          export PATH="$PIP_PREFIX/bin:$PATH"
          unset SOURCE_DATE_EPOCH
        '';
      };
  };
}
