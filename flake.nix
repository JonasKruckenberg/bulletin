{
  description = "Bulletin — scheduled digest pipeline";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
    }:
    let
      inherit (nixpkgs) lib;

      defaultSystems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];

      forAllSystems =
        fn:
        lib.genAttrs defaultSystems (
          system:
          let
            pkgs = import nixpkgs {
              inherit system;
              overlays = [ rust-overlay.overlays.default ];
            };
          in
          fn pkgs
        );

      # Workspace source minus heavy/irrelevant trees, so the build closure stays
      # small and source-only changes don't churn the vendored deps.
      src = lib.cleanSourceWith {
        src = ./.;
        filter =
          path: _type:
          let
            base = baseNameOf (toString path);
          in
          !(lib.elem base [
            "target"
            ".git"
            ".jj"
            "_sketch"
          ]);
      };
    in
    {
      packages = forAllSystems (
        pkgs:
        let
          # Build with the exact toolchain pinned in rust-toolchain.toml.
          rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
          rustPlatform = pkgs.makeRustPlatform {
            cargo = rustToolchain;
            rustc = rustToolchain;
          };

          # `cargoLock.lockFile` vendors the whole workspace from the committed
          # lockfile (no git deps → no cargoHash). No DB or `.sqlx` cache is needed
          # at build time: all queries are runtime `.bind` and migrations embed via
          # `sqlx::migrate!`. Tests need a live Postgres, so they run in the devShell
          # (nextest), not the sandbox.
          mkBin =
            { pname, crate }:
            rustPlatform.buildRustPackage {
              inherit pname src;
              version = "0.1.0";
              cargoLock.lockFile = ./Cargo.lock;
              cargoBuildFlags = [
                "-p"
                crate
              ];
              doCheck = false;
              meta.mainProgram = pname;
            };

          bulletin = mkBin {
            pname = "bulletin";
            crate = "bulletin";
          };
        in
        {
          inherit bulletin;
          default = bulletin;
        }
      );

      nixosModules.bulletin = import ./nix/module.nix self;
      nixosModules.default = self.nixosModules.bulletin;

      devShells = forAllSystems (
        pkgs:
        let
          rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

          devInputs = with pkgs; [
            rustToolchain
            cargo-nextest
            cargo-deny

            sqlx-cli

            postgresql_18
          ];
        in
        {
          default = pkgs.mkShell {
            name = "bulletin";
            buildInputs = devInputs;
          };
        }
      );
    };
}
