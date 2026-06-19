{
  description = "Bulletin — scheduled digest pipeline";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";

    # crane splits the build into a deps-only layer (keyed on Cargo.lock) and a thin
    # workspace layer, so source-only changes reuse the cached dependency artifacts
    # instead of recompiling the whole dependency graph on every commit.
    crane.url = "github:ipetkov/crane";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      crane,
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
          craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

          # No DB or `.sqlx` cache is needed at build time: all queries are runtime
          # `.bind` and migrations embed via `sqlx::migrate!` (the `src` filter above
          # keeps the `.sql` files). Tests need a live Postgres, so they run in the
          # devShell (nextest), not the sandbox.
          commonArgs = {
            inherit src;
            strictDeps = true;
            doCheck = false;
          };

          # Deps-only layer: keyed on Cargo.lock/Cargo.toml (crane dummies out the
          # workspace sources), so source-only commits reuse this artifact instead of
          # recompiling the whole dependency graph. Built once, cached in the Nix store.
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          mkBin =
            {
              pname,
              crate,
              features ? [ ],
            }:
            craneLib.buildPackage (
              commonArgs
              // {
                inherit pname cargoArtifacts;
                version = "0.1.0";
                cargoExtraArgs =
                  "-p ${crate}"
                  + lib.optionalString (features != [ ]) " --features ${lib.concatStringsSep "," features}";
                meta.mainProgram = pname;
              }
            );

          # Summarization is a mandatory part of the pipeline now (no `llm-summarization` cargo feature,
          # no compile-time kill switch) — there is a single binary. The worker requires a reachable
          # summarization sidecar at boot (configure it via `BULLETIN_LLM_*` / the NixOS module's `llm.*`).
          bulletin = mkBin {
            pname = "bulletin";
            crate = "bulletin";
          };

          # `bulletin-llm` is retained as an alias of `bulletin` for backward compatibility (the NixOS
          # module and any pinned references still name it); summarization is no longer a build choice, so
          # the two are identical.
          bulletin-llm = bulletin;
        in
        {
          inherit bulletin bulletin-llm;
          default = bulletin;

          # Expose the crane deps-only layer as a build target so CI can `nix build .#deps
          # --out-link ...` it. That out-link registers a GC root, which is what keeps
          # cargoArtifacts in the Nix store through cache-nix-action's save-time gc. Without a
          # root this layer — a build-only input of `.#bulletin`, absent from the final binary's
          # runtime closure — was collected before the cache was saved, so every run cold-rebuilt
          # the whole dependency graph (~5.5 min) despite the cache "hitting".
          deps = cargoArtifacts;
        }
      );

      nixosModules.bulletin = import ./nix/module.nix self;
      nixosModules.default = self.nixosModules.bulletin;

      # NixOS VM tests. Guarded to Linux — the test driver boots QEMU VMs, which
      # the Darwin builders can't do.
      checks = forAllSystems (
        pkgs:
        lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
          # Regression guard for the llama.cpp sidecar's no-egress lockdown.
          llama-cpp-egress = import ./nix/test/llama-egress.nix { inherit self pkgs lib; };
        }
      );

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
