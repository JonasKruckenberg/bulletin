# NixOS module for the Bulletin digest pipeline. Exported from the flake as
# `nixosModules.bulletin` (the leading `self:` closes over the flake so `package`
# can default to the flake's own `bulletin` build).
#
# Single `bulletin all` process (HTTP health server + background worker) plus a oneshot migration
# unit that runs before it. Provisions a local PostgreSQL DB with unix-socket peer auth by default
# (no passwords anywhere). Logs JSON to stdout → journald (for Alloy → Loki) and exposes a
# Prometheus exporter. The optional SMTP credentials are fed via systemd `EnvironmentFile`, so the
# secret (e.g. an agenix file with `BULLETIN_SMTP_PASSWORD=…`) never lands in the Nix store.
self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.bulletin;

  # A static system user (not DynamicUser) so ad-hoc CLI ops — seeding feeds/subscribers and the
  # `digest-explain` iteration loop — run as `sudo -u bulletin bulletin …` with the same
  # unix-socket peer-auth identity the service uses (a DynamicUser has no persistent passwd entry
  # to sudo into). All the hardening below still applies to a static user.
  user = "bulletin";
  dbName = "bulletin";

  # Two DB roles for two-context RLS (design §12): the owner role (`bulletin`) owns the DDL and runs
  # migrations; the least-privilege runtime role (`bulletin_app`: non-owner, no BYPASSRLS) is what
  # serve/worker/debug log in as, so the FORCE ROW LEVEL SECURITY policies actually bind. Both are
  # reached by the single OS user `bulletin` over unix-socket peer auth via an ident map (below).
  ownerRole = "bulletin";
  runtimeRole = "bulletin_app";

  # Runtime connection string (serve/worker/debug → runtime role). The owner string (migrate → owner
  # role) is separate, so a runtime credential can never alter the schema or disable RLS.
  databaseUrl =
    if cfg.database.createLocally then
      "postgres:///${dbName}?host=/run/postgresql&user=${runtimeRole}"
    else
      cfg.database.url;

  migrationUrl =
    if cfg.database.createLocally then
      "postgres:///${dbName}?host=/run/postgresql&user=${ownerRole}"
    else if cfg.database.migrationUrl != null then
      cfg.database.migrationUrl
    else
      cfg.database.url;

  # CLI wrapper on PATH: presets the env the operator CLI needs so `sudo -u bulletin bulletin debug …`
  # (seeding, status, the digest-explain loop) runs without flags. `debug` is now a gRPC client of the
  # admin API, so it needs the API address + admin bearer; it sources `api.adminKeyFile` (which must be
  # bulletin-readable) to present the key. DATABASE_URL + email are still preset for the other CLI
  # commands an operator might run by hand (e.g. `migrate`).
  cliWrapper = pkgs.writeShellScriptBin "bulletin" ''
    export DATABASE_URL="''${DATABASE_URL:-${toString databaseUrl}}"
    export BULLETIN_EMAIL_TRANSPORT="''${BULLETIN_EMAIL_TRANSPORT:-${cfg.email.transport}}"
    export BULLETIN_EMAIL_FROM="''${BULLETIN_EMAIL_FROM:-${cfg.email.from}}"
    export BULLETIN_EMAIL_FILE_DIR="''${BULLETIN_EMAIL_FILE_DIR:-/var/lib/bulletin/outbox}"
    export BULLETIN_API_ADDR="''${BULLETIN_API_ADDR:-${cfg.api.addr}}"
    ${lib.optionalString (cfg.api.adminKeyFile != null) ''
      # Source the admin bearer so `debug` authenticates to the API (file must be bulletin-readable).
      # Quote the path so a directory with spaces doesn't word-split into a silent auth failure.
      if [ -r "${toString cfg.api.adminKeyFile}" ]; then
        set -a; . "${toString cfg.api.adminKeyFile}"; set +a
      fi
    ''}
    exec ${lib.getExe cfg.package} "$@"
  '';

  pgPackage =
    if cfg.database.createLocally then config.services.postgresql.package else pkgs.postgresql_18;

  pgDeps = lib.optional cfg.database.createLocally "postgresql.target";

  httpPort = lib.toInt (lib.last (lib.splitString ":" cfg.http.addr));

  # The local summarization sidecar. Parse the `host:port` authority out of `llm.baseUrl`
  # (e.g. http://127.0.0.1:8080/v1 → 8080) so `services.llama-cpp` and the worker agree on the port.
  llmAuthority = lib.head (
    lib.splitString "/" (lib.removePrefix "https://" (lib.removePrefix "http://" cfg.llm.baseUrl))
  );
  llmPort = lib.toInt (lib.last (lib.splitString ":" llmAuthority));
  llmPackage = if cfg.llm.package != null then cfg.llm.package else pkgs.llama-cpp;

  # Where the GGUF the sidecar serves lives. An explicit `modelPath` wins (the manual / `fetchurl`
  # paths); otherwise, when `modelUrl` is set, it is derived into a dedicated, world-readable dir
  # (`/var/lib/bulletin-models`, *not* under the 0700 state dir — the llama-cpp sidecar runs as its
  # own user and must be able to read the file). `null` when neither is configured.
  llmModelBasename = lib.last (lib.splitString "/" cfg.llm.modelUrl);
  llmModelDir = "/var/lib/bulletin-models";
  llmModelPath =
    if cfg.llm.modelPath != null then
      toString cfg.llm.modelPath
    else if cfg.llm.modelUrl != null then
      "${llmModelDir}/${llmModelBasename}"
    else
      null;
  # Whether the module provisions the fetch-on-activation oneshot (declarative URL + hash → /var/lib).
  # Independent of `modelPath`: with both set, it fetches into the explicit path.
  llmFetchModel = cfg.llm.serveLocally && cfg.llm.modelUrl != null;

  # Root-readable secret env files loaded via systemd `EnvironmentFile` (SMTP creds + the sealed
  # GitHub App credentials), so neither enters the Nix store.
  envSecretFiles =
    lib.optional (cfg.email.smtpSecretFile != null) cfg.email.smtpSecretFile
    ++ lib.optional (cfg.github.secretFile != null) cfg.github.secretFile
    ++ lib.optional (cfg.api.adminKeyFile != null) cfg.api.adminKeyFile;

  # systemd hardening shared by both units. Verified not to break the PG unix socket (AF_UNIX),
  # outbound HTTPS for RSS polling, or future SMTP (AF_INET*). Plain Rust has no JIT, so
  # MemoryDenyWriteExecute is safe.
  hardening = {
    User = user;
    Group = user;
    NoNewPrivileges = true;
    ProtectSystem = "strict";
    ProtectHome = true;
    PrivateTmp = true;
    PrivateDevices = true;
    ProtectKernelTunables = true;
    ProtectKernelModules = true;
    ProtectKernelLogs = true;
    ProtectControlGroups = true;
    ProtectClock = true;
    ProtectHostname = true;
    ProtectProc = "invisible";
    ProcSubset = "pid";
    RestrictNamespaces = true;
    RestrictRealtime = true;
    RestrictSUIDSGID = true;
    LockPersonality = true;
    MemoryDenyWriteExecute = true;
    RemoveIPC = true;
    CapabilityBoundingSet = [ "" ];
    AmbientCapabilities = [ "" ];
    RestrictAddressFamilies = [
      "AF_INET"
      "AF_INET6"
      "AF_UNIX"
    ];
    SystemCallArchitectures = "native";
    SystemCallFilter = [
      "@system-service"
      "~@resources"
      "~@privileged"
    ];
    UMask = "0077";
  };
in
{
  options.services.bulletin = {
    enable = lib.mkEnableOption "the Bulletin digest pipeline";

    package = lib.mkOption {
      type = lib.types.package;
      # The compile-time kill switch in action: `llm.enable` selects the summarization-enabled build
      # (`bulletin-llm`) over the plain `bulletin`. There is no runtime flag — off genuinely ships a
      # binary with no summarization code. Override this to pin a specific build.
      default =
        self.packages.${pkgs.stdenv.hostPlatform.system}.${if cfg.llm.enable then "bulletin-llm" else "bulletin"};
      defaultText = lib.literalExpression "bulletin.packages.\${system}.\${if cfg.llm.enable then \"bulletin-llm\" else \"bulletin\"}";
      description = "The `bulletin` package to run. Defaults to the LLM-enabled build when `llm.enable`.";
    };

    database = {
      createLocally = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Provision a local PostgreSQL database + role using unix-socket peer auth.";
      };
      url = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        example = "postgres://bulletin_app@db.internal/bulletin";
        description = ''
          Runtime DATABASE_URL (the least-privilege `bulletin_app` role) to use when
          `database.createLocally = false`.
        '';
      };
      migrationUrl = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        example = "postgres://bulletin@db.internal/bulletin";
        description = ''
          Owner/migration connection string (a more-privileged role that owns the DDL) for `migrate`,
          when `database.createLocally = false`. Falls back to `database.url` if unset — but then the
          runtime role must itself be able to create the runtime role + RLS policies, so a dedicated
          owner URL is strongly recommended.
        '';
      };
    };

    http.addr = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1:3000";
      description = "Bind address for the `/health` HTTP server.";
    };

    metrics.addr = lib.mkOption {
      type = lib.types.str;
      default = "127.0.0.1:9464";
      description = "Bind address for the Prometheus `/metrics` exporter.";
    };

    api = {
      addr = lib.mkOption {
        type = lib.types.str;
        default = "127.0.0.1:50051";
        description = ''
          Bind address for the gRPC admin API (started as part of `bulletin all`), and the address the
          `bulletin debug …` CLI dials. Loopback by default — the `debug` CLI is now a thin client of
          this API, so it runs against the live engine without the DB credential or the SMTP secret.
        '';
      };
      adminKeyFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        example = "/run/agenix/bulletin-api-admin-key";
        description = ''
          Path to an env file holding `BULLETIN_API_ADMIN_KEY=<high-entropy key>` (one `KEY=value`
          line). It is loaded into the `bulletin` service via systemd `EnvironmentFile` AND sourced by
          the `bulletin` CLI wrapper, so the engine accepts admin RPCs and the `debug` CLI presents the
          matching bearer. **Make it readable by the `bulletin` user** (e.g. agenix `owner = "bulletin"`)
          so the CLI wrapper — run via `sudo -u bulletin` — can source it.

          Absent ⇒ the admin plane is fail-closed: every admin RPC is rejected and `bulletin debug …`
          cannot talk to the engine. Set it to use the CLI or the API.
        '';
      };
    };

    log = {
      format = lib.mkOption {
        type = lib.types.enum [
          "text"
          "json"
        ];
        default = "json";
        description = "Log output format. `json` is recommended for journald → Alloy → Loki.";
      };
      level = lib.mkOption {
        type = lib.types.str;
        default = "info";
        example = "info,bulletin=debug";
        description = "RUST_LOG filter.";
      };
    };

    email = {
      transport = lib.mkOption {
        type = lib.types.enum [
          "file"
          "smtp"
        ];
        default = "file";
        description = "`file` writes .eml into the state dir; `smtp` sends via a relay.";
      };
      from = lib.mkOption {
        type = lib.types.str;
        default = "bulletin@localhost";
        description = "From address for digest emails.";
      };
      smtpSecretFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        example = "/run/agenix/bulletin-smtp";
        description = ''
          Path to a root-readable env file (e.g. an agenix secret) holding the SMTP credentials,
          one `KEY=value` per line. Loaded via systemd `EnvironmentFile` (read as root before the
          service drops to the bulletin user), so it never enters the Nix store. Required when
          `email.transport = "smtp"`.

          Recognized keys: `BULLETIN_SMTP_HOST`, `BULLETIN_SMTP_USERNAME`,
          `BULLETIN_SMTP_PASSWORD` (required), and optionally `BULLETIN_SMTP_PORT` /
          `BULLETIN_SMTP_TLS` (`starttls` | `implicit`). For a Proton custom domain on Mail Plus:

          ```
          BULLETIN_SMTP_HOST=smtp.protonmail.ch
          BULLETIN_SMTP_USERNAME=bulletin@your.domain
          BULLETIN_SMTP_PASSWORD=<SMTP token from Settings → IMAP/SMTP → SMTP tokens>
          ```

          (host/username may also be set non-secret via `email.from` and the service
          `environment`; only `BULLETIN_SMTP_PASSWORD` truly needs to be a secret.)
        '';
      };
    };

    github = {
      secretFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        example = "/run/agenix/bulletin-github";
        description = ''
          Path to a root-readable env file (e.g. an agenix secret) holding the GitHub App
          credentials, one `KEY=value` per line. Loaded via systemd `EnvironmentFile` (read as root
          before the service drops to the bulletin user), so it never enters the Nix store.

          The two real secrets are sealed at rest (envelope-encrypted under the master key) — produce
          them offline with `bulletin secrets keygen` + `bulletin secrets seal`. Recognized keys:
          `BULLETIN_MASTER_KEY` (base64 32-byte master key), `BULLETIN_GITHUB_APP_ID` (numeric, not a
          secret), `BULLETIN_GITHUB_APP_PRIVATE_KEY` (sealed envelope),
          `BULLETIN_GITHUB_WEBHOOK_SECRET_SEALED` (sealed envelope). Omit this and GitHub ingestion
          stays disabled (RSS is unaffected).
        '';
      };
    };

    llm = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = ''
          Turn on LLM cluster summarization (docs/llm-summarization.md, Phase A). This is a
          **compile-time** switch: it selects the `bulletin-llm` build (the `llm-summarization` cargo
          feature) over the plain `bulletin`, whose worker runs a best-effort, off-the-punctual-path
          summarization sweep after each public build against the sidecar at `llm.baseUrl`. Off ⇒ a
          binary with **no** summarization code at all (the deterministic digest baseline) — there is
          no runtime flag. Pair with `llm.serveLocally` to also run the sidecar on this host. No data
          egress: the sidecar is 100% local (design §12).
        '';
      };
      baseUrl = lib.mkOption {
        type = lib.types.str;
        default = "http://127.0.0.1:8080/v1";
        description = "OpenAI-compatible base URL of the local summarization sidecar (no trailing /).";
      };
      model = lib.mkOption {
        type = lib.types.str;
        default = "qwen3.5-4b-instruct";
        example = "granite-4.0-h-micro";
        description = ''
          Served model name sent in each request (`local-ml-options.md` §7 — a small Apache instruct
          model). Also stamped (with the prompt version) onto `cluster.summary_model`, so changing it
          re-summarizes the corpus on the next sweep.
        '';
      };
      promptVersion = lib.mkOption {
        type = lib.types.int;
        default = 2;
        description = "Prompt/schema version; bump to invalidate + re-summarize the whole corpus.";
      };
      serveLocally = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = ''
          Provision a local `llama-server` (llama.cpp) sidecar via `services.llama-cpp`, serving the
          GGUF on the port parsed from `llm.baseUrl`. The bulletin worker is ordered after it. Requires
          a model — either `llm.modelPath` or `llm.modelUrl` + `llm.modelSha256` (see `modelPath`). On
          Apple-silicon/Asahi, set `llm.package` to a Vulkan build and mind the shader-cache env
          (`local-ml-options.md` §2/§4).
        '';
      };
      package = lib.mkOption {
        type = lib.types.nullOr lib.types.package;
        default = null;
        defaultText = lib.literalExpression "pkgs.llama-cpp";
        description = "The llama.cpp package for the local sidecar (e.g. a Vulkan-enabled build). Defaults to pkgs.llama-cpp.";
      };
      modelPath = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        example = "/var/lib/bulletin-models/qwen3.5-4b-instruct-q4_k_m.gguf";
        description = ''
          Path to the GGUF model file the local sidecar serves. With `llm.serveLocally = true` you must
          supply the model one of three ways: set `modelPath` to a file you place out of band; set it
          to a `pkgs.fetchurl { … }` store path (declarative + reproducible, but the multi-GB blob then
          rides your Nix store / binary cache); or — the recommended hybrid — leave it unset and give
          `llm.modelUrl` + `llm.modelSha256`, and the module fetches + verifies the GGUF into
          `/var/lib/bulletin-models` on activation (declarative reference, no store/cache bloat). When
          both `modelPath` and `modelUrl` are set, the fetch writes into `modelPath`.
        '';
      };
      modelUrl = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        example = "https://huggingface.co/.../qwen3.5-4b-instruct-q4_k_m.gguf";
        description = ''
          URL of the GGUF model to fetch on activation (the recommended hybrid for `serveLocally`): a
          `bulletin-model-fetch` oneshot downloads it into `modelPath` (default
          `/var/lib/bulletin-models/<basename>`) if absent or hash-mismatched, ordered before the
          sidecar so a missing/corrupt model fails loudly rather than degrading silently to baselines.
          Idempotent — a present, verified file is a no-op. Requires `llm.modelSha256`. The model is not
          secret (it never needs agenix); keep it out of the 0700 state dir so the sidecar's user can
          read it (the default dir is world-readable).
        '';
      };
      modelSha256 = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        example = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
        description = ''
          The expected SHA-256 of `llm.modelUrl`, as the hex digest `sha256sum` prints (64 hex chars —
          *not* the SRI / nix-base32 form). The fetch oneshot rejects a download whose hash differs.
          Required whenever `llm.modelUrl` is set.
        '';
      };
      contextSize = lib.mkOption {
        type = lib.types.ints.positive;
        default = 8192;
        description = ''
          Context window (`--ctx-size`) the local sidecar loads with. Bulletin's summarization inputs
          are tiny (budgeted source snippets + a short `max_tokens`), so a small window is plenty.
          Left unset, `llama-server` defaults to the model's *training* context (e.g. Qwen3.5-4B's
          262144), whose KV cache is multiple GB and a real OOM risk on a RAM-bound box — an OOM-kill +
          restart leaves the port down for the reload window, surfacing as transient `connect`-refused
          summarization warnings (`local-ml-options.md` §2/§7). Raise only if a future tier feeds longer
          inputs.
        '';
      };
    };

    openFirewall = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = "Open the health/metrics HTTP port (only needed for non-loopback binds).";
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.database.createLocally || cfg.database.url != null;
        message = "services.bulletin: set database.url when database.createLocally = false.";
      }
      {
        assertion = cfg.email.transport != "smtp" || cfg.email.smtpSecretFile != null;
        message = ''services.bulletin: email.transport = "smtp" requires email.smtpSecretFile.'';
      }
      {
        assertion = !cfg.llm.serveLocally || llmModelPath != null;
        message = "services.bulletin: llm.serveLocally = true requires a model — set llm.modelPath, or llm.modelUrl + llm.modelSha256.";
      }
      {
        assertion = cfg.llm.modelUrl == null || cfg.llm.modelSha256 != null;
        message = "services.bulletin: llm.modelUrl requires llm.modelSha256 (the hex sha256 to verify the download against).";
      }
    ];

    users.users.${user} = {
      isSystemUser = true;
      group = user;
      description = "Bulletin service user";
    };
    users.groups.${user} = { };

    services.postgresql = lib.mkIf cfg.database.createLocally {
      enable = true;
      package = pkgs.postgresql_18;
      ensureDatabases = [ dbName ];
      ensureUsers = [
        {
          name = ownerRole;
          ensureDBOwnership = true;
        }
        # The least-privilege runtime role: created here (so the non-superuser owner needn't hold
        # CREATEROLE — the RLS migration's role-create then no-ops), owns nothing. Its table grants
        # come from `grant_runtime_role`, run during `migrate`. NixOS ensureUsers makes a plain
        # LOGIN role: non-superuser, no BYPASSRLS — exactly what FORCE RLS requires.
        {
          name = runtimeRole;
        }
      ];

      # The single OS user `bulletin` reaches both DB roles over unix-socket peer auth: migrate as
      # the owner, serve/worker as the runtime role. The ident map authorizes that, and the explicit
      # pg_hba line (matched before the default catch-all) applies the map to the bulletin database.
      identMap = ''
        bulletin-map ${user} ${ownerRole}
        bulletin-map ${user} ${runtimeRole}
      '';
      authentication = lib.mkBefore ''
        local ${dbName} ${ownerRole},${runtimeRole} peer map=bulletin-map
      '';
    };

    # CLI wrapper (DATABASE_URL preset) on PATH for seeding/ops + the digest-explain loop.
    environment.systemPackages = [ cliWrapper ];

    # The optional local summarization sidecar (`llama-server` over llama.cpp, OpenAI-compatible). No
    # egress — it binds loopback and the worker calls it over AF_INET (design §12, local-ml-options §4).
    services.llama-cpp = lib.mkIf cfg.llm.serveLocally {
      enable = true;
      package = llmPackage;
      model = llmModelPath;
      host = "127.0.0.1";
      port = llmPort;
      # Stop a reasoning model (Qwen3 et al.) from spending the small summary token budget on a
      # `<think>` block and returning an empty completion — the "EOF while parsing a value at line 1
      # column 0" / timeout symptoms. Two llama.cpp knobs from the server README, belt-and-suspenders:
      #   --jinja             enables the template path the worker's `chat_template_kwargs`
      #                       (`enable_thinking: false`) and reasoning parsing both need.
      #   --reasoning-budget 0  "immediate end" of thinking, enforced server-side for *every* request
      #                       regardless of the model's template — the reliable switch, since
      #                       `enable_thinking: false` is template-dependent and not always honoured
      #                       (llama.cpp#13189). Requires a recent llama.cpp; verify against the pinned
      #                       nixpkgs (this surface is fast-moving — see the handoff doc).
      extraFlags = [
        "--jinja"
        "--reasoning-budget"
        "0"
        # Cap the context window so the KV cache stays small — without this `llama-server` loads the
        # model's full training context (Qwen3.5-4B: 256K), whose multi-GB KV cache risks an OOM-kill +
        # restart, the reload window of which is the transient `connect`-refused the worker logs.
        "--ctx-size"
        (toString cfg.llm.contextSize)
      ];
    };

    # When the model is fetched declaratively, gate the sidecar on a verified file: chain
    # model-fetch → llama-cpp → bulletin so a missing/corrupt GGUF fails at activation (and, under
    # deploy-rs, rolls the generation back) rather than the worker silently degrading to baselines.
    systemd.services.llama-cpp = lib.mkIf llmFetchModel {
      after = [ "bulletin-model-fetch.service" ];
      requires = [ "bulletin-model-fetch.service" ];
    };

    # Fetch-on-activation oneshot (docs/llm-summarization.md): download `llm.modelUrl` into the model
    # path and verify its sha256, only when missing or mismatched (idempotent — a present, verified
    # file is a no-op, so a rebuild doesn't re-pull a 2.5 GB blob). The download stays out of the Nix
    # store / binary cache; only the URL + hash are declarative. Runs as root, writes a world-readable
    # file the sidecar's own user can read.
    systemd.services.bulletin-model-fetch = lib.mkIf llmFetchModel {
      description = "Fetch + verify the Bulletin summarization model (GGUF)";
      # Pulled in + ordered by llama-cpp's `requires`/`after` above — no wantedBy/before needed here.
      path = [
        pkgs.curl
        pkgs.coreutils
      ];
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
      };
      script = ''
        set -euo pipefail
        dest=${lib.escapeShellArg llmModelPath}
        url=${lib.escapeShellArg cfg.llm.modelUrl}
        want=${lib.escapeShellArg (lib.toLower (toString cfg.llm.modelSha256))}

        if [ -f "$dest" ] && [ "$(sha256sum "$dest" | cut -d' ' -f1)" = "$want" ]; then
          echo "model present and verified: $dest"
          exit 0
        fi

        mkdir -p "$(dirname "$dest")"
        tmp="$dest.partial"
        echo "fetching summarization model: $url"
        curl -fL --retry 3 --retry-delay 5 -o "$tmp" "$url"

        got="$(sha256sum "$tmp" | cut -d' ' -f1)"
        if [ "$got" != "$want" ]; then
          rm -f "$tmp"
          echo "sha256 mismatch for $url: got $got, want $want" >&2
          exit 1
        fi
        mv -f "$tmp" "$dest"
        chmod 0644 "$dest"
        echo "model ready: $dest"
      '';
    };

    networking.firewall.allowedTCPPorts = lib.mkIf cfg.openFirewall [ httpPort ];

    # Prune pre-migrate dumps older than 14 days.
    systemd.tmpfiles.rules = lib.mkIf cfg.database.createLocally [
      "e /var/lib/bulletin/backups - - - 14d"
    ];

    # Grant the owner role CREATE on `public` before it migrates. NixOS provisions the owner via
    # `ensureDBOwnership`, but on PG15+ owning the database no longer implies CREATE on `public`
    # unless `public` is owned by `pg_database_owner` — and on a stock cluster `public` is owned by
    # `postgres`, so the owner has only USAGE there. The domain migrations happen to skip this gap
    # (a pre-existing DB has them already recorded in `_sqlx_migrations`), but apalis's
    # `setup_storage` (worker::setup_storage → PostgresStorage::migrations) is the first run to issue
    # unqualified `CREATE`s into `public` — `CREATE EXTENSION pgcrypto`, `generate_ulid()`, and its
    # own `_sqlx_migrations` table — which fail with "permission denied for schema public". This
    # oneshot (run as the postgres superuser, ordered before migrate) closes that gap idempotently
    # for both fresh and existing databases.
    systemd.services.bulletin-grant-public = lib.mkIf cfg.database.createLocally {
      description = "Grant the Bulletin owner role CREATE on schema public";
      after = [ "postgresql.target" ];
      requires = [ "postgresql.target" ];
      before = [ "bulletin-migrate.service" ];
      requiredBy = [ "bulletin-migrate.service" ];
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        User = "postgres";
      };
      script = "${pgPackage}/bin/psql -d ${dbName} -c 'GRANT USAGE, CREATE ON SCHEMA public TO ${ownerRole};'";
    };

    systemd.services.bulletin-migrate = {
      description = "Bulletin database migrations";
      after = pgDeps;
      requires = pgDeps;
      before = [ "bulletin.service" ];
      # Migrate as the owner role (owns the DDL, creates the runtime role + RLS, grants it access).
      environment.DATABASE_URL = migrationUrl;
      serviceConfig =
        hardening
        // {
          Type = "oneshot";
          RemainAfterExit = true;
          StateDirectory = "bulletin";
          StateDirectoryMode = "0700";
          ExecStart = "${lib.getExe cfg.package} migrate";
        }
        # Snapshot the DB immediately before migrating (only on deploy). A failed dump fails the
        # oneshot, which — via the main unit's Requires= — blocks the new binary and leaves the
        # already-running old one up.
        #
        # The dump runs as the `postgres` superuser, not the owner role: the scope-bearing tables
        # carry FORCE ROW LEVEL SECURITY (migrations 0019/0020), and a non-superuser without
        # BYPASSRLS — which the owner deliberately is — makes pg_dump abort with "query would be
        # affected by row-level security policy" (it sets `row_security = off` and refuses a partial
        # dump). `--enable-row-security` would "succeed" but silently drop every private/PII row, so
        # that is not an option. Running backups as the superuser is the standard posture (it is what
        # physical-backup tools do) and keeps the owner free of a standing RLS bypass. The `+` prefix
        # runs ExecStartPre with full privileges so it can `runuser` into postgres (which peer-auths
        # as the DB superuser); the redirect/mkdir run as root into the unit's StateDirectory.
        // lib.optionalAttrs cfg.database.createLocally {
          ExecStartPre = "+${pkgs.writeShellScript "bulletin-pre-migrate-backup" ''
            set -euo pipefail
            mkdir -p "$STATE_DIRECTORY/backups"
            ${pkgs.util-linux}/bin/runuser -u postgres -- ${pgPackage}/bin/pg_dump -Fc ${dbName} \
              > "$STATE_DIRECTORY/backups/pre-migrate-$(date +%Y%m%dT%H%M%S).dump"
          ''}";
        };
    };

    systemd.services.bulletin = {
      description = "Bulletin digest pipeline (server + worker)";
      wantedBy = [ "multi-user.target" ];
      # Requires= (not Wants=) on the migrate oneshot: a failed migration cancels the new binary's
      # start but leaves an already-running instance up — no outage, no half-migrated binary. The
      # sidecar is only `wants`/`after` (not `requires`): summarization is best-effort, so a sidecar
      # that's down or slow must never block the worker or a digest.
      after = pgDeps ++ [ "bulletin-migrate.service" ] ++ lib.optional cfg.llm.serveLocally "llama-cpp.service";
      requires = pgDeps ++ [ "bulletin-migrate.service" ];
      wants = lib.optional cfg.llm.serveLocally "llama-cpp.service";
      environment = {
        DATABASE_URL = databaseUrl;
        RUST_LOG = cfg.log.level;
        BULLETIN_LOG_FORMAT = cfg.log.format;
        BULLETIN_HTTP_ADDR = cfg.http.addr;
        BULLETIN_METRICS_ADDR = cfg.metrics.addr;
        BULLETIN_API_ADDR = cfg.api.addr;
        BULLETIN_EMAIL_TRANSPORT = cfg.email.transport;
        BULLETIN_EMAIL_FROM = cfg.email.from;
        BULLETIN_EMAIL_FILE_DIR = "%S/bulletin/outbox";
      }
      // lib.optionalAttrs cfg.llm.enable {
        # Configure (not enable — the feature build is the switch) the worker's summarization sidecar:
        # where it lives, which model, which prompt version.
        BULLETIN_LLM_BASE_URL = cfg.llm.baseUrl;
        BULLETIN_LLM_MODEL = cfg.llm.model;
        BULLETIN_LLM_PROMPT_VERSION = toString cfg.llm.promptVersion;
      };
      serviceConfig =
        hardening
        // {
          ExecStart = "${lib.getExe cfg.package} all";
          Restart = "on-failure";
          RestartSec = 5;
          StateDirectory = "bulletin";
          StateDirectoryMode = "0700";
          # Mark the unit failed if /health doesn't come up → deploy-rs (or any rollback) reverts.
          ExecStartPost = "${pkgs.curl}/bin/curl --fail --silent --max-time 5 --retry 15 --retry-delay 1 --retry-connrefused http://${cfg.http.addr}/health";
        }
        // lib.optionalAttrs (envSecretFiles != [ ]) {
          # Both secret env files (SMTP creds + the sealed GitHub App credentials) are read as root
          # before the unit drops to the bulletin user, so neither lands in the Nix store.
          EnvironmentFile = envSecretFiles;
        };
    };
  };
}
