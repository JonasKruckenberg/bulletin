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
      "postgres://${runtimeRole}@/${dbName}?host=/run/postgresql"
    else
      cfg.database.url;

  migrationUrl =
    if cfg.database.createLocally then
      "postgres://${ownerRole}@/${dbName}?host=/run/postgresql"
    else if cfg.database.migrationUrl != null then
      cfg.database.migrationUrl
    else
      cfg.database.url;

  # CLI wrapper on PATH: presets DATABASE_URL + email env to the deployed values so operators run
  # `sudo -u bulletin bulletin debug …` (seeding, status, the digest-explain loop) without flags.
  cliWrapper = pkgs.writeShellScriptBin "bulletin" ''
    export DATABASE_URL="''${DATABASE_URL:-${toString databaseUrl}}"
    export BULLETIN_EMAIL_TRANSPORT="''${BULLETIN_EMAIL_TRANSPORT:-${cfg.email.transport}}"
    export BULLETIN_EMAIL_FROM="''${BULLETIN_EMAIL_FROM:-${cfg.email.from}}"
    export BULLETIN_EMAIL_FILE_DIR="''${BULLETIN_EMAIL_FILE_DIR:-/var/lib/bulletin/outbox}"
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

  # Root-readable secret env files loaded via systemd `EnvironmentFile` (SMTP creds + the sealed
  # GitHub App credentials), so neither enters the Nix store.
  envSecretFiles =
    lib.optional (cfg.email.smtpSecretFile != null) cfg.email.smtpSecretFile
    ++ lib.optional (cfg.github.secretFile != null) cfg.github.secretFile;

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
        default = 1;
        description = "Prompt/schema version; bump to invalidate + re-summarize the whole corpus.";
      };
      serveLocally = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = ''
          Provision a local `llama-server` (llama.cpp) sidecar via `services.llama-cpp`, serving
          `llm.modelPath` on the port parsed from `llm.baseUrl`. The bulletin worker is ordered after
          it. Requires `llm.modelPath`. On Apple-silicon/Asahi, set `llm.package` to a Vulkan build and
          mind the shader-cache env (`local-ml-options.md` §2/§4).
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
        example = "/var/lib/bulletin/models/qwen3.5-4b-instruct-q4_k_m.gguf";
        description = "Path to the GGUF model file served by the local sidecar. Required when `llm.serveLocally = true`.";
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
        assertion = !cfg.llm.serveLocally || cfg.llm.modelPath != null;
        message = "services.bulletin: llm.serveLocally = true requires llm.modelPath (a GGUF file).";
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
      model = cfg.llm.modelPath;
      host = "127.0.0.1";
      port = llmPort;
    };

    networking.firewall.allowedTCPPorts = lib.mkIf cfg.openFirewall [ httpPort ];

    # Prune pre-migrate dumps older than 14 days.
    systemd.tmpfiles.rules = lib.mkIf cfg.database.createLocally [
      "e /var/lib/bulletin/backups - - - 14d"
    ];

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
        // lib.optionalAttrs cfg.database.createLocally {
          ExecStartPre = pkgs.writeShellScript "bulletin-pre-migrate-backup" ''
            set -euo pipefail
            mkdir -p "$STATE_DIRECTORY/backups"
            ${pgPackage}/bin/pg_dump -Fc ${dbName} \
              > "$STATE_DIRECTORY/backups/pre-migrate-$(date +%Y%m%dT%H%M%S).dump"
          '';
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
