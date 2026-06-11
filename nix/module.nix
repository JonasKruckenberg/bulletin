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

  databaseUrl =
    if cfg.database.createLocally then
      "postgres:///${dbName}?host=/run/postgresql"
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
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.bulletin;
      defaultText = lib.literalExpression "bulletin.packages.\${system}.bulletin";
      description = "The `bulletin` package to run.";
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
        example = "postgres://bulletin@db.internal/bulletin";
        description = "DATABASE_URL to use when `database.createLocally = false`.";
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
    ];

    users.users.${user} = {
      isSystemUser = true;
      group = user;
      description = "Bulletin service user";
    };
    users.groups.${user} = { };

    services.postgresql = lib.mkIf cfg.database.createLocally {
      enable = true;
      ensureDatabases = [ dbName ];
      ensureUsers = [
        {
          name = user;
          ensureDBOwnership = true;
        }
      ];
    };

    # CLI wrapper (DATABASE_URL preset) on PATH for seeding/ops + the digest-explain loop.
    environment.systemPackages = [ cliWrapper ];

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
      environment.DATABASE_URL = databaseUrl;
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
      # start but leaves an already-running instance up — no outage, no half-migrated binary.
      after = pgDeps ++ [ "bulletin-migrate.service" ];
      requires = pgDeps ++ [ "bulletin-migrate.service" ];
      environment = {
        DATABASE_URL = databaseUrl;
        RUST_LOG = cfg.log.level;
        BULLETIN_LOG_FORMAT = cfg.log.format;
        BULLETIN_HTTP_ADDR = cfg.http.addr;
        BULLETIN_METRICS_ADDR = cfg.metrics.addr;
        BULLETIN_EMAIL_TRANSPORT = cfg.email.transport;
        BULLETIN_EMAIL_FROM = cfg.email.from;
        BULLETIN_EMAIL_FILE_DIR = "%S/bulletin/outbox";
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
        // lib.optionalAttrs (cfg.email.smtpSecretFile != null) {
          EnvironmentFile = [ cfg.email.smtpSecretFile ];
        };
    };
  };
}
