# NixOS VM test: the local `llama-server` summarization sidecar must not be able
# to reach the network (the design §12 / local-ml-options "no data egress"
# invariant), yet the loopback path the bulletin worker uses must keep working.
#
# This is a regression guard for the `IPAddressDeny=any` / `IPAddressAllow=localhost`
# lockdown the module adds to the `llama-cpp` unit. It stubs out the heavy bits the
# property doesn't depend on — real llama.cpp + a GGUF model can't ride a sandboxed
# VM test — with a tiny HTTP stand-in (`fake-llama-server.py`) that probes its own
# network reachability from *inside* the unit's confinement.
{ self, pkgs, lib }:
let
  # Drops in for `pkgs.llama-cpp`: a `llama-server` on PATH that ignores the real
  # flags and serves the egress/loopback probes the test script reads.
  fakeLlama = pkgs.writeShellScriptBin "llama-server" ''
    exec ${pkgs.python3}/bin/python3 ${./fake-llama-server.py} "$@"
  '';
in
pkgs.testers.runNixOSTest {
  name = "bulletin-llama-cpp-egress";

  nodes = {
    # A stand-in "external host" on the test VLAN, serving HTTP on :80. Used to
    # prove the route works for an *unconfined* client on the sidecar box — so a
    # failure from inside the sidecar is the policy, not a missing route.
    internet =
      { pkgs, ... }:
      {
        systemd.services.internet-http = {
          wantedBy = [ "multi-user.target" ];
          serviceConfig.ExecStart = "${pkgs.python3}/bin/python3 -m http.server 80";
        };
        networking.firewall.allowedTCPPorts = [ 80 ];
      };

    sidecar =
      { pkgs, ... }:
      {
        imports = [ self.nixosModules.bulletin ];
        virtualisation.memorySize = 2048;
        virtualisation.diskSize = 4096;
        environment.systemPackages = [ pkgs.curl ];

        services.bulletin = {
          enable = true;
          # Skip the real PostgreSQL/migrate/worker stack: the egress lockdown sits
          # on the standalone `llama-cpp` unit (the worker only `wants` it), so this
          # test never needs the rest of the pipeline to come up.
          database.createLocally = false;
          database.url = "postgres://bulletin@localhost/bulletin";
          llm = {
            enable = true;
            serveLocally = true;
            package = fakeLlama;
            modelPath = "/var/empty/fake.gguf";
            contextSize = 512;
          };
        };
      };
  };

  testScript = ''
    import json

    start_all()

    internet.wait_for_unit("internet-http.service")
    internet.wait_for_open_port(80)
    internet_ip = internet.succeed("ip -4 -o addr show eth1").split()[3].split("/")[0]

    # The lockdown lives on the standalone llama-cpp unit; it must come up without
    # the rest of the pipeline.
    sidecar.wait_for_unit("llama-cpp.service")
    sidecar.wait_for_open_port(8080)

    # 1. Static wiring: the unit actually carries the deny/allow pair.
    show = sidecar.succeed("systemctl show llama-cpp.service -p IPAddressDeny -p IPAddressAllow")
    deny = next(l for l in show.splitlines() if l.startswith("IPAddressDeny="))
    allow = next(l for l in show.splitlines() if l.startswith("IPAddressAllow="))
    assert deny.split("=", 1)[1].strip() != "", f"no IPAddressDeny on the unit: {show!r}"
    assert ("127.0.0" in allow) or ("localhost" in allow), f"loopback not allowed: {show!r}"

    # 2. The route to the external host works for an unconfined client on the same
    #    box, so step 3's failure is provably the policy and not a dead route.
    sidecar.succeed(f"curl -sS --max-time 10 http://{internet_ip}/ -o /dev/null")

    # 3. From inside the confined llama-cpp unit: egress to that same host is blocked
    #    (cgroup BPF → EPERM/EACCES), while loopback (the worker's path) still works.
    egress = json.loads(
        sidecar.succeed(f"curl -sS 'http://127.0.0.1:8080/probe/egress?target={internet_ip}:80'")
    )
    assert egress["ok"] is False, f"sidecar reached an external host — egress not blocked: {egress}"
    assert egress["errno"] in (1, 13), f"egress failed, but not via the IP policy (EPERM/EACCES): {egress}"

    loopback = json.loads(sidecar.succeed("curl -sS http://127.0.0.1:8080/probe/loopback"))
    assert loopback["ok"] is True, f"loopback blocked — this would break the worker: {loopback}"
  '';
}
