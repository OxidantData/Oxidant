"""Strict executable boundary for test_ci_ownership; fixture I/O only."""
import atexit
import json
import os
import re
import signal
import subprocess
import sys
from pathlib import Path

root = Path(os.environ["RECORD_ROOT"])
tool = Path(sys.argv[0]).name
args = sys.argv[1:]
with (root / "commands.jsonl").open("a") as out:
    out.write(json.dumps({"tool": tool, "argv": args, "executable": sys.argv[0], "pid": os.getpid(),
                          "endpoint": os.environ.get("OXIDANT_MINIO_ENDPOINT"),
                          "minio_test": os.environ.get("OXIDANT_MINIO_TEST"),
                          "allow_no_oracle": os.environ.get("OXIDANT_TPCDS_ALLOW_NO_ORACLE")}) + "\n")


exit_status = 0


def record_exit():
    with (root / "exits.jsonl").open("a") as out:
        out.write(json.dumps({"pid": os.getpid(), "exit": exit_status}) + "\n")


atexit.register(record_exit)


def finish(status: int | str = 0):
    global exit_status
    exit_status = status if isinstance(status, int) else 1
    raise SystemExit(status)


def unhandled(kind, value, traceback):
    global exit_status
    exit_status = 1
    sys.__excepthook__(kind, value, traceback)


sys.excepthook = unhandled


def refuse():
    (root / "unexpected-command").write_text(f"{tool} {args!r}")
    finish(f"UNEXPECTED {tool} argv: {args!r}")


def require(condition):
    if not condition:
        refuse()


def save(model):
    (root / "docker.json").write_text(json.dumps(model))


if tool == "sudo":
    if args == ["unzip", "-o", "/tmp/duckdb.zip", "-d", "/usr/local/bin"]:
        (root / "global-duckdb").write_text("overwritten by workflow")
    elif args in (["apt-get", "update", "-y"], ["apt-get", "install", "-y", "build-essential", "flex", "bison"], ["apt-get", "install", "-y", "unzip"]):
        (root / "global-packages").write_text("unowned global mutation")
    else:
        require(args == ["rm", "-rf", "/usr/share/dotnet", "/usr/local/lib/android", "/opt/ghc",
                         "/usr/local/share/boost", "/usr/local/.ghcup", str(root / "tool cache")])
        (root / "tool cache/sentinel").unlink()
elif tool in ("dbgen", "dsdgen"):
    require(args == (["-h"] if tool == "dbgen" else ["-HELP"]))
    print("fixture kit help")
elif tool == "sleep":
    require(args == ["1"])
elif tool == "cargo":
    require(args in (["test", "--workspace"], ["run", "-p", "oxidant-bench", "--", "tpcds", "--sf", "1", "--data", str(root / "tpc data/tpcds-sf1")]))
    if os.environ.get("SIGNAL_CARGO"):
        exit_status = -signal.SIGTERM
        record_exit()
        os.kill(os.getpid(), signal.SIGTERM)
    if args[0] == "run":
        # The unchanged Rust oracle probes `duckdb --version` through PATH first.
        subprocess.run(["duckdb", "--version"], check=True)
    finish(int(os.environ.get("CARGO_EXIT", "0")))
elif tool == "duckdb":
    require(args == ["--version"])
    print("fixture DuckDB version (not the real oracle)")
    finish(23 if os.environ.get("FAIL_VERSION") else 0)
elif tool == "unzip":
    require(len(args) == 4 and args[0] == "-q" and args[2] == "-d")
    archive, directory = Path(args[1]), Path(args[3])
    require(directory.is_relative_to(root / "runner temp") and archive == directory / "duckdb.zip" and archive.is_file())
    if os.environ.get("FAIL_UNZIP"):
        finish(24)
    (directory / "duckdb").symlink_to(root / "record-tool")
elif tool == "curl":
    if len(args) == 4 and args[:3] == ["-fsSL", "https://github.com/duckdb/duckdb/releases/download/v1.3.2/duckdb_cli-linux-amd64.zip", "-o"]:
        target = root / "global-zip" if args[3] == "/tmp/duckdb.zip" else Path(args[3])
        require(target == root / "global-zip" or target.is_relative_to(root / "runner temp"))
        if os.environ.get("FAIL_DOWNLOAD"):
            finish(22)
        target.write_text("fixture archive")
        finish(0)
    require(len(args) in (2, 6))
    if len(args) == 2:
        require(args[0] == "-fsS")
    else:
        require(args[:5] == ["-fsS", "--connect-timeout", "2", "--max-time", "2"])
    require(re.fullmatch(r"http://127\.0\.0\.1:(9000|2[0-9]{4})/minio/health/live", args[-1]))
    finish(22 if os.environ.get("FAIL_HEALTH") else 0)
elif tool == "docker":
    model = json.loads((root / "docker.json").read_text())
    containers = model["containers"]
    if args == ["context", "show"]:
        print("fixture")
    elif args == ["context", "inspect", "fixture", "--format", "{{json .Endpoints.docker.Host}}"]:
        print(json.dumps(os.environ.get("CONTEXT_HOST", "unix:///fixture/docker.sock")))
    else:
        pinned = args[:2] == ["--context", "fixture"]
        if pinned:
            args = args[2:]
        if args == ["info", "--format", "{{.ID}}"]:
            require(pinned)
            print(os.environ.get("DAEMON_ID", "fixture-daemon"))
        elif args == ["rm", "-f", "oxidant-minio"]:
            require(not pinned)
            containers.pop("f" * 64, None)
            save(model)
        elif args[:2] == ["run", "-d"]:
            require(not pinned and args == ["run", "-d", "--name", "oxidant-minio", "-p", "9000:9000",
                "-e", "MINIO_ROOT_USER=minioadmin", "-e", "MINIO_ROOT_PASSWORD=minioadmin123",
                "quay.io/minio/minio:latest", "server", "/data"])
            containers["a" * 64] = {"Id": "a" * 64, "Config": {"Labels": {}}, "Name": "/oxidant-minio"}
            save(model)
            print("a" * 64)
        elif args[:2] == ["run", "--rm"]:
            require(not pinned and args == ["run", "--rm", "--network", "host", "-e",
                "MC_HOST_local=http://minioadmin:minioadmin123@127.0.0.1:9000", "minio/mc", "mb", "--ignore-existing", "local/oxidant-test"])
        elif args and args[0] == "create":
            require(pinned and len(args) >= 12 and args[1] == "--cidfile" and args[3] == "--name")
            cidfile = Path(args[2])
            require(cidfile.is_relative_to(root / "runner temp") and not cidfile.exists())
            require(re.fullmatch(r"oxidant-minio-123-[12]-build-test-[0-9a-f]{32}-(server|client)", args[4]))
            require(args[5] == "--label" and args[6].startswith("io.oxidant.ci.owner="))
            require(args[7] == "--label" and args[8] == "io.oxidant.ci.job=fixture/engine/123/" + os.environ["GITHUB_RUN_ATTEMPT"] + "/build-test")
            rest = args[9:]
            if args[4].endswith("-server"):
                require(rest == ["-p", "127.0.0.1::9000", "-e", "MINIO_ROOT_USER=minioadmin", "-e",
                    "MINIO_ROOT_PASSWORD=minioadmin123", "quay.io/minio/minio:latest", "server", "/data"])
            else:
                require(rest[:3] == ["--network", "host", "-e"])
                require(re.fullmatch(r"MC_HOST_local=http://minioadmin:minioadmin123@127\.0\.0\.1:2[0-9]{4}", rest[3]))
                require(rest[4:] == ["minio/mc", "mb", "--ignore-existing", "local/oxidant-test"])
            cid = f'{model["next"]:064x}'
            if os.environ.get("NAME_CONFLICT") == args[4].rsplit("-", 1)[1]:
                containers["c" * 64] = {"Id": "c" * 64, "Name": "/" + args[4], "Config": {"Labels": {}}}
                save(model)
                finish(17)
            port = str(20000 + model["next"])
            model["next"] += 1
            containers[cid] = {"Id": cid, "Name": "/" + args[4], "Config": {"Labels": dict(a.split("=", 1) for a in (args[6], args[8]))},
                "NetworkSettings": {"Ports": {"9000/tcp": [{"HostIp": "127.0.0.1", "HostPort": port}]}}, "role": args[4].rsplit("-", 1)[1]}
            cidfile.write_text(cid)
            model.setdefault("volumes", ["unrelated-volume"]).append(cid)
            save(model)
            print(cid)
            if os.environ.get("TERMINATE_AFTER_CREATE") == containers[cid]["role"]:
                os.kill(os.getppid(), signal.SIGTERM)
            if os.environ.get("FAIL_CREATE") == containers[cid]["role"]:
                finish(17)
        elif args and args[0] == "start":
            require(pinned and (len(args) == 2 or (len(args) == 3 and args[1] == "-a")))
            require(args[-1] in containers)
            finish(18 if os.environ.get("FAIL_START") == containers[args[-1]].get("role") else 0)
        elif args[:2] == ["container", "inspect"]:
            require(pinned and len(args) == 3 and re.fullmatch(r"[0-9a-f]{64}", args[2]))
            require(args[2] in containers)
            print(json.dumps([containers[args[2]]]))
        elif args[:2] == ["container", "ls"]:
            require(pinned and len(args) == 8 and args[2:5] == ["-a", "--no-trunc", "--filter"] and args[6:] == ["--format", "{{.ID}}"])
            require(re.fullmatch(r"id=[0-9a-f]{64}", args[5]))
            if args[5][3:] in containers:
                print(args[5][3:])
        elif args[:2] == ["rm", "-f"]:
            require(pinned and (len(args) == 3 or (len(args) == 4 and args[2] == "-v")) and re.fullmatch(r"[0-9a-f]{64}", args[-1]))
            if args[-1] not in containers:
                finish(1)
            if os.environ.get("FAIL_CLEANUP"):
                finish(19)
            del containers[args[-1]]
            if "-v" in args:
                model["volumes"].remove(args[-1])
            save(model)
        else:
            refuse()
else:
    refuse()
