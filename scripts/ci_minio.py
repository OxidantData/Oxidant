"""CI-only MinIO lifetime: private receipt, pinned local daemon, CID + labels.

Usage: python3 -B scripts/ci_minio.py run -- cargo test --workspace
       python3 -B scripts/ci_minio.py cleanup /absolute/receipt/directory
No names, lists of labels, or ambient endpoint confer cleanup authority.
"""
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

OWNER = "io.oxidant.ci.owner"
JOB = "io.oxidant.ci.job"


def call(*args, env=None):
    return subprocess.check_output(args, text=True, env=env).strip()


def identity():
    keys = ("GITHUB_REPOSITORY", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_JOB")
    values = [os.environ[k] for k in keys]
    if any(not re.fullmatch(r"[A-Za-z0-9_./-]+", v) for v in values):
        raise ValueError("invalid CI job identity")
    return "/".join(values)


def docker(state, *args):
    return call("docker", "--context", state["context"], *args)


def host(context):
    value = json.loads(call("docker", "context", "inspect", context, "--format", "{{json .Endpoints.docker.Host}}"))
    if not isinstance(value, str) or not value.startswith("unix:///"):
        raise ValueError("MinIO requires a local Unix-socket Docker context (loopback health/MC)")
    return value


def write_state(directory, state):
    temporary = directory / "state.next"
    temporary.write_text(json.dumps(state) + "\n")
    temporary.replace(directory / "state.json")


def read_state(directory):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate receipt key")
            result[key] = value
        return result

    if not directory.is_absolute() or directory.is_symlink() or (directory / "state.json").is_symlink():
        raise ValueError("receipt must be a private absolute directory, not a symlink")
    state = json.loads((directory / "state.json").read_text(), object_pairs_hook=unique)
    if set(state) != {"version", "job", "owner", "context", "host", "daemon", "roles", "cleaned"}:
        raise ValueError("invalid receipt fields")
    if type(state["version"]) is not int or state["version"] != 1 or state["job"] != identity() or not re.fullmatch(r"[0-9a-f]{32}", state["owner"]):
        raise ValueError("receipt does not belong to this CI job")
    if state["roles"] not in ([], ["server"], ["server", "client"]) or type(state["cleaned"]) is not bool:
        raise ValueError("invalid receipt lifecycle")
    if not all(isinstance(state[k], str) and state[k] for k in ("context", "host", "daemon")):
        raise ValueError("missing Docker context identity")
    if any((directory / (role + ".cid")).exists() for role in {"server", "client"} - set(state["roles"])):
        raise ValueError("receipt omits an allocated role")
    return state


def container(state, cid):
    data = json.loads(docker(state, "container", "inspect", cid))
    if len(data) != 1 or data[0]["Id"] != cid:
        raise ValueError("container CID mismatch")
    labels = data[0]["Config"].get("Labels") or {}
    if labels.get(OWNER) != state["owner"] or labels.get(JOB) != state["job"]:
        raise ValueError("container ownership mismatch; refusing removal")
    return data[0]


def cleanup(directory):
    state = read_state(directory)
    if state["cleaned"]:
        return
    if host(state["context"]) != state["host"] or docker(state, "info", "--format", "{{.ID}}") != state["daemon"]:
        raise ValueError("Docker context/daemon changed; refusing cleanup")
    owned = []
    seen = set()
    for role in state["roles"]:
        path = directory / (role + ".cid")
        if path.is_symlink():
            raise ValueError("CID file is a symlink")
        cid = path.read_text()
        if not re.fullmatch(r"[0-9a-f]{64}\n?", cid):
            raise ValueError("missing or malformed immutable CID")
        cid = cid.rstrip("\n")
        if cid in seen:
            raise ValueError("duplicate immutable CID")
        seen.add(cid)
        present = docker(state, "container", "ls", "-a", "--no-trunc", "--filter", "id=" + cid, "--format", "{{.ID}}")
        if present:
            if present != cid:
                raise ValueError("ambiguous container ID")
            container(state, cid)
            owned.append(cid)
    # Preflight all receipts before removing any resource. Never fall back to name.
    for cid in reversed(owned):
        docker(state, "rm", "-f", "-v", cid)
    state["cleaned"] = True
    write_state(directory, state)


def create(directory, state, role, arguments):
    state["roles"].append(role)
    write_state(directory, state)  # record intent before a possibly partial create
    name = "oxidant-minio-{}-{}-{}-{}-{}".format(
        os.environ["GITHUB_RUN_ID"], os.environ["GITHUB_RUN_ATTEMPT"], os.environ["GITHUB_JOB"], state["owner"], role)
    docker(state, "create", "--cidfile", str(directory / (role + ".cid")), "--name", name,
           "--label", OWNER + "=" + state["owner"], "--label", JOB + "=" + state["job"], *arguments)
    cid = (directory / (role + ".cid")).read_text().strip()
    if not re.fullmatch(r"[0-9a-f]{64}", cid):
        raise ValueError("Docker did not produce an immutable CID")
    container(state, cid)
    return cid


def run(command):
    directory = None
    status = 0
    try:
        if os.environ.get("DOCKER_HOST"):
            raise ValueError("DOCKER_HOST is ambiguous; select a local DOCKER_CONTEXT instead")
        state = {"version": 1, "job": identity(), "owner": uuid.uuid4().hex,
                 "context": call("docker", "context", "show"), "roles": [], "cleaned": False}
        state["host"] = host(state["context"])
        state["daemon"] = docker(state, "info", "--format", "{{.ID}}")
        directory = Path(tempfile.mkdtemp(prefix="oxidant-minio-", dir=os.environ["RUNNER_TEMP"]))
        write_state(directory, state)
        with open(os.environ["GITHUB_ENV"], "a") as output:
            output.write(f"CI_MINIO_STATE={directory}\n")
        print(f"MinIO ownership receipt: {directory}; context={state['context']}; daemon={state['daemon']}", flush=True)
        server = create(directory, state, "server", ["-p", "127.0.0.1::9000", "-e", "MINIO_ROOT_USER=minioadmin",
                        "-e", "MINIO_ROOT_PASSWORD=minioadmin123", "quay.io/minio/minio:latest", "server", "/data"])
        docker(state, "start", server)
        ports = container(state, server)["NetworkSettings"]["Ports"]["9000/tcp"]
        if len(ports) != 1 or ports[0]["HostIp"] != "127.0.0.1" or not re.fullmatch(r"[0-9]{1,5}", ports[0]["HostPort"]):
            raise ValueError("MinIO must publish exactly one loopback port")
        port = int(ports[0]["HostPort"])
        if not 1 <= port <= 65535:
            raise ValueError("invalid MinIO port")
        endpoint = f"http://127.0.0.1:{port}"
        for attempt in range(30):
            try:
                call("curl", "-fsS", "--connect-timeout", "2", "--max-time", "2", endpoint + "/minio/health/live")
                break
            except subprocess.CalledProcessError:
                if attempt == 29:
                    raise
                time.sleep(1)
        client = create(directory, state, "client", ["--network", "host", "-e",
                        f"MC_HOST_local=http://minioadmin:minioadmin123@127.0.0.1:{port}",
                        "minio/mc", "mb", "--ignore-existing", "local/oxidant-test"])
        docker(state, "start", "-a", client)
        # Override conflicting ambient endpoints at the actual workspace consumer.
        status = subprocess.call(command, env={**os.environ, "OXIDANT_MINIO_ENDPOINT": endpoint})
    except subprocess.CalledProcessError as error:
        status = error.returncode
    except KeyboardInterrupt:
        status = 130
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"MinIO setup failed: {error}", file=sys.stderr)
        status = 1
    finally:
        if directory is not None:
            try:
                cleanup(directory)
            except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
                print(f"MinIO cleanup refused/failed; retain {directory}: {error}", file=sys.stderr)
                status = status or 1
    return 128 - status if status < 0 else status


if __name__ == "__main__":
    if len(sys.argv) >= 4 and sys.argv[1:3] == ["run", "--"]:
        sys.exit(run(sys.argv[3:]))
    if len(sys.argv) == 3 and sys.argv[1] == "cleanup":
        try:
            cleanup(Path(sys.argv[2]))
        except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
            sys.exit(f"MinIO cleanup refused/failed: {error}")
    else:
        sys.exit("usage: ci_minio.py run -- COMMAND [ARG...] | cleanup ABSOLUTE_RECEIPT_DIR")
