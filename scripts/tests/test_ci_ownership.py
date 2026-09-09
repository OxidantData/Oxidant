"""Offline command-boundary contracts; never contacts Docker or the network.

Extract the named workflow steps verbatim (not a replacement workflow). The
small reader deliberately supports only the run/env scalars used by these steps;
GitHub's YAML syntax is validated separately, not by this reader.
"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = Path(os.environ.get("CI_OWNERSHIP_WORKFLOW", ROOT / ".github/workflows/ci.yml"))


def steps(job):
    text = WORKFLOW.read_text().splitlines()
    begin = text.index(f"  {job}:") + 1
    end = next((i for i in range(begin, len(text)) if re.match(r"  [\w-]+:", text[i])), len(text))
    result = []
    for line in text[begin:end]:
        if line.startswith("      - "):
            result.append({"lines": []})
        if result:
            result[-1]["lines"].append(line)
    for step in result:
        lines = step.pop("lines")
        step["name"] = lines[0].removeprefix("      - name: ")
        step["env"] = {}
        for i, line in enumerate(lines):
            if line.startswith(("        run: ", "      - run: ")):
                value = line.split("run: ", 1)[1]
                if value == "|":
                    body = []
                    for following in lines[i + 1:]:
                        if following and not following.startswith("          "):
                            break
                        body.append(following[10:])
                    step["run"] = "\n".join(body) + "\n"
                else:
                    step["run"] = value + "\n"
            if line == "        env:":
                for following in lines[i + 1:]:
                    if not following.startswith("          "):
                        break
                    if following.lstrip().startswith("#"):
                        continue
                    key, value = following.strip().split(": ", 1)
                    step["env"][key] = value.strip('"')
            if line.startswith("        if: "):
                step["if"] = line.split("if: ", 1)[1]
    return result


class Ownership(unittest.TestCase):
    def setUp(self):
        parent = os.environ.get("CI_OWNERSHIP_RECEIPTS")
        if parent:
            Path(parent).mkdir(parents=True, exist_ok=True)
        self.temp = Path(tempfile.mkdtemp(prefix=self._testMethodName + " spaces ", dir=parent))
        if not parent:
            self.addCleanup(shutil.rmtree, self.temp)
        self.bin = self.temp / "bin"
        self.bin.mkdir()
        for command in ("bash", "df", "mkdir", "mktemp", "dirname", "head", "seq"):
            executable = shutil.which(command)
            self.assertIsNotNone(executable, f"required test utility: {command}")
            (self.bin / command).symlink_to(str(executable))
        (self.bin / "python3").symlink_to(sys.executable)
        recorder = self.temp / "record-tool"
        recorder.write_text(f"#!{sys.executable} -B\n" + (ROOT / "scripts/tests/ci_record_tool.py").read_text())
        recorder.chmod(0o700)
        for command in ("docker", "curl", "cargo", "sudo", "unzip", "sleep", "duckdb"):
            (self.bin / command).symlink_to(recorder)
        for directory in ("home", "tmp", "runner temp", "tool cache"):
            (self.temp / directory).mkdir()
        (self.temp / "tool cache/sentinel").write_text("unrelated SDK")
        (self.temp / "global-duckdb").write_text("unrelated global binary")
        (self.temp / "global-zip").write_text("unrelated download")
        self.env = {
            "PATH": str(self.bin), "HOME": str(self.temp / "home"),
            "TMPDIR": str(self.temp / "tmp"), "RUNNER_TEMP": str(self.temp / "runner temp"),
            "AGENT_TOOLSDIRECTORY": str(self.temp / "tool cache"),
            "GITHUB_ENV": str(self.temp / "github-env"), "GITHUB_PATH": str(self.temp / "github-path"),
            "GITHUB_REPOSITORY": "fixture/engine", "GITHUB_RUN_ID": "123",
            "GITHUB_RUN_ATTEMPT": "1", "GITHUB_JOB": "build-test",
            "RECORD_ROOT": str(self.temp), "PYTHONDONTWRITEBYTECODE": "1",
            "OXIDANT_MINIO_ENDPOINT": "http://127.0.0.1:19999",
        }
        self.model = self.temp / "docker.json"
        self.model.write_text(json.dumps({"containers": {"f" * 64: {
            "Id": "f" * 64, "Name": "/oxidant-minio", "Config": {"Labels": {}},
            "NetworkSettings": {"Ports": {"9000/tcp": [{"HostIp": "127.0.0.1", "HostPort": "9000"}]}},
        }}, "next": 1}))

    def named(self, job, name):
        return next(s for s in steps(job) if s["name"] == name)

    def run_step(self, step, outcome="success"):
        env = {**self.env, **step.get("env", {})}
        script = step["run"].replace("${{ steps.duckdb.outcome }}", outcome)
        self.assertNotIn("${{", script, "unhandled Actions substitution")
        result = subprocess.run([str(self.bin / "bash"), "--noprofile", "--norc", "-eo", "pipefail", "-c", script],
                                cwd=ROOT, env=env, text=True, capture_output=True, timeout=90, check=False)
        with (self.temp / "shells.jsonl").open("a") as out:
            out.write(json.dumps({"step": step["name"], "script": script, "env": env,
                                  "exit": result.returncode, "stdout": result.stdout, "stderr": result.stderr}) + "\n")
        for filename, key in (("github-env", None), ("github-path", "PATH")):
            path = self.temp / filename
            if path.exists():
                for line in path.read_text().splitlines():
                    if key:
                        self.env[key] = line + os.pathsep + self.env[key]
                    else:
                        name, value = line.split("=", 1)
                        self.env[name] = value
                path.write_text("")
        return result

    def commands(self):
        log = self.temp / "commands.jsonl"
        return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

    def lifecycle(self):
        results = []
        for step in steps("build-test"):
            if step["name"] in ("Start MinIO service container", "test (workspace)") and not any(r.returncode for r in results):
                results.append(self.run_step(step))
        cleanup = [s for s in steps("build-test") if s["name"] == "Clean up owned MinIO containers"]
        for step in cleanup:
            self.assertEqual(step.get("if"), "always()")
            results.append(self.run_step(step))
        return results

    def test_workflow_owns_minio_and_passes_dynamic_endpoint(self):
        results = self.lifecycle()
        self.assertTrue(all(r.returncode == 0 for r in results), str([(r.returncode, r.stderr) for r in results]))
        containers = json.loads(self.model.read_text())["containers"]
        with self.subTest(contract="unrelated fixed-name sentinel"):
            self.assertIn("f" * 64, containers, "workflow deleted unrelated fixed-name MinIO")
        with self.subTest(contract="owned cleanup"):
            self.assertEqual(list(containers), ["f" * 64])
        calls = self.commands()
        cargo = [c for c in calls if c["tool"] == "cargo"]
        with self.subTest(contract="actual workspace consumer"):
            self.assertEqual(len(cargo), 1)
            self.assertEqual(cargo[0]["argv"], ["test", "--workspace"])
            self.assertEqual(cargo[0]["minio_test"], "1")
            self.assertRegex(cargo[0]["endpoint"], r"^http://127\.0\.0\.1:2[0-9]{4}$")
            self.assertIn(cargo[0]["endpoint"] + "/minio/health/live", [c["argv"][-1] for c in calls if c["tool"] == "curl"])

    def kit_fixture(self):
        kits = self.temp / "tpc kits"
        for relative in ("tpch-kit/dbgen/dbgen", "tpcds-kit/tools/dsdgen"):
            binary = kits / relative
            binary.parent.mkdir(parents=True, exist_ok=True)
            binary.symlink_to(self.temp / "record-tool")
        self.env.update(OXIDANT_TPC_KITS=str(kits), OXIDANT_TPC_DATA=str(self.temp / "tpc data"))

    def test_missing_build_dependencies_never_install_globally(self):
        self.kit_fixture()
        result = self.run_step(self.named("query-gates", "Fetch + build official TPC kits (dbgen / dsdgen)"))
        with self.subTest(contract="no package-manager mutation"):
            self.assertFalse((self.temp / "global-packages").exists(), "workflow mutated host packages")
        self.assertNotEqual(result.returncode, 0, "missing build dependencies must fail clearly")
        self.assertIn("gcc", result.stderr)

    def test_duckdb_install_is_private_and_consumer_resolves_it(self):
        self.kit_fixture()
        install = self.run_step(self.named("query-gates", "Install DuckDB CLI (optional result oracle)"))
        self.assertEqual(install.returncode, 0, install.stderr)
        consumer = self.run_step(self.named("query-gates", "TPC-DS coverage (Q1–Q99, official dsdgen SF1, pass-set ratchet)"))
        self.assertEqual(consumer.returncode, 0, consumer.stderr)
        with self.subTest(contract="global binary preserved"):
            self.assertEqual((self.temp / "global-duckdb").read_text(), "unrelated global binary")
        with self.subTest(contract="global download preserved"):
            self.assertEqual((self.temp / "global-zip").read_text(), "unrelated download")
        duckdb = [c for c in self.commands() if c["tool"] == "duckdb"]
        with self.subTest(contract="exact owned oracle executable"):
            self.assertEqual(len(duckdb), 2)
            self.assertEqual(duckdb[0]["executable"], duckdb[1]["executable"])
            self.assertTrue(Path(duckdb[1]["executable"]).is_relative_to(self.temp / "runner temp"))
        self.assertIsNone(next(c for c in self.commands() if c["tool"] == "cargo")["allow_no_oracle"])

    def tearDown(self):
        unexpected = self.temp / "unexpected-command"
        self.assertFalse(unexpected.exists(), unexpected.read_text() if unexpected.exists() else "")

    def case(self, label):
        case = Ownership(self._testMethodName)
        case.setUp()
        (case.temp / "subcase.json").write_text(json.dumps({"method": self._testMethodName, "subcase": label}))
        self.addCleanup(case.doCleanups)
        self.addCleanup(case.tearDown)
        return case

    def cleanup_step(self):
        return self.named("build-test", "Clean up owned MinIO containers")

    def test_startup_and_primary_failure_status(self):
        for label, overrides, primary, cleanup in (
            ("server partial create", {"FAIL_CREATE": "server"}, 17, 0),
            ("client partial create", {"FAIL_CREATE": "client"}, 17, 0),
            ("server start", {"FAIL_START": "server"}, 18, 0),
            ("client start", {"FAIL_START": "client"}, 18, 0),
            ("workspace failure", {"CARGO_EXIT": "7"}, 7, 0),
            ("primary beats cleanup", {"CARGO_EXIT": "7", "FAIL_CLEANUP": "1"}, 7, 1),
            ("cleanup alone fails", {"FAIL_CLEANUP": "1"}, 1, 1),
        ):
            with self.subTest(case=label):
                case = self.case(label)
                case.env.update(overrides)
                results = case.lifecycle()
                self.assertEqual([r.returncode for r in results], [primary, cleanup])
                containers = json.loads(case.model.read_text())["containers"]
                self.assertIn("f" * 64, containers)
                if not cleanup:
                    self.assertEqual(list(containers), ["f" * 64])
                if label not in ("workspace failure", "primary beats cleanup", "cleanup alone fails"):
                    self.assertFalse(any(c["tool"] == "cargo" for c in case.commands()))
                if cleanup:
                    del case.env["FAIL_CLEANUP"]
                    self.assertEqual(case.run_step(case.cleanup_step()).returncode, 0)
                before = len(case.commands())
                self.assertEqual(case.run_step(case.cleanup_step()).returncode, 0)
                self.assertEqual(len(case.commands()), before, "repeated successful cleanup contacts no daemon")

    def test_health_failure_never_runs_workspace(self):
        self.env["FAIL_HEALTH"] = "1"
        results = self.lifecycle()
        self.assertEqual([r.returncode for r in results], [22, 0])
        self.assertFalse(any(c["tool"] == "cargo" for c in self.commands()))
        self.assertEqual(len([c for c in self.commands() if c["tool"] == "curl"]), 30)
        self.assertEqual(list(json.loads(self.model.read_text())["containers"]), ["f" * 64])

    def test_cleanup_refuses_untrusted_or_malformed_receipts(self):
        modes = ("wrong-label", "missing-label", "wrong-cid", "missing-cid", "short-cid", "repeated-cid",
                 "duplicate-cids", "missing-context", "wrong-context-host", "wrong-daemon", "wrong-job",
                 "duplicate-json-key", "duplicate-role", "omitted-role", "boolean-version", "state-symlink")
        for mode in modes:
            with self.subTest(mode=mode):
                case = self.case(mode)
                case.env["FAIL_CLEANUP"] = "1"
                self.assertEqual([r.returncode for r in case.lifecycle()], [1, 1])
                del case.env["FAIL_CLEANUP"]
                directory = Path(case.env["CI_MINIO_STATE"])
                statefile = directory / "state.json"
                state = json.loads(statefile.read_text())
                cidfile = directory / "server.cid"
                cid = cidfile.read_text()
                model = json.loads(case.model.read_text())
                if mode == "wrong-label":
                    model["containers"][cid]["Config"]["Labels"]["io.oxidant.ci.owner"] = "e" * 32
                elif mode == "missing-label":
                    model["containers"][cid]["Config"]["Labels"] = {}
                elif mode == "wrong-cid":
                    cidfile.write_text("f" * 64)
                elif mode == "missing-cid":
                    cidfile.unlink()
                elif mode == "short-cid":
                    cidfile.write_text(cid[:12])
                elif mode == "repeated-cid":
                    cidfile.write_text(cid + "\n" + cid)
                elif mode == "duplicate-cids":
                    (directory / "client.cid").write_text(cid)
                elif mode == "missing-context":
                    del state["context"]
                elif mode == "wrong-context-host":
                    case.env["CONTEXT_HOST"] = "unix:///fixture/other.sock"
                elif mode == "wrong-daemon":
                    case.env["DAEMON_ID"] = "other-daemon"
                elif mode == "wrong-job":
                    case.env["GITHUB_RUN_ATTEMPT"] = "2"
                elif mode == "duplicate-role":
                    state["roles"] = ["server", "server"]
                elif mode == "omitted-role":
                    state["roles"] = []
                elif mode == "boolean-version":
                    state["version"] = True
                statefile.write_text(json.dumps(state))
                if mode == "duplicate-json-key":
                    statefile.write_text(statefile.read_text()[:-1] + ', "version": 1}')
                elif mode == "state-symlink":
                    saved = directory / "copied-state"
                    statefile.rename(saved)
                    statefile.symlink_to(saved)
                case.model.write_text(json.dumps(model))
                before_model = case.model.read_bytes()
                before_commands = len(case.commands())
                result = case.run_step(case.cleanup_step())
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(case.model.read_bytes(), before_model, "refusal removed a container")
                self.assertFalse(any(c["tool"] == "docker" and "rm" in c["argv"] for c in case.commands()[before_commands:]))

    def test_two_runs_do_not_adopt_or_delete_each_other(self):
        self.env["FAIL_CLEANUP"] = "1"
        self.assertEqual([r.returncode for r in self.lifecycle()], [1, 1])
        first = self.env["CI_MINIO_STATE"]
        first_model = json.loads(self.model.read_text())["containers"]
        del self.env["FAIL_CLEANUP"]
        self.env["GITHUB_RUN_ATTEMPT"] = "2"
        self.assertEqual([r.returncode for r in self.lifecycle()], [0, 0])
        self.assertNotEqual(first, self.env["CI_MINIO_STATE"])
        self.assertEqual(json.loads(self.model.read_text())["containers"], first_model)
        endpoints = [c["endpoint"] for c in self.commands() if c["tool"] == "cargo"]
        self.assertEqual(len(set(endpoints)), 2)
        self.env.update(GITHUB_RUN_ATTEMPT="1", CI_MINIO_STATE=first)
        self.assertEqual(self.run_step(self.cleanup_step()).returncode, 0)
        self.assertEqual(list(json.loads(self.model.read_text())["containers"]), ["f" * 64])

    def test_optional_oracle_failure_keeps_existing_fallback(self):
        for mode in ("FAIL_DOWNLOAD", "FAIL_UNZIP", "FAIL_VERSION", "missing-unzip"):
            with self.subTest(mode=mode):
                case = self.case(mode)
                case.kit_fixture()
                original_path = case.env["PATH"]
                if mode == "missing-unzip":
                    (case.bin / "unzip").unlink()
                else:
                    case.env[mode] = "1"
                install = case.run_step(case.named("query-gates", "Install DuckDB CLI (optional result oracle)"))
                self.assertNotEqual(install.returncode, 0)
                self.assertEqual(case.env["PATH"], original_path, "failed oracle was exported")
                case.env.pop(mode, None)
                consumer = case.run_step(case.named("query-gates", "TPC-DS coverage (Q1–Q99, official dsdgen SF1, pass-set ratchet)"), outcome="failure")
                self.assertEqual(consumer.returncode, 0, consumer.stderr)
                self.assertEqual(next(c for c in case.commands() if c["tool"] == "cargo")["allow_no_oracle"], "1")
                self.assertEqual((case.temp / "global-duckdb").read_text(), "unrelated global binary")
                self.assertEqual((case.temp / "global-zip").read_text(), "unrelated download")
                self.assertFalse(any(c["tool"] == "sudo" for c in case.commands()))

    def test_provisioned_kits_and_each_missing_build_tool(self):
        tools = ("gcc", "g++", "make", "flex", "bison")
        for missing in (None, *tools):
            with self.subTest(missing=missing):
                case = self.case(str(missing))
                case.kit_fixture()
                for tool in tools:
                    if tool != missing:
                        (case.bin / tool).symlink_to(case.temp / "record-tool")
                result = case.run_step(case.named("query-gates", "Fetch + build official TPC kits (dbgen / dsdgen)"))
                self.assertEqual(result.returncode, 1 if missing else 0, result.stderr)
                if missing:
                    self.assertIn("Missing host build dependency: " + missing, result.stderr)
                else:
                    self.assertEqual([c["tool"] for c in case.commands()], ["dbgen", "dsdgen"])
                self.assertFalse(any(c["tool"] == "sudo" for c in case.commands()))

    def test_early_setup_failure_has_no_container_cleanup(self):
        for mode in ("allocation", "remote-context", "ambient-host", "environment-export"):
            with self.subTest(mode=mode):
                case = self.case(mode)
                if mode == "allocation":
                    case.env["RUNNER_TEMP"] = str(case.temp / "global-zip")
                elif mode == "remote-context":
                    case.env["CONTEXT_HOST"] = "tcp://remote.example:2376"
                elif mode == "ambient-host":
                    case.env["DOCKER_HOST"] = "unix:///unexpected.sock"
                else:
                    case.env["GITHUB_ENV"] = str(case.temp / "home")
                results = case.lifecycle()
                self.assertEqual([r.returncode for r in results], [1, 0])
                self.assertEqual(list(json.loads(case.model.read_text())["containers"]), ["f" * 64])
                self.assertFalse(any(c["tool"] == "docker" and any(a in c["argv"] for a in ("create", "start", "rm")) for c in case.commands()))

    def test_always_cleanup_recovers_terminated_setup(self):
        self.env["TERMINATE_AFTER_CREATE"] = "server"
        results = self.lifecycle()
        self.assertIn(results[0].returncode, (-15, 143))
        self.assertEqual(results[-1].returncode, 0, results[-1].stderr)
        self.assertEqual(list(json.loads(self.model.read_text())["containers"]), ["f" * 64])
        self.assertFalse(any(c["tool"] == "cargo" for c in self.commands()))

    def test_cleanup_removes_only_owned_anonymous_volumes(self):
        self.assertEqual([r.returncode for r in self.lifecycle()], [0, 0])
        self.assertEqual(json.loads(self.model.read_text())["volumes"], ["unrelated-volume"], "owned image-declared volumes leaked")

    def test_suite_is_wired_into_existing_cheap_gate(self):
        self.assertTrue(any(s.get("run") == "python3 -B scripts/tests/test_ci_ownership.py\n" for s in steps("fmt")))

    def test_create_without_cid_never_adopts_matching_name(self):
        self.env["NAME_CONFLICT"] = "server"
        results = self.lifecycle()
        self.assertEqual([r.returncode for r in results], [17, 1])
        containers = json.loads(self.model.read_text())["containers"]
        self.assertEqual(set(containers), {"f" * 64, "c" * 64})
        attempted_name = next(c["argv"][6] for c in self.commands() if c["tool"] == "docker" and "create" in c["argv"])
        self.assertEqual(containers["c" * 64]["Name"], "/" + attempted_name)
        self.assertFalse(any(c["tool"] == "docker" and "rm" in c["argv"] for c in self.commands()))
        self.assertFalse(any(c["tool"] == "cargo" for c in self.commands()))

    def test_signalled_workspace_preserves_signal_status(self):
        self.env.update(SIGNAL_CARGO="1", FAIL_CLEANUP="1")
        self.assertEqual([r.returncode for r in self.lifecycle()], [143, 1])
        self.assertIn("f" * 64, json.loads(self.model.read_text())["containers"])

    def test_healthy_without_ambient_endpoint(self):
        del self.env["OXIDANT_MINIO_ENDPOINT"]
        self.assertEqual([r.returncode for r in self.lifecycle()], [0, 0])
        cargo = next(c for c in self.commands() if c["tool"] == "cargo")
        self.assertRegex(cargo["endpoint"], r"^http://127\.0\.0\.1:2[0-9]{4}$")
        self.assertEqual(list(json.loads(self.model.read_text())["containers"]), ["f" * 64])

    def test_capacity_reporting_preserves_shared_sdks(self):
        step = next(s for s in steps("build-test") if s["name"] in ("Reclaim runner disk", "Report runner disk capacity"))
        result = self.run_step(step)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.temp / "tool cache/sentinel").exists(), "workflow deleted unrelated SDK sentinel")
        self.assertFalse(any(c["tool"] == "sudo" for c in self.commands()))


class RecordingResult(unittest.TextTestResult):
    """Optional machine-readable receipts; ordinary CI needs no output directory."""
    def startTestRun(self):
        super().startTestRun()
        self.methods = []
        self.subcases = []

    def startTest(self, test):
        self.methods.append(test.id())
        super().startTest(test)

    def addSubTest(self, test, subtest, err):
        self.subcases.append({"id": str(subtest), "passed": err is None})
        super().addSubTest(test, subtest, err)

    def stopTestRun(self):
        super().stopTestRun()
        destination = os.environ.get("CI_OWNERSHIP_RECEIPTS")
        if destination:
            Path(destination).mkdir(parents=True, exist_ok=True)
            (Path(destination) / "tests-run.json").write_text(json.dumps({
                "tests_run": self.testsRun, "methods": self.methods, "subcases": self.subcases,
                "failures": len(self.failures), "errors": len(self.errors), "skips": self.skipped,
                "successful": self.wasSuccessful(),
            }, indent=2) + "\n")


if __name__ == "__main__":
    unittest.main(testRunner=unittest.TextTestRunner(resultclass=RecordingResult, verbosity=2))
