"""Hermetic tests: temp files only, every command/systemd/network interaction mocked."""
import importlib.machinery
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import call, patch

loader = importlib.machinery.SourceFileLoader("deploy_woven", str(Path(__file__).with_name("deploy-woven")))
spec = importlib.util.spec_from_loader(loader.name, loader)
if spec is None:
    raise RuntimeError("failed to load deployment helper specification")
deploy = importlib.util.module_from_spec(spec)
loader.exec_module(deploy)
A, B, C = (character * 40 for character in "abc")


class ValidationTests(unittest.TestCase):
    def test_unit_skips_start_without_an_executable_release(self):
        unit = Path(__file__).with_name("woven-server.service").read_text()
        self.assertIn("ConditionFileIsExecutable=/opt/woven/current/woven-server\n", unit)
        self.assertIn("Environment=WOVEN_MANAGED_QUIC=1\n", unit)
        self.assertIn("Environment=WOVEN_MANAGED_WEBTRANSPORT=1\n", unit)
        self.assertIn("Environment=WOVEN_QUIC_BIND=0.0.0.0:4433\n", unit)
        self.assertIn("Environment=WOVEN_WEBTRANSPORT_BIND=0.0.0.0:4434\n", unit)
        self.assertIn("Environment=WOVEN_ADMIN_BIND=127.0.0.1:8083\n", unit)
        self.assertIn("Environment=WOVEN_TLS_CERT_FILE=/etc/woven/tls/current/fullchain.pem\n", unit)
        self.assertIn("Environment=WOVEN_TLS_KEY_FILE=/etc/woven/tls/current/privkey.pem\n", unit)
        self.assertIn("Environment=WOVEN_ADMIN_TOKEN_FILE=/etc/woven/credentials/admin-token\n", unit)
        self.assertIn("ProtectHome=true\n", unit)
        self.assertNotIn("WOVEN_REMOTE_QUIC", unit)
        self.assertNotIn("ConditionPathIsExecutable", unit)

    def test_exact_sha_only(self):
        self.assertEqual(deploy.validate_args([A]), A)
        for args in [[], [A, B], ["main"], ["a" * 39], ["a" * 41], ["A" * 40],
                     ["-" * 40], [A + "\n"], ["a;id"], ["../" + A]]:
            with self.subTest(args=args), self.assertRaises(deploy.DeployError):
                deploy.validate_args(args)

    def test_invalid_arguments_do_not_execute_commands(self):
        with patch.object(deploy, "run") as command:
            with self.assertRaises(deploy.DeployError):
                deploy.main(["main"])
            command.assert_not_called()

    def test_reset_failed_reloads_a_garbage_collected_unit_first(self):
        with patch.object(deploy, "run") as command:
            deploy.service("reset-failed")
        self.assertEqual(
            command.call_args_list,
            [
                call(["/usr/bin/systemctl", "daemon-reload"], seconds=45),
                call(["/usr/bin/systemctl", "reset-failed", deploy.UNIT], seconds=45),
            ],
        )

    def test_dirty_source_fails_before_build(self):
        with patch.object(deploy, "git", return_value=" M Cargo.toml") as git:
            with self.assertRaisesRegex(deploy.DeployError, "dirty"):
                deploy.build(A)
            self.assertEqual(git.call_count, 1)

    def test_noncommit_object_rejected(self):
        with patch.object(deploy, "git", side_effect=["", B]):
            with self.assertRaisesRegex(deploy.DeployError, "exact commit"):
                deploy.build(A)

    def test_exact_isolated_build_is_unprivileged_locked_and_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "releases").mkdir()
            scratch = root / "scratch"
            scratch.mkdir()

            def git(*args, **kwargs):
                if args[0] == "status":
                    return ""
                if args[0] == "rev-parse":
                    return A
                return None

            def user_command(args, **kwargs):
                if "rev-parse" in args:
                    return A
                if "status" in args:
                    return ""
                self.assertEqual(args[-6:], ["/home/this/.cargo/bin/cargo", "build", "--locked",
                                            "--release", "-p", "woven-server"])
                self.assertEqual(kwargs["seconds"], 1800)
                self.assertIn("--chdir=" + str(scratch / "source"), args)
                binary = scratch / "target/release/woven-server"
                binary.parent.mkdir(parents=True)
                binary.write_bytes(b"test binary")

            mkdtemp = tempfile.mkdtemp
            with patch.object(deploy, "ROOT", root), \
                    patch.object(deploy, "git", side_effect=git) as commands, \
                    patch.object(deploy, "as_this", side_effect=user_command), \
                    patch.object(deploy.pwd, "getpwnam"), \
                    patch.object(deploy.os, "fchown"), \
                    patch.object(deploy.tempfile, "mkdtemp", side_effect=lambda **kwargs:
                                 str(scratch) if kwargs["prefix"] == "woven-build-" else mkdtemp(**kwargs)):
                self.assertEqual(deploy.build(A), "releases/" + A)
                self.assertEqual((root / "releases" / A / "woven-server").read_bytes(), b"test binary")
                self.assertFalse(scratch.exists())
                self.assertTrue(any("--detach" in call.args and A in call.args
                                    for call in commands.call_args_list))
                self.assertFalse(any("reset" in call.args or "clean" in call.args
                                     for call in commands.call_args_list))

    def test_busy_lock_rejects_without_build(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "releases").mkdir()
            with (root / "deploy.lock").open("w") as lock, \
                    patch.object(deploy, "ROOT", root), \
                    patch.object(deploy, "trusted_directory"), \
                    patch.object(deploy.os, "geteuid", return_value=0), \
                    patch.object(deploy.os, "umask"), \
                    patch.object(deploy.signal, "signal"), \
                    patch.object(deploy, "fetch_source") as fetch, \
                    patch.object(deploy, "build") as build:
                deploy.fcntl.flock(lock, deploy.fcntl.LOCK_EX | deploy.fcntl.LOCK_NB)
                with self.assertRaisesRegex(deploy.DeploymentBusy, "busy"):
                    deploy.main([A])
                with patch("builtins.print"):
                    self.assertEqual(deploy.cli([A]), 75)
                build.assert_not_called()
                fetch.assert_not_called()


class PreflightAndFetchTests(unittest.TestCase):
    def test_exit_codes_distinguish_busy_from_other_failures(self):
        for error in [deploy.DeployError("failed"), deploy.Cancelled("cancelled"),
                      deploy.RollbackFailed("rollback failed"), OSError(),
                      deploy.subprocess.TimeoutExpired("mock", 1)]:
            with self.subTest(error=error), patch.object(deploy, "main", side_effect=error), \
                    patch("builtins.print"):
                self.assertEqual(deploy.cli([A]), 1)
        with patch.object(deploy, "main"), patch("builtins.print"):
            self.assertEqual(deploy.cli([A]), 0)

    def test_nonroot_rejected_before_commands_or_filesystem_changes(self):
        with patch.object(deploy.os, "geteuid", return_value=1000), \
                patch.object(deploy, "trusted_directory") as directory, \
                patch.object(deploy, "run") as command:
            with self.assertRaisesRegex(deploy.DeployError, "sudo"):
                deploy.main([A])
            directory.assert_not_called()
            command.assert_not_called()

    def test_root_directory_metadata_guard(self):
        for uid, mode, accepted in [(0, deploy.stat.S_IFDIR | 0o755, True),
                                    (1000, deploy.stat.S_IFDIR | 0o755, False),
                                    (0, deploy.stat.S_IFDIR | 0o775, False),
                                    (0, deploy.stat.S_IFDIR | 0o757, False),
                                    (0, deploy.stat.S_IFLNK | 0o755, False),
                                    (0, deploy.stat.S_IFREG | 0o755, False)]:
            with self.subTest(uid=uid, mode=mode), \
                    patch.object(Path, "lstat", return_value=SimpleNamespace(st_uid=uid, st_mode=mode)):
                if accepted:
                    deploy.trusted_directory(Path("/opt/woven"))
                else:
                    with self.assertRaises(deploy.DeployError):
                        deploy.trusted_directory(Path("/opt/woven"))

    def test_bad_root_path_prevents_fetch(self):
        with patch.object(deploy.os, "geteuid", return_value=0), \
                patch.object(deploy.os, "umask"), \
                patch.object(deploy, "trusted_directory", side_effect=deploy.DeployError("unsafe path")), \
                patch.object(deploy, "run") as command:
            with self.assertRaisesRegex(deploy.DeployError, "unsafe path"):
                deploy.main([A])
            command.assert_not_called()

    def test_fixed_fetch_runs_as_this_without_hooks_or_credentials(self):
        with patch.object(deploy, "run") as command:
            deploy.fetch_source()
        command.assert_called_once_with([
            "/usr/sbin/runuser", "-u", "this", "--", "/usr/bin/env", "-i",
            "HOME=/home/this", "USER=this", "LOGNAME=this", "LANG=C",
            "GIT_TERMINAL_PROMPT=0", "PATH=/home/this/.cargo/bin:/usr/bin:/bin",
            "/usr/bin/git", "--no-pager", "-c", "core.hooksPath=/dev/null",
            "-C", "/home/this/woven", "-c", "credential.helper=", "-c", "core.askPass=",
            "fetch", "--no-tags", "https://github.com/jdharrison/woven.git",
            "+refs/heads/main:refs/remotes/origin/main"], seconds=120)

    def test_fetch_holds_lock_and_failure_never_builds_or_switches(self):
        outcomes = [None, deploy.DeployError("fetch failed"),
                    deploy.subprocess.TimeoutExpired("mock fetch", 120)]
        for failure in outcomes:
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                events = []

                def fetch():
                    with (root / "deploy.lock").open("w") as contender:
                        with self.assertRaises(BlockingIOError):
                            deploy.fcntl.flock(contender, deploy.fcntl.LOCK_EX | deploy.fcntl.LOCK_NB)
                    events.append("fetch")
                    if failure is not None:
                        raise failure

                def build(sha):
                    events.append("build")
                    self.assertEqual(sha, A)
                    return "releases/" + A

                with patch.object(deploy, "ROOT", root), \
                        patch.object(deploy.os, "geteuid", return_value=0), \
                        patch.object(deploy.os, "umask"), \
                        patch.object(deploy.signal, "signal"), \
                        patch.object(deploy, "trusted_directory"), \
                        patch.object(deploy, "release_link", return_value=None), \
                        patch.object(deploy, "prune"), \
                        patch.object(deploy, "fetch_source", side_effect=fetch), \
                        patch.object(deploy, "build", side_effect=build) as build_command, \
                        patch.object(deploy, "activate") as activate, \
                        patch.object(deploy, "run", side_effect=AssertionError("real commands forbidden")), \
                        patch("builtins.print"):
                    if failure is None:
                        deploy.main([A])
                        self.assertEqual(events, ["fetch", "build"])
                        activate.assert_called_once_with("releases/" + A)
                    else:
                        with self.assertRaises(type(failure)):
                            deploy.main([A])
                        build_command.assert_not_called()
                        activate.assert_not_called()


class TransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "releases").mkdir()
        self.patch(deploy, "ROOT", self.root)
        # Fixtures are owned by the test runner, not root. Production checks remain mandatory.
        self.patch(deploy, "trusted_directory")

        real_release_link = deploy.release_link

        def fixture_link(name):
            path = self.root / name
            if not path.is_symlink():
                return None
            return deploy.os.readlink(path)

        self.patch(deploy, "release_link", side_effect=fixture_link)
        self.service = self.patch(deploy, "service")
        self.health = self.patch(deploy, "healthy", return_value=True)
        self.signals = self.patch(deploy.signal, "signal")
        # Catch any accidental command execution, even in a new code path.
        self.patch(deploy, "run", side_effect=AssertionError("real commands forbidden"))
        self.real_release_link = real_release_link

    def patch(self, obj, name, *args, **kwargs):
        context = patch.object(obj, name, *args, **kwargs)
        result = context.start()
        self.addCleanup(context.stop)
        return result

    def release(self, sha):
        path = self.root / "releases" / sha
        path.mkdir()
        (path / "woven-server").write_bytes(sha.encode())
        return "releases/" + sha

    def seed(self):
        old_previous = self.release(A)
        old_current = self.release(B)
        candidate = self.release(C)
        deploy.point("previous", old_previous)
        deploy.point("current", old_current)
        return old_previous, old_current, candidate

    def test_success_switches_and_keeps_two(self):
        _, old, candidate = self.seed()
        deploy.activate(candidate)
        self.assertEqual(deploy.release_link("current"), candidate)
        self.assertEqual(deploy.release_link("previous"), old)
        self.assertEqual(sorted(p.name for p in (self.root / "releases").iterdir()), [B, C])
        self.service.assert_any_call("restart")

    def test_failed_health_restores_both_pointers_and_removes_candidate(self):
        previous, old, candidate = self.seed()
        self.health.side_effect = [False, True]
        with self.assertRaisesRegex(deploy.DeployError, "prior release restored"):
            deploy.activate(candidate)
        self.assertEqual(deploy.release_link("current"), old)
        self.assertEqual(deploy.release_link("previous"), previous)
        self.assertFalse((self.root / candidate).exists())
        self.assertEqual(self.health.call_args_list[-1].args, (old,))

    def test_restart_failure_rolls_back(self):
        _, old, candidate = self.seed()
        self.service.side_effect = [None, deploy.DeployError("restart failed"), None, None]
        with self.assertRaisesRegex(deploy.DeployError, "prior release restored"):
            deploy.activate(candidate)
        self.assertEqual(deploy.release_link("current"), old)

    def test_failed_rollback_is_not_reported_as_success(self):
        _, old, candidate = self.seed()
        self.health.return_value = False
        with self.assertRaisesRegex(deploy.DeployError, "ROLLBACK FAILED"):
            deploy.activate(candidate)
        self.assertEqual(deploy.release_link("current"), old)

    def test_main_retains_all_releases_on_rollback_failure_or_uncertainty(self):
        _, old, candidate = self.seed()
        self.patch(deploy.os, "geteuid", return_value=0)
        self.patch(deploy.os, "umask")
        self.patch(deploy, "fetch_source")
        self.patch(deploy, "build", return_value=candidate)
        self.health.return_value = False
        with self.assertRaises(deploy.RollbackFailed):
            deploy.main([C])
        self.assertEqual(deploy.release_link("current"), old)
        self.assertEqual(sorted(p.name for p in (self.root / "releases").iterdir()), [A, B, C])
        with patch.object(deploy, "activate", side_effect=deploy.Cancelled("uncertain")):
            with self.assertRaises(deploy.Cancelled):
                deploy.main([C])
        self.assertEqual(sorted(p.name for p in (self.root / "releases").iterdir()), [A, B, C])

    def test_cancelled_candidate_rolls_back_with_all_cancellation_signals_ignored(self):
        previous, old, candidate = self.seed()
        self.health.side_effect = [deploy.Cancelled("interrupted"), True]
        with self.assertRaisesRegex(deploy.DeployError, "prior release restored"):
            deploy.activate(candidate)
        self.assertEqual(deploy.release_link("current"), old)
        self.assertEqual(deploy.release_link("previous"), previous)
        for signum in deploy.CANCELLATION_SIGNALS:
            self.signals.assert_any_call(signum, deploy.signal.SIG_IGN)
        self.assertIn(deploy.signal.SIGHUP, deploy.CANCELLATION_SIGNALS)

    def test_seeded_first_deployment_failure_restores_seed_under_systemd(self):
        seed = self.release(A)
        candidate = self.release(B)
        deploy.point("current", seed)
        self.health.side_effect = [False, True]
        with self.assertRaisesRegex(deploy.DeployError, "prior release restored"):
            deploy.activate(candidate)
        self.assertEqual(deploy.release_link("current"), seed)
        self.assertIsNone(deploy.release_link("previous"))
        self.assertFalse((self.root / candidate).exists())
        self.assertEqual(self.health.call_args_list[-1].args, (seed,))
        self.assertNotIn("stop", [call.args[0] for call in self.service.call_args_list])

    def test_initial_failure_stops_only_managed_unit(self):
        candidate = self.release(C)
        self.health.return_value = False
        with self.assertRaises(deploy.DeployError):
            deploy.activate(candidate)
        self.assertIsNone(deploy.release_link("current"))
        self.assertIsNone(deploy.release_link("previous"))
        self.service.assert_any_call("stop")
        self.assertEqual(list((self.root / "releases").iterdir()), [])

    def test_same_sha_is_noop_only_if_healthy(self):
        _, old, _ = self.seed()
        deploy.activate(old)
        self.service.assert_not_called()
        self.health.return_value = False
        with self.assertRaises(deploy.DeployError):
            deploy.activate(old)
        self.service.assert_not_called()

    def test_unexpected_pointer_rejected(self):
        (self.root / "current").symlink_to("/tmp/elsewhere")
        with self.assertRaisesRegex(deploy.DeployError, "unexpected release pointer"):
            self.real_release_link("current")


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        (self.root / "releases").mkdir()
        self.scratch = self.root / "scratch"
        self.binary = self.scratch / "target/release/woven-server"
        self.binary.parent.mkdir(parents=True)
        self.binary.write_bytes(b"test binary")
        self.destination = self.root / "releases" / A
        context = patch.object(deploy, "ROOT", self.root)
        context.start()
        self.addCleanup(context.stop)

    def copy(self):
        fd = deploy.open_directory(self.scratch)
        try:
            deploy.copy_artifact(fd, self.destination)
        finally:
            deploy.os.close(fd)

    def assert_no_release_or_stage(self):
        self.assertEqual(list((self.root / "releases").iterdir()), [])

    def test_completed_copy_only_is_promoted(self):
        self.copy()
        self.assertEqual((self.destination / "woven-server").read_bytes(), b"test binary")
        self.assertEqual((self.destination / "woven-server").stat().st_mode & 0o777, 0o755)
        self.assertEqual(list((self.root / "releases").iterdir()), [self.destination])

    def test_rejects_symlinks_at_scratch_target_release_and_binary(self):
        for relative in ["", "target", "target/release", "target/release/woven-server"]:
            path = self.scratch / relative
            moved = self.root / "moved"
            with self.subTest(component=relative):
                path.rename(moved)
                path.symlink_to(moved)
                try:
                    with patch.object(deploy.os, "read") as read:
                        with self.assertRaises(OSError):
                            self.copy()
                        read.assert_not_called()
                    self.assert_no_release_or_stage()
                finally:
                    path.unlink()
                    moved.rename(path)

    def test_scratch_descriptor_survives_path_replacement(self):
        fd = deploy.open_directory(self.scratch)
        moved = self.root / "original"
        self.scratch.rename(moved)
        self.scratch.symlink_to(self.root / "elsewhere")
        try:
            deploy.copy_artifact(fd, self.destination)
        finally:
            deploy.os.close(fd)
        self.assertEqual((self.destination / "woven-server").read_bytes(), b"test binary")

    def test_rejects_empty_oversized_and_nonregular_artifacts(self):
        for size in [0, deploy.MAX_ARTIFACT_BYTES + 1]:
            with self.subTest(size=size):
                with self.binary.open("wb") as binary:
                    binary.truncate(size)
                with self.assertRaisesRegex(deploy.DeployError, "invalid"):
                    self.copy()
                self.assert_no_release_or_stage()
        self.binary.unlink()
        deploy.os.mkfifo(self.binary)
        with self.assertRaisesRegex(deploy.DeployError, "invalid"):
            self.copy()
        self.assert_no_release_or_stage()

    def test_rejects_growth_truncation_and_same_size_mutation(self):
        original_read = deploy.os.read
        for mutation in ["grow", "truncate", "rewrite"]:
            self.binary.write_bytes(b"test binary")
            changed = False

            def read(fd, size):
                nonlocal changed
                if not changed:
                    changed = True
                    if mutation == "truncate":
                        self.binary.write_bytes(b"")
                    elif mutation == "grow":
                        with self.binary.open("ab") as binary:
                            binary.write(b"extra")
                    else:
                        self.binary.write_bytes(b"other bytes")
                        deploy.os.utime(self.binary, ns=(1, 1))
                return original_read(fd, size)

            with self.subTest(mutation=mutation), patch.object(deploy.os, "read", side_effect=read):
                with self.assertRaisesRegex(deploy.DeployError, "truncated|grew|changed"):
                    self.copy()
                self.assert_no_release_or_stage()

    def test_copy_deadline_and_cancellation_remove_staging(self):
        for clock in [[0, deploy.COPY_SECONDS + 1], [0, 0, deploy.COPY_SECONDS + 1]]:
            with self.subTest(clock=clock), patch.object(deploy.time, "monotonic", side_effect=clock):
                with self.assertRaisesRegex(deploy.DeployError, "deadline"):
                    self.copy()
            self.assert_no_release_or_stage()
        with patch.object(deploy.os, "read", side_effect=deploy.Cancelled("interrupted")):
            with self.assertRaises(deploy.Cancelled):
                self.copy()
        self.assert_no_release_or_stage()


class HealthTests(unittest.TestCase):
    def test_child_exit_race_does_not_mask_cancellation(self):
        with patch.object(deploy.subprocess, "Popen") as popen, \
                patch.object(deploy.os, "killpg", side_effect=ProcessLookupError()):
            process = popen.return_value.__enter__.return_value
            process.communicate.side_effect = deploy.Cancelled("interrupted")
            with self.assertRaises(deploy.Cancelled):
                deploy.run(["mock-command"])
            process.wait.assert_called_once()

    def test_cancellation_at_every_health_probe_is_not_retried(self):
        for signum in deploy.CANCELLATION_SIGNALS:
            for probe in ["is-active", "show", "readlink", "-Hlunp", "-Hltnp", "curl", "sleep"]:
                visited = []

                def observe(name, value=None):
                    visited.append(name)
                    if name == probe:
                        deploy.interrupted(signum, None)
                    return value

                def command(args, **kwargs):
                    if "is-active" in args:
                        return observe("is-active")
                    if "show" in args:
                        return observe("show", "123")
                    if args[0] == "/usr/bin/ss":
                        port = args[-1].rsplit(":", 1)[-1]
                        address = (
                            "0.0.0.0:" + port
                            if args[1] == "-Hlunp"
                            else "127.0.0.1:" + port
                        )
                        return observe(args[1], 'UNCONN 0 0 ' + address + ' * users:(("woven",pid=123,fd=9))')
                    return observe("curl")

                with self.subTest(signal=signum, probe=probe), \
                        patch.object(deploy, "run", side_effect=command), \
                        patch.object(deploy.os, "readlink", side_effect=lambda path:
                                     observe("readlink", str(deploy.ROOT / "releases" / A / "woven-server"))), \
                        patch.object(deploy.time, "sleep", side_effect=lambda seconds: observe("sleep")):
                    with self.assertRaises(deploy.Cancelled):
                        deploy.healthy("releases/" + A)
                    self.assertEqual(visited[-1], probe)
                    self.assertEqual(visited.count(probe), 1)

    def test_three_consecutive_full_checks_pass(self):
        def command(args, **kwargs):
            if "show" in args:
                return "123"
            if args[0] == "/usr/bin/ss":
                port = args[-1].rsplit(":", 1)[-1]
                address = (
                    "0.0.0.0:" + port
                    if "-Hlunp" in args
                    else "127.0.0.1:" + port
                )
                return 'UNCONN 0 0 ' + address + ' 0.0.0.0:* users:(("woven-server",pid=123,fd=9))'
            return None

        with patch.object(deploy, "run", side_effect=command) as run, \
                patch.object(deploy.os, "readlink", return_value=str(deploy.ROOT / "releases" / A / "woven-server")), \
                patch.object(deploy.time, "sleep") as sleep:
            self.assertTrue(deploy.healthy("releases/" + A))
            self.assertEqual(sum(call.args[0][0] == "/usr/bin/curl" for call in run.call_args_list), 3)
            self.assertEqual(sleep.call_count, 2)

    def test_checks_are_bounded_and_require_service_owned_listeners(self):
        with patch.object(deploy, "run", side_effect=deploy.DeployError("inactive")) as command, \
                patch.object(deploy.time, "sleep") as sleep:
            self.assertFalse(deploy.healthy("releases/" + A))
            self.assertEqual(command.call_count, 12)
            self.assertEqual(sleep.call_count, 12)

    def test_metrics_alone_cannot_pass(self):
        def command(args, **kwargs):
            if "show" in args:
                return "123"
            if args[0] == "/usr/bin/ss":
                return 'UNCONN 0 0 0.0.0.0:4433 0.0.0.0:* users:(("other",pid=456,fd=9))'
            return None

        with patch.object(deploy, "run", side_effect=command) as run, \
                patch.object(deploy.os, "readlink", return_value=str(deploy.ROOT / "releases" / A / "woven-server")), \
                patch.object(deploy.time, "sleep"):
            self.assertFalse(deploy.healthy("releases/" + A))
            self.assertFalse(any(call.args[0][0] == "/usr/bin/curl" for call in run.call_args_list))


if __name__ == "__main__":
    unittest.main()
