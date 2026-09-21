"""Hermetic tests for the public certificate installer."""
import importlib.machinery
import importlib.util
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

loader = importlib.machinery.SourceFileLoader(
    "install_woven_tls", str(Path(__file__).with_name("install-woven-tls"))
)
spec = importlib.util.spec_from_loader(loader.name, loader)
if spec is None:
    raise RuntimeError("failed to load TLS installer specification")
installer = importlib.util.module_from_spec(spec)
loader.exec_module(installer)


class InstallTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.destination = Path(self.temp.name) / "etc/woven/tls"
        self.releases = self.destination / "releases"
        self.releases.mkdir(parents=True)
        self.account = type("Account", (), {"pw_uid": os.getuid(), "pw_gid": os.getgid()})()
        self.group = type("Group", (), {"gr_gid": os.getgid()})()

    def install(
        self,
        certificate,
        private_key,
        *,
        active=False,
        restart=None,
        healthy=True,
        gateway_active=False,
        reload_gateway=None,
        gateway_healthy=True,
    ):
        with patch.object(installer, "DESTINATION", self.destination), \
                patch.object(installer, "RELEASES", self.releases), \
                patch.object(installer, "LOCK", Path(self.temp.name) / "install.lock"), \
                patch.object(installer, "ROOT_UID", os.getuid()), \
                patch.object(installer.os, "geteuid", return_value=0), \
                patch.object(installer, "trusted_directory"), \
                patch.object(installer, "validate_admin_credential"), \
                patch.object(installer, "read_source", side_effect=[certificate, private_key]), \
                patch.object(installer.pwd, "getpwnam", return_value=self.account) as get_user, \
                patch.object(installer.grp, "getgrnam", return_value=self.group) as get_group, \
                patch.object(installer, "validate_pair") as validate, \
                patch.object(installer, "service_active", return_value=active), \
                patch.object(installer, "gateway_active", return_value=gateway_active), \
                patch.object(installer, "restart_service", side_effect=restart) as restart_service, \
                patch.object(installer, "reload_gateway", side_effect=reload_gateway) as reload, \
                patch.object(installer, "healthy", return_value=healthy), \
                patch.object(installer, "gateway_healthy", return_value=gateway_healthy):
            installer.install()
        get_user.assert_called_once_with("woven")
        get_group.assert_called_once_with("woven")
        return validate, restart_service, reload

    def current_target(self):
        return os.readlink(self.destination / "current")

    def test_requires_root_before_reading_sources(self):
        with patch.object(installer.os, "geteuid", return_value=1000), \
                patch.object(installer, "read_source") as read:
            with self.assertRaisesRegex(installer.InstallError, "root"):
                installer.install()
            read.assert_not_called()

    def test_runtime_user_primary_group_must_match_named_group(self):
        account = type("Account", (), {"pw_uid": 1001, "pw_gid": 1001})()
        group = type("Group", (), {"gr_gid": 1002})()
        with patch.object(installer.os, "geteuid", return_value=0), \
                patch.object(installer, "LOCK", Path(self.temp.name) / "identity.lock"), \
                patch.object(installer, "ROOT_UID", os.getuid()), \
                patch.object(installer.pwd, "getpwnam", return_value=account), \
                patch.object(installer.grp, "getgrnam", return_value=group), \
                patch.object(installer, "read_source") as read:
            with self.assertRaisesRegex(installer.InstallError, "identity do not match"):
                installer.install()
            read.assert_not_called()

    def test_inactive_install_atomically_selects_versioned_pair_without_starting(self):
        certificate = b"certificate"
        private_key = b"private-key"
        validate, restart, reload = self.install(certificate, private_key)
        target = self.current_target()
        release = self.destination / target
        self.assertEqual((release / "fullchain.pem").read_bytes(), certificate)
        self.assertEqual((release / "privkey.pem").read_bytes(), private_key)
        self.assertEqual((release / "privkey.pem").stat().st_mode & 0o777, 0o400)
        self.assertFalse((self.destination / "previous").exists())
        validate.assert_called_once()
        restart.assert_not_called()
        reload.assert_not_called()

    def test_release_files_are_chowned_to_service_not_root(self):
        requested = []
        real_chown = os.chown

        def chown(path, user_id, group_id):
            requested.append((Path(path).name, user_id, group_id))
            real_chown(path, os.getuid(), os.getgid())

        with patch.object(installer, "DESTINATION", self.destination), \
                patch.object(installer, "RELEASES", self.releases), \
                patch.object(installer, "ROOT_UID", 4242), \
                patch.object(installer.os, "chown", side_effect=chown), \
                patch.object(installer, "validate_pair"):
            target = installer.stage_release(
                b"certificate",
                b"private-key",
                os.getuid(),
                os.getgid(),
            )
        release = self.destination / target
        self.assertIn(("fullchain.pem", os.getuid(), os.getgid()), requested)
        self.assertIn(("privkey.pem", os.getuid(), os.getgid()), requested)
        self.assertTrue(
            any(name.startswith(".tls-") and user_id == 4242 for name, user_id, _ in requested)
        )
        self.assertEqual((release / "fullchain.pem").stat().st_mode & 0o777, 0o400)
        self.assertEqual((release / "privkey.pem").stat().st_mode & 0o777, 0o400)

    def test_active_restart_failure_restores_previous_pointer_and_prunes_candidate(self):
        self.install(b"old-certificate", b"old-private-key")
        old = self.current_target()
        with self.assertRaisesRegex(installer.InstallError, "previous release restored"):
            self.install(
                b"new-certificate",
                b"new-private-key",
                active=True,
                restart=[installer.InstallError("failed"), None],
            )
        self.assertEqual(self.current_target(), old)
        self.assertEqual([path.name for path in self.releases.iterdir()], [old.split("/")[1]])

    def test_failed_service_rollback_retains_both_complete_releases(self):
        self.install(b"old-certificate", b"old-private-key")
        old = self.current_target()
        with self.assertRaisesRegex(installer.InstallError, "releases retained"):
            self.install(
                b"new-certificate",
                b"new-private-key",
                active=True,
                restart=[installer.InstallError("new failed"), installer.InstallError("old failed")],
            )
        self.assertEqual(self.current_target(), old)
        releases = sorted(path.name for path in self.releases.iterdir())
        self.assertEqual(len(releases), 2)
        for release in releases:
            self.assertTrue((self.releases / release / "fullchain.pem").is_file())
            self.assertTrue((self.releases / release / "privkey.pem").is_file())

    def test_active_gateway_loads_new_certificate(self):
        self.install(b"old-certificate", b"old-private-key")
        _, _, reload = self.install(
            b"new-certificate",
            b"new-private-key",
            gateway_active=True,
        )
        reload.assert_called_once_with(self.current_target())

    def test_gateway_reload_failure_restores_previous_pointer_and_gateway(self):
        self.install(b"old-certificate", b"old-private-key")
        old = self.current_target()
        with self.assertRaisesRegex(installer.InstallError, "previous release restored"):
            self.install(
                b"new-certificate",
                b"new-private-key",
                gateway_active=True,
                reload_gateway=[installer.InstallError("new failed"), None],
            )
        self.assertEqual(self.current_target(), old)
        self.assertEqual([path.name for path in self.releases.iterdir()], [old.split("/")[1]])

    def test_failed_gateway_rollback_retains_both_complete_releases(self):
        self.install(b"old-certificate", b"old-private-key")
        old = self.current_target()
        with self.assertRaisesRegex(installer.InstallError, "releases retained"):
            self.install(
                b"new-certificate",
                b"new-private-key",
                gateway_active=True,
                reload_gateway=[
                    installer.InstallError("new failed"),
                    installer.InstallError("old failed"),
                ],
            )
        self.assertEqual(self.current_target(), old)
        self.assertEqual(len(list(self.releases.iterdir())), 2)

    def test_same_target_reconciles_both_active_consumers(self):
        self.install(b"certificate", b"private-key")
        _, restart, reload = self.install(
            b"certificate",
            b"private-key",
            active=True,
            gateway_active=True,
        )
        restart.assert_called_once_with()
        reload.assert_called_once_with(self.current_target())

    def test_interrupted_same_target_failure_rolls_both_consumers_to_previous(self):
        self.install(b"old-certificate", b"old-private-key")
        old = self.current_target()
        self.install(b"new-certificate", b"new-private-key")
        candidate = self.current_target()
        with self.assertRaisesRegex(installer.InstallError, "reconciliation failed; previous"):
            self.install(
                b"new-certificate",
                b"new-private-key",
                active=True,
                gateway_active=True,
                restart=[None, None],
                reload_gateway=[installer.InstallError("new failed"), None],
            )
        self.assertEqual(self.current_target(), old)
        self.assertEqual(os.readlink(self.destination / "previous"), candidate)
        self.assertEqual(len(list(self.releases.iterdir())), 2)

    def test_pointer_restore_failure_still_attempts_both_consumers(self):
        target = "releases/" + "a" * 64
        with patch.object(
            installer,
            "point",
            side_effect=[None, installer.InstallError("previous pointer failed")],
        ) as point, patch.object(installer, "restart_service") as restart, patch.object(
            installer, "reload_gateway"
        ) as reload, patch.object(installer, "prune") as prune:
            with self.assertRaisesRegex(installer.InstallError, "rollback failed"):
                installer.restore(target, None, 1, 1, True, True)
        self.assertEqual(point.call_count, 2)
        restart.assert_called_once_with()
        reload.assert_called_once_with(target)
        prune.assert_not_called()

    def test_active_service_without_current_pointer_fails_before_staging(self):
        with self.assertRaisesRegex(installer.InstallError, "no managed certificate release"):
            self.install(b"certificate", b"private-key", active=True)
        self.assertEqual(list(self.releases.iterdir()), [])

    def test_active_gateway_without_current_pointer_fails_before_staging(self):
        with self.assertRaisesRegex(installer.InstallError, "gateway"):
            self.install(b"certificate", b"private-key", gateway_active=True)
        self.assertEqual(list(self.releases.iterdir()), [])


class ValidationTests(unittest.TestCase):
    def test_install_lock_rejects_concurrent_invocation(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(installer, "LOCK", Path(directory) / "install.lock"), \
                patch.object(installer, "ROOT_UID", os.getuid()):
            first = installer.acquire_lock()
            try:
                with self.assertRaisesRegex(installer.InstallError, "already running"):
                    installer.acquire_lock()
            finally:
                os.close(first)
            second = installer.acquire_lock()
            os.close(second)

    def test_install_lock_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(installer, "LOCK", Path(directory) / "install.lock"):
            installer.LOCK.symlink_to(Path(directory) / "target")
            with self.assertRaisesRegex(installer.InstallError, "cannot open"):
                installer.acquire_lock()

    def test_managed_directory_requires_exact_root_service_group_mode(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(installer, "ROOT_UID", os.getuid()):
            path = Path(directory)
            path.chmod(0o750)
            installer.trusted_directory(path, os.getgid())
            for mode in [0o700, 0o755, 0o770]:
                path.chmod(mode)
                with self.subTest(mode=mode), self.assertRaises(installer.InstallError):
                    installer.trusted_directory(path, os.getgid())
            path.chmod(0o755)
            installer.trusted_directory(path, os.getgid(), managed=False)

    def test_admin_credential_is_bounded_ascii_with_exact_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            token = Path(directory) / "admin-token"
            token.write_bytes(b"a" * 32 + b"\n")
            token.chmod(0o400)
            with patch.object(installer, "ADMIN_TOKEN", token):
                installer.validate_admin_credential(os.getuid(), os.getgid())
                info = token.lstat()
                wrong_owners = [
                    (os.getuid() + 1, os.getgid()),
                    (os.getuid(), os.getgid() + 1),
                ]
                for user_id, group_id in wrong_owners:
                    wrong = SimpleNamespace(
                        st_mode=info.st_mode,
                        st_uid=user_id,
                        st_gid=group_id,
                        st_size=info.st_size,
                    )
                    with self.subTest(user_id=user_id, group_id=group_id), \
                            patch.object(Path, "lstat", return_value=wrong), \
                            self.assertRaisesRegex(installer.InstallError, "service-owned"):
                        installer.validate_admin_credential(os.getuid(), os.getgid())
                token.chmod(0o600)
                token.write_bytes(b"contains whitespace in the token!!")
                token.chmod(0o400)
                with self.assertRaisesRegex(installer.InstallError, "non-whitespace ASCII"):
                    installer.validate_admin_credential(os.getuid(), os.getgid())
                token.chmod(0o600)
                with self.assertRaisesRegex(installer.InstallError, "mode 0400"):
                    installer.validate_admin_credential(os.getuid(), os.getgid())

    def certificate(self, alternative_names):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        certificate = Path(directory.name) / "certificate.pem"
        private_key = Path(directory.name) / "private-key.pem"
        subprocess.run(
            [
                "/usr/bin/openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "30",
                "-subj",
                "/CN=api.woven.host",
                "-addext",
                "subjectAltName=" + alternative_names,
                "-keyout",
                str(private_key),
                "-out",
                str(certificate),
            ],
            check=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=30,
        )
        return certificate, private_key

    def test_accepts_only_the_exact_single_api_hostname(self):
        installer.validate_pair(*self.certificate("DNS:api.woven.host"))
        for names in [
            "DNS:*.woven.host",
            "DNS:api.woven.host,DNS:other.woven.host",
            "DNS:api.woven.host,IP:127.0.0.1",
        ]:
            with self.subTest(names=names), self.assertRaises(installer.InstallError):
                installer.validate_pair(*self.certificate(names))

    def test_certificate_leaf_der_accepts_first_pem_certificate_only(self):
        chain = (
            b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n"
            b"-----BEGIN CERTIFICATE-----\nBAUG\n-----END CERTIFICATE-----\n"
        )
        self.assertEqual(installer.certificate_leaf_der(chain), b"\x01\x02\x03")
        for malformed in [b"", b"-----BEGIN CERTIFICATE-----\n!\n-----END CERTIFICATE-----"]:
            with self.subTest(malformed=malformed), self.assertRaises(installer.InstallError):
                installer.certificate_leaf_der(malformed)

    def test_gateway_health_requires_three_exact_leaf_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory)
            target = "releases/" + "a" * 64
            release = destination / target
            release.mkdir(parents=True)
            (release / "fullchain.pem").write_bytes(
                b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n"
            )
            with patch.object(installer, "DESTINATION", destination), \
                    patch.object(installer, "run", return_value=0) as run, \
                    patch.object(installer, "gateway_peer_leaf", return_value=b"\x01\x02\x03") as peer, \
                    patch.object(installer.time, "sleep") as sleep:
                self.assertTrue(installer.gateway_healthy(target))
        self.assertEqual(run.call_count, 3)
        self.assertEqual(peer.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_gateway_health_resets_on_mismatch_and_is_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory)
            target = "releases/" + "a" * 64
            release = destination / target
            release.mkdir(parents=True)
            (release / "fullchain.pem").write_bytes(
                b"-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n"
            )
            with patch.object(installer, "DESTINATION", destination), \
                    patch.object(installer, "run", return_value=0), \
                    patch.object(
                        installer,
                        "gateway_peer_leaf",
                        side_effect=[b"\x01\x02\x03", b"wrong", b"\x01\x02\x03", b"\x01\x02\x03", b"\x01\x02\x03"],
                    ) as peer, patch.object(installer.time, "sleep") as sleep:
                self.assertTrue(installer.gateway_healthy(target))
            self.assertEqual(peer.call_count, 5)
            self.assertEqual(sleep.call_count, 4)
            with patch.object(installer, "DESTINATION", destination), \
                    patch.object(installer, "run", return_value=0), \
                    patch.object(installer, "gateway_peer_leaf", return_value=b"wrong") as peer, \
                    patch.object(installer.time, "sleep") as sleep:
                self.assertFalse(installer.gateway_healthy(target))
            self.assertEqual(peer.call_count, 12)
            self.assertEqual(sleep.call_count, 12)

    def test_gateway_reload_validates_config_and_selected_leaf(self):
        target = "releases/" + "a" * 64
        with patch.object(installer, "run", return_value=0) as run, \
                patch.object(installer, "gateway_healthy", return_value=True) as healthy:
            installer.reload_gateway(target)
        self.assertEqual(
            [call.args[0] for call in run.call_args_list],
            [
                ["/usr/sbin/nginx", "-t"],
                ["/usr/bin/systemctl", "reload", "nginx.service"],
            ],
        )
        healthy.assert_called_once_with(target)

    def test_health_requires_three_complete_pid_owned_observations(self):
        def command(args, **kwargs):
            if "show" in args:
                return b"123\n"
            if args[0] == "/usr/bin/ss":
                port = args[-1].rsplit(":", 1)[-1]
                address = "0.0.0.0:" + port if "-Hlunp" in args else "127.0.0.1:" + port
                return (
                    'UNCONN 0 0 ' + address + ' * users:(("woven-server",pid=123,fd=9))'
                ).encode()
            return b"" if kwargs.get("capture") else 0

        with patch.object(installer, "run", side_effect=command) as run, \
                patch.object(Path, "resolve", return_value=Path("/opt/woven/releases/current/woven-server")), \
                patch.object(installer.os, "readlink", return_value="/opt/woven/releases/current/woven-server"), \
                patch.object(installer.time, "sleep") as sleep:
            self.assertTrue(installer.healthy())
        self.assertEqual(sum(call.args[0][0] == "/usr/bin/curl" for call in run.call_args_list), 3)
        self.assertEqual(sleep.call_count, 2)


class GatewayConfigTests(unittest.TestCase):
    def test_private_gateway_has_only_reviewed_listener_and_routes(self):
        config = Path(__file__).with_name("woven-admin-gateway.nginx.conf").read_text(
            encoding="utf-8"
        )
        self.assertEqual(config.count("listen 10.128.0.2:8443 ssl;"), 1)
        self.assertNotRegex(config, r"(?m)^\s*listen\s+(?!10\.128\.0\.2:8443\s+ssl;).+;")
        self.assertIn("access_log off;", config)
        self.assertEqual(config.count("proxy_pass "), 2)
        self.assertIn("location = /metrics {", config)
        self.assertIn("proxy_pass http://127.0.0.1:8080/metrics;", config)
        self.assertIn("location ^~ /v1/ {", config)
        self.assertIn("proxy_pass http://127.0.0.1:8083;", config)
        self.assertIn("location / {\n            return 404;", config)


if __name__ == "__main__":
    unittest.main()
