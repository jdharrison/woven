"""Hermetic tests for the public certificate installer."""
import importlib.machinery
import importlib.util
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
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

    def install(self, certificate, private_key, *, active=False, restart=None, healthy=True):
        with patch.object(installer, "DESTINATION", self.destination), \
                patch.object(installer, "RELEASES", self.releases), \
                patch.object(installer, "ROOT_UID", os.getuid()), \
                patch.object(installer.os, "geteuid", return_value=0), \
                patch.object(installer, "trusted_directory"), \
                patch.object(installer, "validate_admin_credential"), \
                patch.object(installer, "read_source", side_effect=[certificate, private_key]), \
                patch.object(installer.pwd, "getpwnam", return_value=self.account), \
                patch.object(installer, "validate_pair") as validate, \
                patch.object(installer, "service_active", return_value=active), \
                patch.object(installer, "restart_service", side_effect=restart) as restart_service, \
                patch.object(installer, "healthy", return_value=healthy):
            installer.install()
        return validate, restart_service

    def current_target(self):
        return os.readlink(self.destination / "current")

    def test_requires_root_before_reading_sources(self):
        with patch.object(installer.os, "geteuid", return_value=1000), \
                patch.object(installer, "read_source") as read:
            with self.assertRaisesRegex(installer.InstallError, "root"):
                installer.install()
            read.assert_not_called()

    def test_inactive_install_atomically_selects_versioned_pair_without_starting(self):
        certificate = b"certificate"
        private_key = b"private-key"
        validate, restart = self.install(certificate, private_key)
        target = self.current_target()
        release = self.destination / target
        self.assertEqual((release / "fullchain.pem").read_bytes(), certificate)
        self.assertEqual((release / "privkey.pem").read_bytes(), private_key)
        self.assertEqual((release / "privkey.pem").stat().st_mode & 0o777, 0o640)
        self.assertFalse((self.destination / "previous").exists())
        validate.assert_called_once()
        restart.assert_not_called()

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

    def test_active_service_without_current_pointer_fails_before_staging(self):
        with self.assertRaisesRegex(installer.InstallError, "no managed certificate release"):
            self.install(b"certificate", b"private-key", active=True)
        self.assertEqual(list(self.releases.iterdir()), [])


class ValidationTests(unittest.TestCase):
    def test_admin_credential_is_bounded_ascii_with_exact_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            token = Path(directory) / "admin-token"
            token.write_bytes(b"a" * 32 + b"\n")
            token.chmod(0o640)
            with patch.object(installer, "ADMIN_TOKEN", token), \
                    patch.object(installer, "ROOT_UID", os.getuid()):
                installer.validate_admin_credential(os.getgid())
                token.write_bytes(b"contains whitespace in the token!!")
                with self.assertRaisesRegex(installer.InstallError, "non-whitespace ASCII"):
                    installer.validate_admin_credential(os.getgid())
                token.chmod(0o600)
                with self.assertRaisesRegex(installer.InstallError, "mode 0640"):
                    installer.validate_admin_credential(os.getgid())

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


if __name__ == "__main__":
    unittest.main()
