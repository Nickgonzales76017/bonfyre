#!/usr/bin/env python3
"""Real native peers, temporary grants and bytes, IPv4 loopback only.

Uses only Python's standard library. Its small MessagePack fixture codec and
hashlib BLAKE2b independently exercise the native packet and proof contract.
Set SWARM_TEST_CFLAGS / SWARM_TEST_LDFLAGS for an isolated SQLite toolchain,
or SWARM_TEST_BINARY to test an already-built binary.
"""
import hashlib
import os
from pathlib import Path
import secrets
import select
import shlex
import socket
import sqlite3
import struct
import subprocess
import tempfile
import threading
import time
import unittest


def pack(value):
    if value is True:
        return b"\xc3"
    if value is False:
        return b"\xc2"
    if isinstance(value, int):
        if 0 <= value < 128:
            return bytes([value])
        return b"\xcf" + struct.pack(">Q", value)
    if isinstance(value, str):
        data = value.encode()
        return (bytes([0xa0 + len(data)]) if len(data) < 32 else b"\xda" + struct.pack(">H", len(data))) + data
    if isinstance(value, bytes):
        return b"\xc5" + struct.pack(">H", len(value)) + value
    if isinstance(value, list):
        if len(value) >= 16:
            raise ValueError("fixture array too large")
        return bytes([0x90 + len(value)]) + b"".join(map(pack, value))
    raise TypeError(type(value))


def unpack(data):
    position = 0

    def take(n):
        nonlocal position
        value = data[position:position + n]
        if len(value) != n:
            raise ValueError("truncated MessagePack")
        position += n
        return value

    def number(n):
        return int.from_bytes(take(n), "big")

    def one():
        tag = number(1)
        if tag < 128:
            return tag
        if tag in (0xc2, 0xc3):
            return tag == 0xc3
        if tag in (0xcc, 0xcd, 0xce, 0xcf):
            return number({0xcc: 1, 0xcd: 2, 0xce: 4, 0xcf: 8}[tag])
        if 0xa0 <= tag <= 0xbf:
            return take(tag - 0xa0).decode()
        if tag in (0xd9, 0xda, 0xdb):
            return take(number({0xd9: 1, 0xda: 2, 0xdb: 4}[tag])).decode()
        if tag in (0xc4, 0xc5, 0xc6):
            return take(number({0xc4: 1, 0xc5: 2, 0xc6: 4}[tag]))
        if 0x90 <= tag <= 0x9f:
            return [one() for _ in range(tag - 0x90)]
        if 0x80 <= tag <= 0x8f:
            return {one(): one() for _ in range(tag - 0x80)}
        raise ValueError("unsupported fixture tag: " + hex(tag))

    value = one()
    if position != len(data):
        raise ValueError("trailing MessagePack bytes")
    return value


def receive(sock, n):
    chunks = []
    while n:
        chunk = sock.recv(n)
        if not chunk:
            raise EOFError("peer disconnected")
        chunks.append(chunk)
        n -= len(chunk)
    return b"".join(chunks)


def read_frame(sock):
    length = int.from_bytes(receive(sock, 4), "big")
    if not 0 < length <= 1024:
        raise ValueError("frame exceeds bound")
    raw = receive(sock, length)
    return unpack(raw), raw


def send_frame(sock, value, fragmented=False):
    payload = pack(value)
    frame = struct.pack(">I", len(payload)) + payload
    if fragmented:
        for byte in frame:
            sock.sendall(bytes([byte]))
    else:
        sock.sendall(frame)


class ScopedTransferTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.build_dir = tempfile.TemporaryDirectory(prefix="bonfyre-swarm-build-")
        supplied = os.environ.get("SWARM_TEST_BINARY")
        if supplied:
            cls.binary = str(Path(supplied).resolve())
            return
        root = Path(__file__).resolve().parents[3]
        lib = root / "lib/libbonfyre"
        cls.binary = str(Path(cls.build_dir.name) / "bonfyre-swarm")
        command = shlex.split(os.environ.get("CC", "cc")) + [
            "-std=c11", "-O1", "-g", "-Wall", "-Wextra",
            "-Werror=implicit-function-declaration", "-DBONFYRE_SHA256_PORTABLE",
            "-I" + str(lib / "include"),
        ] + shlex.split(os.environ.get("SWARM_TEST_CFLAGS", "")) + [
            str(root / "cmd/BonfyreSwarm/src/main.c"),
            str(lib / "src/bf_sha256.c"), str(lib / "bf_crypto.c"), str(lib / "bf_msgpack.c"),
        ] + shlex.split(os.environ.get("SWARM_TEST_LDFLAGS", "-lsqlite3")) + ["-o", cls.binary]
        built = subprocess.run(command, capture_output=True, text=True, timeout=90)
        if built.returncode:
            raise RuntimeError(built.stderr)

    @classmethod
    def tearDownClass(cls):
        cls.build_dir.cleanup()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="bonfyre-scoped-peer-")
        self.root = Path(self.temp.name)
        self.data = b"admitted test output\x00\xff\n" * 17003
        self.source = self.root / "source.bin"
        self.source.write_bytes(self.data)
        self.key = secrets.token_bytes(32)
        self.key_file = self.root / "grant.key"
        self.key_file.write_bytes(self.key)
        self.key_file.chmod(0o600)
        self.options = {
            "scope": hashlib.sha256(b"test-account/test-org").hexdigest(),
            "recipient": hashlib.sha256(b"test-recipient").hexdigest(),
            "sha256": hashlib.sha256(self.data).hexdigest(),
            "size": str(len(self.data)), "transfer-id": secrets.token_hex(16),
            "key-file": str(self.key_file), "expires": str(int(time.time()) + 30),
        }
        self.processes = []
        self.serial = 0

    def tearDown(self):
        for process in self.processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
            for stream in [process.stdin, process.stdout, process.stderr]:
                if stream:
                    stream.close()
        self.temp.cleanup()

    def command(self, mode, **changes):
        values = dict(self.options)
        values.update(changes)
        return [self.binary, mode] + [part for name, value in values.items() for part in ["--" + name, str(value)]]

    def run_native(self, command):
        process = subprocess.run(command, capture_output=True, timeout=8)
        packet = unpack(process.stdout)
        self.assertEqual(packet[0], 1)
        self.assertEqual(packet[1], process.returncode == 0)
        self.assertEqual(packet[2]["exit_status"], process.returncode)
        self.assertNotIn(self.key, process.stdout + process.stderr)
        self.assertNotIn(str(self.source).encode(), process.stdout)
        return process, packet[2]

    def start_seed(self, **changes):
        opts = {"artifact": self.source, "port": 0, "index": self.root / "sender.db"}
        opts.update(changes)
        process = subprocess.Popen(self.command("scoped-seed", **opts), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.processes.append(process)
        self.assertTrue(select.select([process.stderr], [], [], 4)[0], "seed did not become ready")
        ready = process.stderr.readline()
        if not ready.startswith(b"SWARM_READY 127.0.0.1 "):
            out, err = process.communicate(timeout=3)
            self.fail("seed failed: " + repr((ready, unpack(out), err)))
        port = int(ready.split()[-1])
        proc_tcp = Path(f"/proc/{process.pid}/net/tcp")
        if proc_tcp.exists():
            listeners = [line.split()[1] for line in proc_tcp.read_text().splitlines()[1:]
                         if line.split()[1].endswith(f":{port:04X}") and line.split()[3] == "0A"]
            self.assertEqual(listeners, [f"0100007F:{port:04X}"])
        return process, port

    def finish_seed(self, process):
        out, err = process.communicate(timeout=5)
        self.assertNotIn(self.key, out + err)
        packet = unpack(out)
        self.assertEqual(packet[2]["exit_status"], process.returncode)
        return process.returncode, packet[2]

    def fetch(self, connection_port, **changes):
        self.serial += 1
        opts = {"port": connection_port, "out": self.root / f"received-{self.serial}.bin",
                "index": self.root / f"receiver-{self.serial}.db"}
        opts.update(changes)
        process, receipt = self.run_native(self.command("scoped-fetch", **opts))
        return process.returncode, receipt, Path(opts["out"]), Path(opts["index"])

    def proof(self, kind, nonce, **changes):
        opts = dict(self.options)
        opts.update(changes)
        material = ("bf.swarm.v1." + kind).encode().ljust(24, b"\0") + nonce
        material += b"".join(bytes.fromhex(opts[k]) for k in ["transfer-id", "scope", "recipient", "sha256"])
        material += struct.pack(">QQ", int(opts["size"]), int(opts["expires"]))
        return hashlib.blake2b(material, key=self.key, digest_size=32).digest()

    def request(self, nonce, **changes):
        opts = dict(self.options)
        opts.update(changes)
        return [1, "request"] + [bytes.fromhex(opts[k]) for k in ["transfer-id", "scope", "recipient", "sha256"]] + [int(opts["size"]), self.proof("request", nonce, **changes)]

    def peer(self, port):
        sock = socket.create_connection(("127.0.0.1", port), timeout=3)
        challenge, raw = read_frame(sock)
        self.assertEqual(challenge[:3], [1, "challenge", bytes.fromhex(self.options["transfer-id"])])
        self.assertNotIn(self.key, raw)
        self.assertEqual(len(challenge[3]), 32)
        return sock, challenge[3]

    def fake_server(self, behavior):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(5)
        errors = []
        def serve():
            try:
                with listener:
                    with listener.accept()[0] as sock:
                        sock.settimeout(5)
                        behavior(sock)
            except BaseException as error:
                errors.append(error)
        port = listener.getsockname()[1]
        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        return port, thread, errors

    def fake_accept(self, sock, proof_valid=True):
        nonce = secrets.token_bytes(32)
        transfer = bytes.fromhex(self.options["transfer-id"])
        send_frame(sock, [1, "challenge", transfer, nonce])
        request, raw = read_frame(sock)
        self.assertNotIn(self.key, raw)
        self.assertEqual(request, self.request(nonce))
        proof = self.proof("accepted", nonce) if proof_valid else bytes(32)
        send_frame(sock, [1, "accepted", transfer, len(self.data), proof])
        return nonce, transfer

    def test_capabilities_are_bounded_and_truthful(self):
        result = subprocess.run([self.binary, "transfer-capabilities"], capture_output=True, timeout=3)
        self.assertEqual(result.returncode, 0)
        packet = unpack(result.stdout)
        self.assertEqual(packet[:2], [1, True])
        self.assertEqual(packet[2]["transport"], "loopback-tcp")
        self.assertFalse(packet[2]["remote_transport"])
        self.assertFalse(packet[2]["distributed_inference"])
        self.assertFalse(packet[2]["automatic_discovery"])
        self.assertEqual(packet[2]["max_bytes"], 64 * 1024 * 1024)

    def test_two_native_peers_verify_publish_and_confirm(self):
        seed, port = self.start_seed()
        rc, receipt, output, database = self.fetch(port)
        self.assertEqual((rc, receipt["state"]), (0, "stored_confirmed"))
        self.assertEqual(output.read_bytes(), self.data)
        self.assertEqual(output.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.finish_seed(seed)[0], 0)
        with sqlite3.connect(database) as db:
            row = db.execute("SELECT scope_hash,recipient_hash,sha256,size_bytes,state FROM swarm_scoped_transfers").fetchone()
            self.assertEqual(row, (self.options["scope"], self.options["recipient"], self.options["sha256"], len(self.data), "stored_confirmed"))
            self.assertNotIn(self.key.hex(), "\n".join(db.iterdump()))
        with sqlite3.connect(self.root / "sender.db") as db:
            self.assertEqual(db.execute("SELECT state FROM swarm_scoped_transfers").fetchone()[0], "receiver_confirmed")
        self.assertFalse(list(self.root.glob(".swarm-*.tmp")))

    def test_wrong_scope_recipient_digest_size_expiry_and_key_are_denied(self):
        seed, port = self.start_seed()
        other_key = self.root / "other.key"
        other_key.write_bytes(secrets.token_bytes(32))
        other_key.chmod(0o600)
        variants = [{"scope": "ab" * 32}, {"recipient": "bc" * 32}, {"sha256": "cd" * 32},
                    {"size": len(self.data) + 1}, {"expires": int(self.options["expires"]) - 1},
                    {"key-file": other_key}]
        for change in variants:
            with self.subTest(change=next(iter(change))):
                rc, receipt, output, _ = self.fetch(port, **change)
                self.assertEqual((rc, receipt["state"]), (77, "not_admitted"))
                self.assertFalse(output.exists())
        self.assertEqual(self.fetch(port)[0], 0)
        self.assertEqual(self.finish_seed(seed)[0], 0)

    def test_nonce_replay_is_denied_and_fresh_request_still_succeeds(self):
        seed, port = self.start_seed()
        first, nonce = self.peer(port)
        old_request = self.request(nonce)
        first.close()
        second, fresh = self.peer(port)
        with second:
            self.assertNotEqual(fresh, nonce)
            send_frame(second, old_request)
            reply, _ = read_frame(second)
            self.assertEqual(reply[1], "denied")
        self.assertEqual(self.fetch(port)[0], 0)
        self.assertEqual(self.finish_seed(seed)[0], 0)

    def test_fragmented_control_frames_and_independent_proofs(self):
        seed, port = self.start_seed()
        sock, nonce = self.peer(port)
        with sock:
            send_frame(sock, self.request(nonce), fragmented=True)
            accepted, raw = read_frame(sock)
            self.assertEqual(accepted, [1, "accepted", bytes.fromhex(self.options["transfer-id"]), len(self.data), self.proof("accepted", nonce)])
            self.assertNotIn(self.key, raw)
            self.assertEqual(receive(sock, len(self.data)), self.data)
            send_frame(sock, [1, "stored", accepted[2], self.proof("stored", nonce)], fragmented=True)
            complete, _ = read_frame(sock)
            self.assertEqual(complete, [1, "complete", accepted[2], self.proof("complete", nonce)])
        self.assertEqual(self.finish_seed(seed)[0], 0)

    def test_unproved_storage_ack_cannot_confirm_a_transfer(self):
        seed, port = self.start_seed()
        sock, nonce = self.peer(port)
        with sock:
            send_frame(sock, self.request(nonce))
            accepted, _ = read_frame(sock)
            self.assertEqual(receive(sock, len(self.data)), self.data)
            # A request proof cannot be reflected into a storage proof.
            send_frame(sock, [1, "stored", accepted[2], self.proof("request", nonce)])
        rc, receipt = self.finish_seed(seed)
        self.assertEqual((rc, receipt["state"]), (75, "unconfirmed"))
        with sqlite3.connect(self.root / "sender.db") as db:
            self.assertEqual(db.execute("SELECT state FROM swarm_scoped_transfers").fetchone()[0], "unconfirmed")

    def test_oversized_truncated_and_trailing_frames_are_rejected(self):
        seed, port = self.start_seed()
        sock, _ = self.peer(port)
        with sock:
            sock.sendall(struct.pack(">I", 1025))
            sock.shutdown(socket.SHUT_WR)
            self.assertEqual(read_frame(sock)[0][1], "denied")
        sock, _ = self.peer(port)
        with sock:
            sock.sendall(struct.pack(">I", 100) + b"\x98\x01")
            sock.shutdown(socket.SHUT_WR)
            self.assertEqual(read_frame(sock)[0][1], "denied")
        sock, nonce = self.peer(port)
        with sock:
            raw = pack(self.request(nonce)) + b"\x01"
            sock.sendall(struct.pack(">I", len(raw)) + raw)
            self.assertEqual(read_frame(sock)[0][1], "denied")
        self.assertEqual(self.fetch(port)[0], 0)
        self.assertEqual(self.finish_seed(seed)[0], 0)

    def test_untrusted_server_cannot_supply_an_acceptance_proof(self):
        def behavior(sock):
            self.fake_accept(sock, proof_valid=False)
        port, thread, errors = self.fake_server(behavior)
        rc, receipt, output, _ = self.fetch(port)
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertFalse(errors)
        self.assertEqual((rc, receipt["state"]), (77, "not_admitted"))
        self.assertFalse(output.exists())

    def test_authenticated_tampered_bytes_fail_digest_before_publication(self):
        def behavior(sock):
            self.fake_accept(sock)
            sock.sendall(bytes([self.data[0] ^ 1]) + self.data[1:])
        port, thread, errors = self.fake_server(behavior)
        rc, receipt, output, _ = self.fetch(port)
        thread.join(5)
        self.assertFalse(errors)
        self.assertEqual((rc, receipt["state"]), (65, "artifact_mismatch"))
        self.assertFalse(output.exists())
        self.assertFalse(list(self.root.glob(".swarm-*.tmp")))

    def test_truncated_bytes_fail_without_destination_or_temporary_file(self):
        def behavior(sock):
            self.fake_accept(sock)
            sock.sendall(self.data[:12345])
        port, thread, errors = self.fake_server(behavior)
        rc, _, output, _ = self.fetch(port)
        thread.join(5)
        self.assertFalse(errors)
        self.assertEqual(rc, 75)
        self.assertFalse(output.exists())
        self.assertFalse(list(self.root.glob(".swarm-*.tmp")))

    def test_lost_confirmation_retains_stored_output_without_claiming_complete(self):
        def behavior(sock):
            nonce, transfer = self.fake_accept(sock)
            sock.sendall(self.data)
            stored, _ = read_frame(sock)
            self.assertEqual(stored, [1, "stored", transfer, self.proof("stored", nonce)])
        port, thread, errors = self.fake_server(behavior)
        rc, receipt, output, database = self.fetch(port)
        thread.join(5)
        self.assertFalse(errors)
        self.assertEqual((rc, receipt["state"]), (75, "stored_unconfirmed"))
        self.assertEqual(output.read_bytes(), self.data)
        with sqlite3.connect(database) as db:
            self.assertEqual(db.execute("SELECT state FROM swarm_scoped_transfers").fetchone()[0], "stored_unconfirmed")

    def test_source_is_verified_before_listening_and_snapshot_does_not_change(self):
        result, receipt = self.run_native(self.command("scoped-seed", artifact=self.source, port=0,
            index=self.root / "bad.db", sha256="00" * 32))
        self.assertEqual((result.returncode, receipt["state"]), (65, "artifact_mismatch"))
        self.assertNotIn(b"SWARM_READY", result.stderr)
        seed, port = self.start_seed()
        self.source.write_bytes(b"changed after owner-admitted snapshot")
        rc, _, output, _ = self.fetch(port)
        self.assertEqual(rc, 0)
        self.assertEqual(output.read_bytes(), self.data)
        self.assertEqual(self.finish_seed(seed)[0], 0)

    def test_empty_artifact_is_an_exact_valid_output(self):
        self.source.write_bytes(b"")
        self.options.update(size="0", sha256=hashlib.sha256(b"").hexdigest())
        seed, port = self.start_seed()
        rc, _, output, _ = self.fetch(port)
        self.assertEqual(rc, 0)
        self.assertEqual(output.read_bytes(), b"")
        self.assertEqual(self.finish_seed(seed)[0], 0)

    def test_existing_destination_and_symlink_are_never_overwritten(self):
        protected = self.root / "protected"
        protected.write_bytes(b"keep this")
        link = self.root / "link"
        link.symlink_to(protected)
        for out in [protected, link]:
            rc, _, _, _ = self.fetch(1, out=out)
            self.assertEqual(rc, 73)
            self.assertEqual(protected.read_bytes(), b"keep this")
        self.assertTrue(link.is_symlink())

    def test_unsafe_key_and_symlink_source_are_rejected(self):
        source_link = self.root / "source-link"
        source_link.symlink_to(self.source)
        process, _ = self.run_native(self.command("scoped-seed", artifact=source_link, port=0, index=self.root / "unsafe.db"))
        self.assertEqual(process.returncode, 65)
        self.key_file.chmod(0o644)
        self.assertEqual(self.fetch(1)[0], 77)
        self.key_file.chmod(0o600)
        key_link = self.root / "key-link"
        key_link.symlink_to(self.key_file)
        self.assertEqual(self.fetch(1, **{"key-file": key_link})[0], 77)
        key_hardlink = self.root / "key-hardlink"
        os.link(self.key_file, key_hardlink)
        self.assertEqual(self.fetch(1)[0], 77)

    def test_expired_grant_and_listener_expiry_do_not_transfer(self):
        self.assertEqual(self.fetch(1, expires=int(time.time()) - 1)[0], 78)
        self.options["expires"] = str(int(time.time()) + 2)
        seed, _ = self.start_seed()
        rc, receipt = self.finish_seed(seed)
        self.assertEqual((rc, receipt["state"]), (78, "expired_or_revoked"))

    def test_removing_key_revokes_waiting_listener(self):
        seed, _ = self.start_seed()
        self.key_file.unlink()
        rc, receipt = self.finish_seed(seed)
        self.assertEqual((rc, receipt["state"]), (78, "expired_or_revoked"))

    def test_transfer_id_cannot_be_reused_even_with_changed_scope(self):
        seed, port = self.start_seed()
        rc, _, _, database = self.fetch(port)
        self.assertEqual(rc, 0)
        self.assertEqual(self.finish_seed(seed)[0], 0)
        process, receipt = self.run_native(self.command("scoped-seed", artifact=self.source, port=0,
            index=self.root / "sender.db", scope="ef" * 32))
        self.assertEqual((process.returncode, receipt["state"]), (77, "grant_already_used"))
        self.assertNotIn(b"SWARM_READY", process.stderr)
        self.assertEqual(self.fetch(port, index=database)[0], 77)

    def test_argument_bounds_and_remote_host_are_rejected(self):
        variants = [{"port": -1}, {"port": 65536}, {"size": -1}, {"size": 67108865},
                    {"expires": int(time.time()) + 1000}, {"host": "example.invalid"},
                    {"scope": "invalid"}, {"transfer-id": "11" * 32}]
        for values in variants:
            with self.subTest(values=values):
                self.assertEqual(self.fetch(1, **values)[0], 64)
        command = self.command("scoped-fetch", port=1, out=self.root / "duplicate", index=self.root / "duplicate.db")
        process, _ = self.run_native(command + ["--scope", self.options["scope"]])
        self.assertEqual(process.returncode, 64)

    def test_legacy_gate_key_is_refused_without_disclosure(self):
        token = "test-secret-must-not-cross-wire"
        process = subprocess.run([self.binary, "seed", str(self.root), "--gate-key", token], capture_output=True, timeout=3)
        self.assertEqual(process.returncode, 77)
        self.assertNotIn(token.encode(), process.stdout + process.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
