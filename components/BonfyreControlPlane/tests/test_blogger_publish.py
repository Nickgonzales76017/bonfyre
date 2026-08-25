import importlib.util
import json
from pathlib import Path


def _module():
    script = Path(__file__).resolve().parents[3] / "bin" / "blogger_publish.py"
    spec = importlib.util.spec_from_file_location("blogger_publish", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_installed_client_uses_registered_loopback_redirect(tmp_path):
    module = _module()
    module.REFS = tmp_path / "missing.env"
    module.CREDS = tmp_path / "credentials.json"
    module.CREDS.write_text(
        json.dumps(
            {
                "installed": {
                    "client_id": "client-id",
                    "client_secret": "client-secret",
                    "redirect_uris": ["http://localhost:8765/callback"],
                }
            }
        )
    )
    assert module.client_config() == (
        "client-id",
        "client-secret",
        "http://localhost:8765/callback",
    )


def test_web_client_without_redirect_is_rejected_before_browser_flow(tmp_path):
    module = _module()
    module.REFS = tmp_path / "missing.env"
    module.CREDS = tmp_path / "credentials.json"
    module.CREDS.write_text(
        json.dumps({"web": {"client_id": "client-id", "client_secret": "secret"}})
    )
    try:
        module.client_config()
    except SystemExit as exc:
        assert "no registered redirect URI" in str(exc)
    else:
        raise AssertionError("an unusable web OAuth client must fail preflight")


def test_reference_and_credentials_client_ids_must_match(tmp_path):
    module = _module()
    module.REFS = tmp_path / "refs.env"
    module.REFS.write_text(
        "BLOGGER_CLIENT_ID=reference-id\nBLOGGER_CLIENT_SECRET=reference-secret\n"
    )
    module.CREDS = tmp_path / "credentials.json"
    module.CREDS.write_text(
        json.dumps(
            {
                "installed": {
                    "client_id": "configured-id",
                    "client_secret": "configured-secret",
                }
            }
        )
    )
    try:
        module.client_config()
    except SystemExit as exc:
        assert "does not match credentials.json" in str(exc)
    else:
        raise AssertionError("unrelated OAuth client material must not be combined")


def test_exported_reference_and_credentials_client_ids_must_match(tmp_path):
    module = _module()
    module.REFS = tmp_path / "refs.env"
    module.REFS.write_text(
        "export BLOGGER_CLIENT_ID=reference-id\n"
        "export BLOGGER_CLIENT_SECRET=reference-secret\n"
    )
    module.CREDS = tmp_path / "credentials.json"
    module.CREDS.write_text(
        json.dumps(
            {
                "installed": {
                    "client_id": "configured-id",
                    "client_secret": "configured-secret",
                }
            }
        )
    )
    try:
        module.client_config()
    except SystemExit as exc:
        assert "does not match credentials.json" in str(exc)
    else:
        raise AssertionError("exported references must retain the coherence fence")


def test_loopback_endpoint_requires_explicit_local_port():
    module = _module()
    assert module.loopback_endpoint("http://localhost:8765/callback") == (
        "127.0.0.1",
        8765,
        "/callback",
    )
    for redirect in (
        "http://localhost",
        "https://localhost:8765/callback",
        "http://example.com:8765/callback",
    ):
        try:
            module.loopback_endpoint(redirect)
        except SystemExit as exc:
            assert "explicit loopback redirect" in str(exc)
        else:
            raise AssertionError(f"unsafe redirect should fail: {redirect}")
