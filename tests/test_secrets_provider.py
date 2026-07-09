"""Unit tests for secrets_provider.py -- pure env/file indirection, no
network involved.
"""
import pytest

from secrets_provider import get_secret, require_secret


def test_get_secret_falls_back_to_plain_env_var(monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "plain-value")
    assert get_secret("MY_TOKEN") == "plain-value"


def test_get_secret_prefers_file_indirection(tmp_path, monkeypatch):
    secret_file = tmp_path / "token.txt"
    secret_file.write_text("file-value\n", encoding="utf-8")
    monkeypatch.setenv("MY_TOKEN_FILE", str(secret_file))
    monkeypatch.setenv("MY_TOKEN", "plain-value")

    assert get_secret("MY_TOKEN") == "file-value"


def test_get_secret_returns_default_when_missing():
    assert get_secret("DOES_NOT_EXIST_TOKEN", default="fallback") == "fallback"


def test_get_secret_returns_empty_string_by_default_when_missing():
    assert get_secret("DOES_NOT_EXIST_TOKEN") == ""


def test_require_secret_raises_key_error_when_missing():
    with pytest.raises(KeyError):
        require_secret("DOES_NOT_EXIST_TOKEN")


def test_require_secret_returns_value_when_present(monkeypatch):
    monkeypatch.setenv("MY_TOKEN", "plain-value")
    assert require_secret("MY_TOKEN") == "plain-value"
