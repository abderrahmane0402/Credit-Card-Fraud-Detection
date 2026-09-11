from src.dashboard.database import connection_config


def test_connection_config_uses_expected_keys(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_DB", "fraud_db")
    monkeypatch.setenv("POSTGRES_USER", "fraud_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test-password")
    config = connection_config()
    assert config["host"] == "localhost"
    assert config["port"] == 5433
    assert config["dbname"] == "fraud_db"
    assert config["user"] == "fraud_user"
    assert config["password"] == "test-password"
