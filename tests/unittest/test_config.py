from urllib.parse import unquote, urlsplit

import pytest
from pydantic import ValidationError
from sqlalchemy.engine import make_url

from app.config import Settings


def values(**changes):
    return {
        "postgres_host": "db.internal",
        "postgres_port": 5432,
        "postgres_db": "orders",
        "postgres_user": "service",
        "postgres_password": "p@ss:/word",
        "rabbitmq_host": "broker.internal",
        "rabbitmq_port": 5672,
        "rabbitmq_user": "service",
        "rabbitmq_password": "p@ss:/word",
        "api_key": "local-test-key",
    } | changes


def test_connection_urls_escape_credentials_and_hide_secrets_in_repr():
    settings = Settings(_env_file=None, **values())
    database = make_url(settings.database_url)
    rabbit = urlsplit(settings.rabbitmq_url)
    assert database.host == "db.internal"
    assert database.password == "p@ss:/word"
    assert unquote(rabbit.password) == "p@ss:/word"
    assert "p@ss:/word" not in repr(settings)


@pytest.mark.parametrize("changes", [{"postgres_password": ""}, {"api_key": ""}, {"postgres_port": 0}])
def test_invalid_or_empty_connection_settings_fail_fast(changes):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **values(**changes))
