import os
import secrets
from pathlib import Path


if not Path(".env").exists():
    values = {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "unit_db",
        "POSTGRES_USER": "unit_user",
        "POSTGRES_PASSWORD": secrets.token_urlsafe(16),
        "RABBITMQ_HOST": "localhost",
        "RABBITMQ_PORT": "5672",
        "RABBITMQ_USER": "unit_user",
        "RABBITMQ_PASSWORD": secrets.token_urlsafe(16),
        "API_KEY": secrets.token_urlsafe(16),
    }
    for key, value in values.items():
        os.environ.setdefault(key, value)
