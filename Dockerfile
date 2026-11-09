FROM python:3.12-slim

WORKDIR /service
COPY pyproject.toml ./
COPY app ./app
COPY alembic.ini ./
COPY migrations ./migrations
RUN pip install --no-cache-dir .

CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
