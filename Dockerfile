FROM python:3.12-slim

WORKDIR /app

# Устанавливаем poetry
RUN pip install poetry

# Копируем конфиги poetry
COPY pyproject.toml /app/pyproject.toml
COPY poetry.lock /app/poetry.lock

# Устанавливаем зависимости через poetry
RUN poetry config virtualenvs.create false \
    && poetry install --no-root --no-interaction --no-ansi

# Копируем общие модули и код продюсера
COPY lab3/ /app

# Запускаем продюсер как модуль
CMD ["faust", "-A", "main", "worker", "-l", "info"]


