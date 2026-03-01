FROM python:3.14-slim

RUN pip install --no-cache-dir --upgrade pip uv

ENV FASTAPI_HOME=/opt/fastapi-app/
WORKDIR $FASTAPI_HOME

COPY pyproject.toml uv.lock* ./

RUN uv sync --no-dev --system

COPY main.py ./
CMD ["python", "main.py"]