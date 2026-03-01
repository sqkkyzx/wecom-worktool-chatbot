FROM python:3.13-slim

RUN pip install --no-cache-dir --upgrade pip uv

ENV FASTAPI_HOME=/opt/fastapi-app/
WORKDIR $FASTAPI_HOME

COPY pyproject.toml uv.lock* ./

RUN uv export --frozen --no-dev --output-file --no-hashes requirements.txt && \
    uv pip install --system --no-cache-dir -r requirements.txt

COPY main.py ./
CMD ["python", "main.py"]