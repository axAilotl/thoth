FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg poppler-utils \
    && rm -rf /var/lib/apt/lists/*
COPY docker/requirements.lock /app/docker/requirements.lock
RUN pip install --no-cache-dir --require-hashes -r docker/requirements.lock

COPY core/ core/
COPY collectors/ collectors/
COPY processors/ processors/
COPY ccf/ ccf/
COPY spec/ spec/
COPY prompts/ prompts/
COPY static/ static/
COPY scripts/ scripts/
COPY thoth.py thoth_api.py thoth_keeper.py thoth_mcp.py keeper_profile.py ./
COPY config.example.json config.schema.json archivist_topics.example.yaml ./
COPY docker/container.py docker/config.container.example.json docker/
RUN ln -s /runtime/config/config.json config.json \
    && ln -s /runtime/config/control.json control.json \
    && ln -s /runtime/config/.env .env \
    && ln -s /runtime/config/archivist_topics.yaml archivist_topics.yaml

USER 1000:1000
EXPOSE 8001
ENTRYPOINT ["python", "docker/container.py"]
CMD ["python", "-m", "uvicorn", "thoth_api:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "1"]
