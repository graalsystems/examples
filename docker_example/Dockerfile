# A self-contained job image: graal runs it as is, with your command.
# Requirements for a graal run: a /bin/sh, and nothing that needs root or a writable root filesystem —
# graal imposes UID 10001, drops every capability and mounts the root filesystem read-only;
# /tmp and /workspace are writable.
FROM python:3.12-slim

RUN pip install --no-cache-dir pandas==2.2.3 \
    && useradd --uid 10001 --create-home --home-dir /home/app app

COPY report.py /app/report.py
ENV HOME=/tmp PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 10001:10001
WORKDIR /app
CMD ["python", "/app/report.py"]
