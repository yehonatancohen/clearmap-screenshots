FROM python:3.12-slim

ENV TZ=Asia/Jerusalem
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browser + system deps for headless Chromium
RUN playwright install --with-deps chromium

# Install dumb-init to properly reap zombie Chromium processes
RUN apt-get update && apt-get install -y dumb-init && rm -rf /var/lib/apt/lists/*

# Copy application code
COPY main.py screenshot_overlay.py polygons.json ./

# Copy logo assets + font for overlays
COPY public/ ./public/

# Copy district metadata for regional grouping in captions
COPY district_to_areas.py ./

# serviceAccountKey.json is mounted at runtime (not baked into image)

# Declare public directory as a volume for persistent logos and fonts
VOLUME ["/app/public"]

ENV PYTHONUNBUFFERED=1

HEALTHCHECK --interval=60s --timeout=10s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["python", "main.py"]
