FROM python:3.12-slim
WORKDIR /app

# Install curl for Docker healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src ./src
COPY prompts ./prompts
COPY migrations ./migrations
COPY main.py .
COPY migrate.py .

# Health endpoint on port 8082
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -f http://localhost:8082/healthz || exit 1

# Run migrations on startup and then start the bot
CMD ["sh", "-c", "python migrate.py && python main.py"]
