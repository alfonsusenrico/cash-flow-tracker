FROM python:3.12-slim
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src ./src
COPY prompts ./prompts
COPY migrations ./migrations
COPY main.py .
COPY migrate.py .

# Run migrations on startup and then start the bot
CMD ["sh", "-c", "python migrate.py && python main.py"]
