#!/bin/bash
# Quick test script to run LLM smoke test in Docker with .env loaded

set -e

echo "Building telegram-bot image..."
docker-compose build telegram-bot

echo ""
echo "Running LLM smoke test..."
docker-compose run --rm telegram-bot python test_llm_standalone.py

echo ""
echo "Test complete!"
