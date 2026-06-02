#!/usr/bin/env python3
"""Entry point for the Telegram bot."""
import asyncio
import sys
from pathlib import Path

# Add src to path so we can import bot modules
sys.path.insert(0, str(Path(__file__).parent / "src"))

from bot.app import main

if __name__ == "__main__":
    asyncio.run(main())
