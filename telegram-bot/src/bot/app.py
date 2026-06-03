"""Main Telegram bot application with webhook handlers."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx
from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from .config import Settings, load_settings
from .crypto import Crypto
from .db import close_pool, init_pool, open_pool
from .finance_client import FinanceClient, FinanceError
from .llm import LLMPlanner
from .store import Store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class TypingContext:
    """Async context manager to keep showing 'typing' status in Telegram during long operations."""
    def __init__(self, bot: Any, chat_id: int | str) -> None:
        self.bot = bot
        self.chat_id = chat_id
        self._task: asyncio.Task | None = None

    async def __aenter__(self) -> TypingContext:
        async def send_typing_loop() -> None:
            while True:
                try:
                    await self.bot.send_chat_action(chat_id=self.chat_id, action="typing")
                except Exception:
                    pass
                await asyncio.sleep(4.5)

        self._task = asyncio.create_task(send_typing_loop())
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass


class BotApp:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.crypto = Crypto(settings.bot_secret)
        self.store = Store(self.crypto)
        self.http = httpx.AsyncClient(timeout=30.0)
        self.finance = FinanceClient(settings.finance_api_base_url, self.http)
        self.llm = LLMPlanner(
            settings.deepseek_api_key,
            settings.deepseek_base_url,
            settings.deepseek_model,
        )
        # Create user preferences directory on initialization
        import os
        os.makedirs("/app/storage/user_preferences", exist_ok=True)

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /start command."""
        await update.message.reply_text(
            "👋 Halo! Saya bot keuangan Anda.\n\n"
            "Gunakan /link <API_KEY> untuk menghubungkan akun Anda.\n"
            "Setelah terhubung, kirim pesan transaksi atau foto struk untuk mencatat otomatis."
        )

    async def link_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /link <api_key> command."""
        if not context.args or len(context.args) != 1:
            await update.message.reply_text(
                "❌ Format: /link <API_KEY>\n\n"
                "Dapatkan API key dari aplikasi web Anda."
            )
            return

        api_key = context.args[0].strip()
        telegram_user_id = update.effective_user.id

        try:
            # Validate API key by fetching user info
            info = await self.finance.api_key_info(api_key)
            username = info.get("username", "")
            
            await self.store.link_user(telegram_user_id, api_key, username)
            await update.message.reply_text(
                f"✅ Akun terhubung!\n"
                f"User: {username}\n\n"
                f"Sekarang Anda bisa kirim pesan transaksi atau foto struk."
            )
            logger.info(f"User {telegram_user_id} linked as {username}")
        except FinanceError as e:
            await update.message.reply_text(
                f"❌ API key tidak valid: {e.detail}"
            )
        except Exception as e:
            logger.exception("Link failed")
            await update.message.reply_text(
                f"❌ Gagal menghubungkan: {str(e)}"
            )

    async def unlink_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /unlink command."""
        telegram_user_id = update.effective_user.id
        deleted = await self.store.unlink_user(telegram_user_id)
        if deleted:
            await update.message.reply_text("✅ Akun berhasil diputus.")
            logger.info(f"User {telegram_user_id} unlinked")
        else:
            await update.message.reply_text("❌ Tidak ada akun yang terhubung.")

    async def clear_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /clear command."""
        telegram_user_id = update.effective_user.id
        await self.store.clear_chat_history(telegram_user_id)
        await update.message.reply_text("🧹 Riwayat percakapan Anda telah dibersihkan. Silakan mulai percakapan baru!")
        logger.info(f"User {telegram_user_id} cleared their chat history")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle text/photo messages."""
        telegram_user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        # Check if user is linked
        api_key = await self.store.get_api_key(telegram_user_id)
        if not api_key:
            await update.message.reply_text(
                "❌ Akun belum terhubung. Gunakan /link <API_KEY> terlebih dahulu."
            )
            return

        # Extract message text and image
        message_text = update.message.text or update.message.caption or ""
        image_bytes: bytes | None = None
        image_mime = "image/jpeg"

        if update.message.photo:
            # Get highest resolution photo
            photo = update.message.photo[-1]
            file = await context.bot.get_file(photo.file_id)
            image_bytes = await file.download_as_bytearray()
            image_mime = "image/jpeg"

        try:
            # Load chronological chat history from store (limit to last 10 messages)
            history = await self.store.get_chat_history(telegram_user_id, limit=10)

            # Load user preferences if they exist
            user_prefs = None
            import os
            prefs_path = f"/app/storage/user_preferences/user_{telegram_user_id}.md"
            if os.path.exists(prefs_path):
                try:
                    with open(prefs_path, "r", encoding="utf-8") as f:
                        user_prefs = f.read()
                except Exception as e:
                    logger.warning(f"Failed to read user preferences: {e}")

            async with TypingContext(context.bot, chat_id):
                # Fetch context in parallel
                accounts, categories = await asyncio.gather(
                    self.finance.list_accounts(api_key),
                    self.finance.list_categories(api_key),
                )

                # Get timezone from user (default to Asia/Jakarta)
                tz = "Asia/Jakarta"
                now_iso = datetime.now(timezone.utc).isoformat()

                uploaded_receipt = False

                # ----------------------------------------------------------
                # Tool executors (closured with user's api_key, accounts, and categories)
                # ----------------------------------------------------------
                async def _tool_get_account_balance(account_name: str) -> str:
                    """Return the current balance for the named account as JSON."""
                    fresh = await self.finance.list_accounts(api_key)
                    match = next(
                        (
                            a for a in fresh
                            if (a.get("account_name") or "").lower() == account_name.lower()
                        ),
                        None,
                    )
                    if match:
                        return json.dumps(
                            {
                                "account_name": match["account_name"],
                                "balance": match.get("balance"),
                                "profile_type": match.get("profile_type"),
                            },
                            ensure_ascii=False,
                        )
                    return json.dumps(
                        {"error": f"Account '{account_name}' not found"},
                        ensure_ascii=False,
                    )

                async def _tool_get_all_balances() -> str:
                    """Return all account balances as JSON."""
                    fresh = await self.finance.list_accounts(api_key)
                    return json.dumps(
                        {
                            "accounts": [
                                {
                                    "account_name": a["account_name"],
                                    "balance": a.get("balance"),
                                    "profile_type": a.get("profile_type"),
                                }
                                for a in fresh
                            ]
                        },
                        ensure_ascii=False,
                    )

                async def _tool_search_transactions(
                    query: str | None = None,
                    account_name: str | None = None,
                    category_name: str | None = None,
                    time_range: str | None = None,
                    limit: int = 50,
                ) -> str:
                    """Search transactions with query keywords, time ranges, and account/category filters."""
                    from_dt = None
                    to_dt = datetime.now(timezone.utc)
                    
                    if time_range == "today":
                        from_dt = to_dt.replace(hour=0, minute=0, second=0, microsecond=0)
                    elif time_range == "yesterday":
                        yesterday = to_dt - timedelta(days=1)
                        from_dt = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
                        to_dt = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
                    elif time_range == "this_week":
                        from_dt = (to_dt - timedelta(days=to_dt.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
                    elif time_range == "last_week":
                        start_of_this_week = to_dt - timedelta(days=to_dt.weekday())
                        from_dt = (start_of_this_week - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
                        to_dt = (start_of_this_week - timedelta(microseconds=1))
                    elif time_range == "this_month":
                        from_dt = to_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    elif time_range == "last_month":
                        first_of_this_month = to_dt.replace(day=1)
                        last_of_last_month = first_of_this_month - timedelta(days=1)
                        from_dt = last_of_last_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                        to_dt = last_of_last_month.replace(hour=23, minute=59, second=59, microsecond=999999)
                    elif time_range == "this_year":
                        from_dt = to_dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                    elif time_range and "_" in time_range and time_range.split("_")[0].isdigit():
                        parts = time_range.split("_")
                        days = int(parts[0])
                        from_dt = to_dt - timedelta(days=days)
                    else:
                        # default: 30 days
                        from_dt = to_dt - timedelta(days=30)
                        
                    payload: dict[str, Any] = {
                        "scope": "all",
                        "limit": 100,
                        "order": "desc",
                    }
                    if query:
                        payload["query"] = query
                    if from_dt:
                        payload["from_date"] = from_dt.strftime("%Y-%m-%d")
                    if to_dt:
                        payload["to_date"] = to_dt.strftime("%Y-%m-%d")
                        
                    result = await self.finance.search_ledger(api_key, payload)
                    rows = result.get("rows", [])
                    
                    if account_name:
                        rows = [
                            r for r in rows
                            if account_name.lower() in (r.get("account_name") or "").lower()
                        ]
                    if category_name:
                        rows = [
                            r for r in rows
                            if category_name.lower() in (r.get("category_name") or "").lower()
                        ]
                        
                    rows = rows[:limit]
                    return json.dumps(
                        {
                            "transactions": [
                                {
                                    "transaction_id": r.get("transaction_id"),
                                    "transaction_name": r.get("transaction_name"),
                                    "account_name": r.get("account_name"),
                                    "category_name": r.get("category_name"),
                                    "date": r.get("date"),
                                    "debit": r.get("debit"),
                                    "credit": r.get("credit"),
                                }
                                for r in rows
                            ]
                        },
                        ensure_ascii=False,
                    )

                async def _tool_record_transaction(
                    type: str,
                    amount: int,
                    name: str,
                    account_name: str,
                    category_name: str | None = None,
                    date: str | None = None,
                ) -> str:
                    nonlocal image_bytes, uploaded_receipt
                    fresh_accounts, fresh_categories = await asyncio.gather(
                        self.finance.list_accounts(api_key),
                        self.finance.list_categories(api_key),
                    )
                    
                    account = next(
                        (a for a in fresh_accounts if (a.get("account_name") or "").lower() == account_name.lower()),
                        None
                    )
                    if not account:
                        return json.dumps({
                            "error": f"Account '{account_name}' not found. Valid accounts: {[a['account_name'] for a in fresh_accounts]}"
                        })
                    
                    category_id = None
                    if category_name:
                        category = next(
                            (c for c in fresh_categories if (c.get("name") or "").lower() == category_name.lower()),
                            None
                        )
                        if category:
                            category_id = category["category_id"]
                        else:
                            return json.dumps({
                                "error": f"Category '{category_name}' not found. Valid categories: {[c['name'] for c in fresh_categories]}"
                            })
                    
                    payload = {
                        "transaction_type": "debit" if type == "income" else "credit",
                        "amount": amount,
                        "transaction_name": name,
                        "account_id": account["account_id"],
                        "category_id": category_id,
                        "is_cycle_topup": False,
                    }
                    if date:
                        payload["date"] = date
                        
                    res = await self.finance.upsert_transaction(api_key, payload)
                    tx_id = res.get("transaction_id")
                    
                    msg_suffix = ""
                    if image_bytes and not uploaded_receipt and tx_id:
                        try:
                            await self.finance.upload_receipt(
                                api_key, tx_id, image_bytes, "receipt.jpg", "image/jpeg"
                            )
                            uploaded_receipt = True
                            msg_suffix = " with receipt image uploaded"
                        except Exception as e:
                            logger.exception("Failed to upload receipt")
                            msg_suffix = f" (failed to upload receipt: {e})"
                            
                    return json.dumps({
                        "success": True,
                        "transaction_id": tx_id,
                        "message": f"Successfully recorded {type} of Rp{amount:,} for '{name}' in account '{account['account_name']}'{msg_suffix}."
                    })

                async def _tool_record_movement(
                    amount: int,
                    source_account_name: str,
                    target_account_name: str,
                    date: str | None = None,
                ) -> str:
                    fresh_accounts = await self.finance.list_accounts(api_key)
                    src_acc = next(
                        (a for a in fresh_accounts if (a.get("account_name") or "").lower() == source_account_name.lower()),
                        None
                    )
                    if not src_acc:
                        return json.dumps({
                            "error": f"Source account '{source_account_name}' not found. Valid accounts: {[a['account_name'] for a in fresh_accounts]}"
                        })
                        
                    tgt_acc = next(
                        (a for a in fresh_accounts if (a.get("account_name") or "").lower() == target_account_name.lower()),
                        None
                    )
                    if not tgt_acc:
                        return json.dumps({
                            "error": f"Target account '{target_account_name}' not found. Valid accounts: {[a['account_name'] for a in fresh_accounts]}"
                        })
                        
                    payload = {
                        "amount": amount,
                        "from_account_id": src_acc["account_id"],
                        "to_account_id": tgt_acc["account_id"],
                    }
                    if date:
                        payload["date"] = date
                        
                    res = await self.finance.create_movement(api_key, payload)
                    transfer_id = res.get("transfer_id")
                    
                    return json.dumps({
                        "success": True,
                        "transfer_id": transfer_id,
                        "message": f"Successfully transferred Rp{amount:,} from '{src_acc['account_name']}' to '{tgt_acc['account_name']}'."
                    })

                async def _tool_delete_transaction(transaction_id: str) -> str:
                    await self.finance.delete_transaction(api_key, transaction_id)
                    return json.dumps({
                        "success": True,
                        "message": f"Successfully deleted transaction {transaction_id}."
                    })

                async def _tool_update_transaction(
                    transaction_id: str,
                    type: str | None = None,
                    amount: int | None = None,
                    name: str | None = None,
                    account_name: str | None = None,
                    category_name: str | None = None,
                    date: str | None = None,
                ) -> str:
                    fresh_accounts, fresh_categories = await asyncio.gather(
                        self.finance.list_accounts(api_key),
                        self.finance.list_categories(api_key),
                    )
                    
                    payload: dict[str, Any] = {
                        "transaction_id": transaction_id,
                    }
                    
                    if type:
                        payload["transaction_type"] = "debit" if type == "income" else "credit"
                    if amount is not None:
                        payload["amount"] = amount
                    if name is not None:
                        payload["transaction_name"] = name
                    if account_name:
                        account = next(
                            (a for a in fresh_accounts if (a.get("account_name") or "").lower() == account_name.lower()),
                            None
                        )
                        if not account:
                            return json.dumps({
                                "error": f"Account '{account_name}' not found. Valid accounts: {[a['account_name'] for a in fresh_accounts]}"
                            })
                        payload["account_id"] = account["account_id"]
                    if category_name:
                        category = next(
                            (c for c in fresh_categories if (c.get("name") or "").lower() == category_name.lower()),
                            None
                        )
                        if not category:
                            return json.dumps({
                                "error": f"Category '{category_name}' not found. Valid categories: {[c['name'] for c in fresh_categories]}"
                            })
                        payload["category_id"] = category["category_id"]
                    if date:
                        payload["date"] = date
                        
                    res = await self.finance.upsert_transaction(api_key, payload)
                    return json.dumps({
                        "success": True,
                        "message": f"Successfully updated transaction {transaction_id}.",
                        "result": res
                    })

                async def _tool_update_user_preferences(preferences_content: str) -> str:
                    import os
                    prefs_dir = "/app/storage/user_preferences"
                    os.makedirs(prefs_dir, exist_ok=True)
                    prefs_path = f"{prefs_dir}/user_{telegram_user_id}.md"
                    try:
                        with open(prefs_path, "w", encoding="utf-8") as f:
                            f.write(preferences_content)
                        return json.dumps({
                            "success": True,
                            "message": "User preferences updated successfully."
                        })
                    except Exception as e:
                        logger.exception("Failed to write user preferences")
                        return json.dumps({
                            "error": f"Failed to save preferences: {str(e)}"
                        })

                tool_executors = {
                    "get_account_balance": _tool_get_account_balance,
                    "get_all_balances": _tool_get_all_balances,
                    "search_transactions": _tool_search_transactions,
                    "record_transaction": _tool_record_transaction,
                    "record_movement": _tool_record_movement,
                    "delete_transaction": _tool_delete_transaction,
                    "update_transaction": _tool_update_transaction,
                    "update_user_preferences": _tool_update_user_preferences,
                }

                # Call LLM to propose action (agentic loop)
                response_text = await self.llm.propose(
                    message_text=message_text,
                    now_iso=now_iso,
                    timezone=tz,
                    accounts=accounts,
                    categories=categories,
                    image_bytes=image_bytes,
                    image_mime=image_mime,
                    history=history,
                    timeout=self.settings.llm_timeout,
                    tool_executors=tool_executors,
                    user_preferences=user_prefs,
                )

            # Save the user message and assistant natural text response to chat history
            await self.store.add_chat_history(telegram_user_id, "user", message_text or "[Photo receipt]")
            await self.store.add_chat_history(telegram_user_id, "assistant", response_text)

            # Clean up double asterisks to single asterisks just in case
            formatted_text = response_text.replace("**", "*")
            
            # Reply to user with Markdown parsing and robust fallback
            try:
                await update.message.reply_text(formatted_text, parse_mode="Markdown")
            except Exception as e:
                logger.warning(f"Failed to send message with Markdown formatting: {e}")
                # Fallback to plain text response
                await update.message.reply_text(response_text)

        except FinanceError as e:
            await update.message.reply_text(f"❌ API error: {e.detail}")
        except Exception as e:
            logger.exception("Message handling failed")
            await update.message.reply_text(f"❌ Terjadi kesalahan: {str(e)}")

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle callback queries (confirmations) gracefully for legacy buttons."""
        query = update.callback_query
        await query.answer()
        await query.edit_message_text("❌ Silakan gunakan chat langsung untuk berkomunikasi.")


async def main() -> None:
    """Main entry point."""
    settings = load_settings()
    
    # Initialize database pool
    init_pool(settings.bot_database_url)
    await open_pool()
    
    try:
        # Create bot application
        app_instance = BotApp(settings)
        
        application = (
            Application.builder()
            .token(settings.telegram_bot_token)
            .build()
        )

        # Register handlers
        application.add_handler(CommandHandler("start", app_instance.start_command))
        application.add_handler(CommandHandler("link", app_instance.link_command))
        application.add_handler(CommandHandler("unlink", app_instance.unlink_command))
        application.add_handler(CommandHandler("clear", app_instance.clear_command))
        application.add_handler(
            MessageHandler(filters.TEXT | filters.PHOTO, app_instance.handle_message)
        )
        application.add_handler(CallbackQueryHandler(app_instance.handle_callback))

        # Start webhook with fallback to polling
        if settings.telegram_webhook_url:
            try:
                logger.info(f"Starting webhook on {settings.webhook_listen}:{settings.webhook_port}")
                await application.initialize()
                await application.start()
                
                webhook_url = f"{settings.telegram_webhook_url.rstrip('/')}/telegram/webhook"
                
                await application.updater.start_webhook(
                    listen=settings.webhook_listen,
                    port=settings.webhook_port,
                    url_path="/telegram/webhook",
                    secret_token=settings.telegram_webhook_secret,
                    webhook_url=webhook_url,
                )
                logger.info("Bot is running with webhook!")
            except Exception as e:
                logger.warning(f"Failed to start webhook, falling back to polling: {e}")
                try:
                    await application.stop()
                    await application.shutdown()
                except Exception:
                    pass
                
                # Re-create application for polling
                application = (
                    Application.builder()
                    .token(settings.telegram_bot_token)
                    .build()
                )
                application.add_handler(CommandHandler("start", app_instance.start_command))
                application.add_handler(CommandHandler("link", app_instance.link_command))
                application.add_handler(CommandHandler("unlink", app_instance.unlink_command))
                application.add_handler(CommandHandler("clear", app_instance.clear_command))
                application.add_handler(
                    MessageHandler(filters.TEXT | filters.PHOTO, app_instance.handle_message)
                )
                application.add_handler(CallbackQueryHandler(app_instance.handle_callback))
                
                logger.info("Starting polling...")
                await application.initialize()
                await application.start()
                await application.updater.start_polling()
                logger.info("Bot is running with polling!")
        else:
            logger.info("Starting polling...")
            await application.initialize()
            await application.start()
            await application.updater.start_polling()
            logger.info("Bot is running with polling!")
            
        # Keep running
        await asyncio.Event().wait()
        
    finally:
        await close_pool()
        await app_instance.http.aclose()


if __name__ == "__main__":
    asyncio.run(main())
