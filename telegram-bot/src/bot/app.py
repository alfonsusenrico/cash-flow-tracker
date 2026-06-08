"""Main Telegram bot application with webhook handlers."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx
from aiohttp import web
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
            vision_model=settings.vision_model,
            vision_base_url=settings.vision_base_url,
            use_two_step_vision=settings.use_two_step_vision,
        )
        # Create user preferences directory on initialization
        import os
        try:
            os.makedirs("/app/storage/user_preferences", exist_ok=True)
        except OSError:
            try:
                os.makedirs("./storage/user_preferences", exist_ok=True)
            except OSError:
                pass

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
            history_saved = False
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
                                api_key, tx_id, bytes(image_bytes), "receipt.jpg", "image/jpeg"
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
                        "source_account_id": src_acc["account_id"],
                        "target_account_id": tgt_acc["account_id"],
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

                async def _tool_create_account(account_name: str, initial_balance: int = 0) -> str:
                    try:
                        res = await self.finance.create_account(api_key, {
                            "account_name": account_name,
                            "initial_balance": initial_balance,
                        })
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_update_account_name(account_name: str, new_account_name: str) -> str:
                    try:
                        fresh_accounts = await self.finance.list_accounts(api_key)
                        acc = next((a for a in fresh_accounts if (a.get("account_name") or "").lower() == account_name.lower()), None)
                        if not acc: return json.dumps({"error": f"Account '{account_name}' not found."})
                        res = await self.finance.update_account(api_key, acc["account_id"], {"account_name": new_account_name})
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_update_account_profile(account_name: str, **kwargs) -> str:
                    try:
                        fresh_accounts = await self.finance.list_accounts(api_key)
                        acc = next((a for a in fresh_accounts if (a.get("account_name") or "").lower() == account_name.lower()), None)
                        if not acc: return json.dumps({"error": f"Account '{account_name}' not found."})
                        res = await self.finance.update_account_profile(api_key, acc["account_id"], kwargs)
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_delete_account(account_name: str) -> str:
                    try:
                        fresh_accounts = await self.finance.list_accounts(api_key)
                        acc = next((a for a in fresh_accounts if (a.get("account_name") or "").lower() == account_name.lower()), None)
                        if not acc: return json.dumps({"error": f"Account '{account_name}' not found."})
                        res = await self.finance.delete_account(api_key, acc["account_id"])
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_update_movement(transfer_id: str, amount: int | None = None, source_account_name: str | None = None, target_account_name: str | None = None, date: str | None = None) -> str:
                    try:
                        payload = {}
                        if amount is not None: payload["amount"] = amount
                        if date: payload["date"] = date
                        
                        if source_account_name or target_account_name:
                            fresh_accounts = await self.finance.list_accounts(api_key)
                            if source_account_name:
                                src = next((a for a in fresh_accounts if (a.get("account_name") or "").lower() == source_account_name.lower()), None)
                                if not src: return json.dumps({"error": f"Source '{source_account_name}' not found."})
                                payload["source_account_id"] = src["account_id"]
                            if target_account_name:
                                tgt = next((a for a in fresh_accounts if (a.get("account_name") or "").lower() == target_account_name.lower()), None)
                                if not tgt: return json.dumps({"error": f"Target '{target_account_name}' not found."})
                                payload["target_account_id"] = tgt["account_id"]
                                
                        res = await self.finance.update_movement(api_key, transfer_id, payload)
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_delete_movement(transfer_id: str) -> str:
                    try:
                        res = await self.finance.delete_movement(api_key, transfer_id)
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_audit_transactions(transaction_id: str | None = None, limit: int = 50) -> str:
                    try:
                        payload = {"limit": limit}
                        if transaction_id: payload["transaction_id"] = transaction_id
                        res = await self.finance.audit_transactions(api_key, payload)
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_get_summary(month: int, year: int) -> str:
                    try:
                        res = await self.finance.get_summary(api_key, {"month": month, "year": year})
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_get_analysis(month: int, year: int) -> str:
                    try:
                        res = await self.finance.get_analysis(api_key, {"month": month, "year": year})
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_get_budget_shift(month: int, year: int, mode: str = "normal") -> str:
                    try:
                        res = await self.finance.get_budget_shift(api_key, {"month": month, "year": year, "mode": mode})
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_list_goals() -> str:
                    try:
                        res = await self.finance.list_goals(api_key)
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_create_goal(name: str, target_amount: int, target_date: str | None = None, notes: str | None = None) -> str:
                    try:
                        payload = {
                            "name": name,
                            "target_amount": target_amount,
                        }
                        if target_date: payload["target_date"] = target_date
                        if notes: payload["notes"] = notes
                        res = await self.finance.create_goal(api_key, payload)
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_update_goal(name: str, **kwargs) -> str:
                    try:
                        goals = await self.finance.list_goals(api_key)
                        goal = next((g for g in goals if g["name"].lower() == name.lower()), None)
                        if not goal:
                            return json.dumps({"error": f"Goal '{name}' not found."})
                        payload = {
                            "name": kwargs.get("new_name") or goal["name"],
                            "target_amount": kwargs.get("target_amount") or goal["target_amount"],
                            "target_date": kwargs.get("target_date") if "target_date" in kwargs else goal["target_date"],
                            "notes": kwargs.get("notes") if "notes" in kwargs else goal["notes"],
                            "status": kwargs.get("status") or goal["status"],
                        }
                        res = await self.finance.update_goal(api_key, goal["goal_id"], payload)
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_delete_goal(name: str) -> str:
                    try:
                        goals = await self.finance.list_goals(api_key)
                        goal = next((g for g in goals if g["name"].lower() == name.lower()), None)
                        if not goal:
                            return json.dumps({"error": f"Goal '{name}' not found."})
                        res = await self.finance.delete_goal(api_key, goal["goal_id"])
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_contribute_goal(name: str, amount: int, source_account_name: str | None = None, notes: str | None = None) -> str:
                    try:
                        goals = await self.finance.list_goals(api_key)
                        goal = next((g for g in goals if g["name"].lower() == name.lower()), None)
                        if not goal:
                            return json.dumps({"error": f"Goal '{name}' not found."})
                        
                        payload_notes = notes
                        if source_account_name:
                            payload_notes = f"{notes} (from {source_account_name})" if notes else f"From {source_account_name}"
                        
                        payload = {
                            "amount": amount,
                            "source": "manual",
                        }
                        if payload_notes:
                            payload["notes"] = payload_notes
                            
                        res = await self.finance.contribute_goal(api_key, goal["goal_id"], payload)
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_list_obligations(kind: str | None = None, status: str | None = None) -> str:
                    try:
                        params = {}
                        if kind and kind != "all": params["kind"] = kind
                        if status and status != "all": params["status"] = status
                        res = await self.finance.list_obligations(api_key, params)
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_create_obligation(kind: str, title: str, principal_amount: int, counterparty_name: str, **kwargs) -> str:
                    try:
                        payload = {
                            "kind": kind,
                            "title": title,
                            "principal_amount": principal_amount,
                            "counterparty_name": counterparty_name,
                        }
                        if kwargs.get("due_date"): payload["due_date"] = kwargs["due_date"]
                        if kwargs.get("notes"): payload["notes"] = kwargs["notes"]
                        
                        if kwargs.get("default_account_name"):
                            account_name = kwargs["default_account_name"]
                            fresh_accounts = await self.finance.list_accounts(api_key)
                            acc = next((a for a in fresh_accounts if (a.get("account_name") or "").lower() == account_name.lower()), None)
                            if not acc:
                                return json.dumps({"error": f"Account '{account_name}' not found."})
                            payload["default_account_id"] = acc["account_id"]
                            
                        res = await self.finance.create_obligation(api_key, payload)
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_update_obligation(title: str, **kwargs) -> str:
                    try:
                        obligations = await self.finance.list_obligations(api_key, {"status": "all"})
                        ob = next((o for o in obligations if o["title"].lower() == title.lower()), None)
                        if not ob:
                            return json.dumps({"error": f"Obligation '{title}' not found."})
                        
                        payload = {}
                        if "new_title" in kwargs and kwargs["new_title"]: payload["title"] = kwargs["new_title"]
                        if "kind" in kwargs and kwargs["kind"]: payload["kind"] = kwargs["kind"]
                        if "principal_amount" in kwargs and kwargs["principal_amount"] is not None: payload["principal_amount"] = kwargs["principal_amount"]
                        if "counterparty_name" in kwargs and kwargs["counterparty_name"]: payload["counterparty_name"] = kwargs["counterparty_name"]
                        if "due_date" in kwargs: payload["due_date"] = kwargs["due_date"]
                        if "notes" in kwargs: payload["notes"] = kwargs["notes"]
                        
                        if "default_account_name" in kwargs:
                            account_name = kwargs["default_account_name"]
                            if account_name:
                                fresh_accounts = await self.finance.list_accounts(api_key)
                                acc = next((a for a in fresh_accounts if (a.get("account_name") or "").lower() == account_name.lower()), None)
                                if not acc:
                                    return json.dumps({"error": f"Account '{account_name}' not found."})
                                payload["default_account_id"] = acc["account_id"]
                            else:
                                payload["default_account_id"] = None
                                
                        res = await self.finance.update_obligation(api_key, ob["obligation_id"], payload)
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_settle_obligation(title: str, amount: int, source_account_name: str, date: str | None = None, notes: str | None = None) -> str:
                    try:
                        obligations = await self.finance.list_obligations(api_key, {"status": "all"})
                        ob = next((o for o in obligations if o["title"].lower() == title.lower()), None)
                        if not ob:
                            return json.dumps({"error": f"Obligation '{title}' not found."})
                        
                        fresh_accounts = await self.finance.list_accounts(api_key)
                        acc = next((a for a in fresh_accounts if (a.get("account_name") or "").lower() == source_account_name.lower()), None)
                        if not acc:
                            return json.dumps({"error": f"Account '{source_account_name}' not found."})
                        
                        payload = {
                            "amount": amount,
                            "account_id": acc["account_id"],
                        }
                        if date: payload["settled_at"] = date
                        if notes: payload["notes"] = notes
                        
                        res = await self.finance.settle_obligation(api_key, ob["obligation_id"], payload)
                        return json.dumps({"success": True, "result": res})
                    except Exception as e:
                        return json.dumps({"error": str(e)})

                async def _tool_upload_receipt_to_transaction(transaction_id: str) -> str:
                    nonlocal image_bytes, uploaded_receipt
                    if not image_bytes:
                        return json.dumps({
                            "error": "No image/receipt found in the current message payload. Please upload a receipt photo with this request."
                        })
                    if uploaded_receipt:
                        return json.dumps({
                            "error": "The receipt image from the current message payload has already been uploaded."
                        })
                    try:
                        await self.finance.upload_receipt(
                            api_key, transaction_id, bytes(image_bytes), "receipt.jpg", "image/jpeg"
                        )
                        uploaded_receipt = True
                        return json.dumps({
                            "success": True,
                            "transaction_id": transaction_id,
                            "message": f"Successfully uploaded receipt to transaction {transaction_id}."
                        })
                    except Exception as e:
                        logger.exception("Failed to upload receipt")
                        return json.dumps({
                            "error": f"Failed to upload receipt: {str(e)}"
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
                    "create_account": _tool_create_account,
                    "update_account_name": _tool_update_account_name,
                    "update_account_profile": _tool_update_account_profile,
                    "delete_account": _tool_delete_account,
                    "update_movement": _tool_update_movement,
                    "delete_movement": _tool_delete_movement,
                    "audit_transactions": _tool_audit_transactions,
                    "get_summary": _tool_get_summary,
                    "get_analysis": _tool_get_analysis,
                    "get_budget_shift": _tool_get_budget_shift,
                    "list_goals": _tool_list_goals,
                    "create_goal": _tool_create_goal,
                    "update_goal": _tool_update_goal,
                    "delete_goal": _tool_delete_goal,
                    "contribute_goal": _tool_contribute_goal,
                    "list_obligations": _tool_list_obligations,
                    "create_obligation": _tool_create_obligation,
                    "update_obligation": _tool_update_obligation,
                    "settle_obligation": _tool_settle_obligation,
                    "upload_receipt_to_transaction": _tool_upload_receipt_to_transaction,
                }
                intermediate_msg = None

                async def _on_intermediate_text(text: str) -> None:
                    nonlocal intermediate_msg
                    display_text = f"⏳ *Mikir dulu...*\n\n{text}"
                    display_text = display_text.replace("**", "*")
                    if len(display_text) > 4000:
                        display_text = display_text[:4000] + "..."
                        
                    try:
                        if not intermediate_msg:
                            intermediate_msg = await update.message.reply_text(
                                display_text,
                                parse_mode="Markdown",
                                reply_to_message_id=update.message.message_id
                            )
                        else:
                            await intermediate_msg.edit_text(
                                display_text,
                                parse_mode="Markdown",
                            )
                    except Exception as e:
                        logger.warning(f"Failed to update intermediate text: {e}")

                # Call LLM to propose action (agentic loop)
                response_text, executed_tools_summary = await self.llm.propose(
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
                    on_intermediate_text=_on_intermediate_text,
                )

            # Save the user message and assistant natural text response to chat history
            await self.store.add_chat_history(telegram_user_id, "user", message_text or "[Photo receipt]")
            
            if not response_text.strip():
                response_text = "Tugas selesai dijalankan."
                
            await self.store.add_chat_history(telegram_user_id, "assistant", response_text)
            
            if executed_tools_summary:
                tool_log_content = "[Tool log: " + ", ".join(executed_tools_summary) + "]"
                await self.store.add_chat_history(telegram_user_id, "system", tool_log_content)
                
            history_saved = True

            # Clean up double asterisks to single asterisks just in case
            formatted_text = response_text.replace("**", "*")
            
            # Reply to user with Markdown parsing and robust fallback
            try:
                if intermediate_msg:
                    await intermediate_msg.edit_text(
                        formatted_text,
                        parse_mode="Markdown",
                    )
                else:
                    await update.message.reply_text(
                        formatted_text, 
                        parse_mode="Markdown",
                        reply_to_message_id=update.message.message_id
                    )
            except Exception as e:
                logger.warning(f"Failed to send message with Markdown formatting: {e}")
                # Fallback to plain text response
                if intermediate_msg:
                    await intermediate_msg.edit_text(formatted_text)
                else:
                    await update.message.reply_text(
                        formatted_text,
                        reply_to_message_id=update.message.message_id
                    )

            # Trigger background task for history summarization to manage token size
            asyncio.create_task(self._summarize_history_if_needed(telegram_user_id))

        except FinanceError as e:
            await update.message.reply_text(
                f"❌ API error: {e.detail}",
                reply_to_message_id=update.message.message_id
            )
            if not history_saved:
                try:
                    await self.store.add_chat_history(telegram_user_id, "user", message_text or "[Photo receipt]")
                    await self.store.add_chat_history(
                        telegram_user_id,
                        "assistant",
                        f"[System: Request failed with error — API error: {e.detail}. User may retry.]"
                    )
                except Exception as hist_err:
                    logger.error(f"Failed to save error chat history: {hist_err}")
        except Exception as e:
            logger.exception("Message handling failed")
            await update.message.reply_text(f"❌ Terjadi kesalahan: {str(e)}")
            if not history_saved:
                try:
                    await self.store.add_chat_history(telegram_user_id, "user", message_text or "[Photo receipt]")
                    await self.store.add_chat_history(
                        telegram_user_id,
                        "assistant",
                        f"[System: Request failed with error — {str(e)}. User may retry.]"
                    )
                except Exception as hist_err:
                    logger.error(f"Failed to save error chat history: {hist_err}")

    async def _summarize_history_if_needed(self, telegram_user_id: int) -> None:
        """
        Check if the user's chat history exceeds the token limit.
        If it does, summarize the oldest half, replace it in the database,
        and keep the last message of the summarized chunk for context continuity (+1).
        """
        MAX_HISTORY_TOKENS = 128000  # Based on deepseek-v4-flash context handling
        try:
            full_history = await self.store.get_full_chat_history(telegram_user_id, limit=5000)
            if not full_history:
                return

            # Estimate total tokens (rough heuristic: 1 token ~= 4 chars)
            total_chars = sum(len(msg.get("content", "")) for msg in full_history)
            total_tokens = total_chars / 4

            if total_tokens <= MAX_HISTORY_TOKENS:
                return

            logger.info(f"User {telegram_user_id} exceeded max history tokens ({total_tokens} > {MAX_HISTORY_TOKENS}). Starting summarization...")

            # We want to summarize until the token count drops below half
            target_tokens_to_summarize = total_tokens / 2
            
            chars_accum = 0
            chunk_end_idx = 0
            for i, msg in enumerate(full_history):
                chars_accum += len(msg.get("content", ""))
                if (chars_accum / 4) >= target_tokens_to_summarize:
                    chunk_end_idx = i
                    break
            
            # Ensure we summarize at least something if logic falls through
            if chunk_end_idx == 0:
                chunk_end_idx = len(full_history) // 2

            # The chunk to summarize
            chunk_to_summarize = full_history[:chunk_end_idx + 1]
            if len(chunk_to_summarize) < 3:
                return  # Not enough messages to bother summarizing
            
            # Format the chunk for the LLM
            lines = []
            for m in chunk_to_summarize:
                lines.append(f"{m['role'].upper()}: {m['content']}")
            raw_text_to_summarize = "\\n".join(lines)

            summary_prompt = (
                "Summarize the following chat history concisely. "
                "Focus on the user's financial state, important context, and any rules/preferences they established. "
                "Do NOT include conversational filler.\\n\\n"
                f"{raw_text_to_summarize}"
            )

            # Call LLM directly for summarization (no tools)
            messages = [{"role": "system", "content": summary_prompt}]
            resp = await self.llm._client.chat.completions.create(
                model=self.llm._model,
                temperature=0.3,
                messages=messages,
            )
            summary_content = resp.choices[0].message.content or "Summary failed."

            # We replace messages [0 ... chunk_end_idx - 1]
            # We keep chunk_to_summarize[-1] (the message at chunk_end_idx) intact for context continuity (+1).
            ids_to_delete = [m["history_id"] for m in chunk_to_summarize[:-1]]
            anchor_timestamp = chunk_to_summarize[-1]["created_at"]
            
            # A tiny offset so the summary always sorts right before the anchor message
            anchor_timestamp = anchor_timestamp - timedelta(milliseconds=1)

            final_summary = f"[SYSTEM MEMORY] Previous context summary:\\n{summary_content}"

            await self.store.replace_chat_history(
                telegram_user_id, ids_to_delete, final_summary, anchor_timestamp
            )
            logger.info(f"Successfully summarized {len(ids_to_delete)} messages for user {telegram_user_id}.")

        except Exception as e:
            logger.exception(f"Error during history summarization for {telegram_user_id}: {e}")

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
        webhook_url: str | None = None
        watchdog_task: asyncio.Task | None = None

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

                # Start webhook watchdog
                watchdog_task = asyncio.create_task(
                    _webhook_watchdog(
                        bot=application.bot,
                        expected_url=webhook_url,
                        secret_token=settings.telegram_webhook_secret,
                    )
                )
                logger.info("Webhook watchdog started (interval=300s)")

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

        # Start health endpoint for Docker healthcheck
        health_state = {"webhook_url": webhook_url, "started_at": time.time(), "watchdog_ok": True}
        health_runner = await _start_health_server(health_state)

        # Keep running
        await asyncio.Event().wait()
        
    finally:
        if watchdog_task:
            watchdog_task.cancel()
        if health_runner:
            await health_runner.cleanup()
        await close_pool()
        await app_instance.http.aclose()


async def _webhook_watchdog(
    bot,
    expected_url: str,
    secret_token: str,
    interval: int = 300,
) -> None:
    """Periodically verify the Telegram webhook is still registered.

    If the webhook URL is empty or different from expected, re-register it.
    Runs every `interval` seconds (default 5 minutes).
    """
    logger.info(f"Webhook watchdog: expecting URL={expected_url}")
    consecutive_failures = 0

    while True:
        await asyncio.sleep(interval)
        try:
            info = await bot.get_webhook_info()
            current_url = info.url or ""

            if current_url == expected_url:
                consecutive_failures = 0
                logger.debug("Webhook watchdog: OK")
                continue

            # Webhook is missing or wrong!
            logger.warning(
                f"Webhook watchdog: URL mismatch! "
                f"expected={expected_url!r}, got={current_url!r}. "
                f"Re-registering webhook..."
            )
            await bot.set_webhook(
                url=expected_url,
                secret_token=secret_token,
            )

            # Verify it took effect
            verify = await bot.get_webhook_info()
            if (verify.url or "") == expected_url:
                logger.info("Webhook watchdog: re-registered successfully ✓")
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                logger.error(
                    f"Webhook watchdog: re-registration FAILED "
                    f"(attempt {consecutive_failures}). "
                    f"URL is still {verify.url!r}"
                )

        except asyncio.CancelledError:
            logger.info("Webhook watchdog: stopped")
            raise
        except Exception:
            consecutive_failures += 1
            logger.exception(
                f"Webhook watchdog: error checking webhook "
                f"(consecutive failures: {consecutive_failures})"
            )


async def _start_health_server(state: dict, port: int = 8082) -> web.AppRunner:
    """Start a tiny HTTP server on `port` for Docker healthcheck.

    GET /healthz returns 200 if the bot process is alive.
    """
    async def healthz(request: web.Request) -> web.Response:
        uptime = int(time.time() - state["started_at"])
        return web.json_response(
            {"status": "ok", "uptime_seconds": uptime, "webhook_url": state.get("webhook_url", "")},
            status=200,
        )

    app = web.Application()
    app.router.add_get("/healthz", healthz)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Health endpoint listening on 0.0.0.0:{port}/healthz")
    return runner


if __name__ == "__main__":
    asyncio.run(main())
