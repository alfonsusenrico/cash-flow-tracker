"""Main Telegram bot application with webhook handlers."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
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
from .resolver import resolve
from .store import Store
from .time_utils import parse_time_range, format_date_range

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

            async with TypingContext(context.bot, chat_id):
                # Fetch context in parallel
                accounts, categories = await asyncio.gather(
                    self.finance.list_accounts(api_key),
                    self.finance.list_categories(api_key),
                )
                
                # Get timezone from user (default to Asia/Jakarta)
                tz = "Asia/Jakarta"
                now_iso = datetime.now(timezone.utc).isoformat()

                # Call LLM to propose action
                proposal = await self.llm.propose(
                    message_text=message_text,
                    now_iso=now_iso,
                    timezone=tz,
                    accounts=accounts,
                    categories=categories,
                    image_bytes=image_bytes,
                    image_mime=image_mime,
                    history=history,
                    timeout=self.settings.llm_timeout,
                )

            # Save the user message and assistant JSON response to chat history
            await self.store.add_chat_history(telegram_user_id, "user", message_text or "[Photo receipt]")
            await self.store.add_chat_history(
                telegram_user_id, "assistant", json.dumps(proposal, ensure_ascii=False)
            )

            # Resolve actions from proposal
            actions_list = proposal.get("actions", [])
            assistant_msg = proposal.get("assistant_message", "")

            if not actions_list:
                action = {
                    "intent": "none",
                    "decision": "clarify",
                    "fields": {},
                    "questions": [],
                    "confidence": 0.0,
                    "assistant_message": assistant_msg or "Maaf, bisa diperjelas kembali?",
                }
            else:
                resolved_actions = []
                for act_proposal in actions_list:
                    res_act = resolve(
                        act_proposal,
                        accounts,
                        categories,
                        self.settings.confidence_threshold,
                    )
                    resolved_actions.append(res_act)

                # Determine overall decision
                decisions = [a.get("decision") for a in resolved_actions]
                if "clarify" in decisions:
                    overall_decision = "clarify"
                elif "ask" in decisions:
                    overall_decision = "ask"
                elif "confirm" in decisions:
                    overall_decision = "confirm"
                elif "query" in decisions:
                    overall_decision = "query"
                else:
                    overall_decision = "execute"

                # Consolidate into a single action proposal for downstream processing
                if len(resolved_actions) == 1:
                    action = resolved_actions[0]
                    action["decision"] = overall_decision
                    action["assistant_message"] = assistant_msg or action.get("assistant_message", "")
                else:
                    # Merge all questions
                    questions = []
                    for a in resolved_actions:
                        questions.extend(a.get("questions", []))
                    
                    action = {
                        "is_batch": True,
                        "actions": resolved_actions,
                        "intent": "batch",
                        "decision": overall_decision,
                        "questions": questions,
                        "assistant_message": assistant_msg,
                    }

            # Handle based on decision
            decision = action.get("decision")
            intent = action.get("intent")
            assistant_msg = action.get("assistant_message", "")

            if decision == "clarify":
                await update.message.reply_text(assistant_msg or "Maaf, saya tidak mengerti.")
                return

            if decision == "query":
                if action.get("is_batch"):
                    first_act = action.get("actions", [])[0]
                    await self._handle_query(update, first_act, api_key, accounts)
                else:
                    await self._handle_query(update, action, api_key, accounts)
                return

            if decision == "ask":
                # Ask for missing/ambiguous fields
                questions = action.get("questions", [])
                msg_lines = [assistant_msg or "Perlu informasi tambahan:"]
                seen_fields = set()
                for q in questions:
                    field = q.get("field", "")
                    if field in seen_fields:
                        continue
                    seen_fields.add(field)
                    candidates = q.get("candidates", [])
                    if candidates:
                        msg_lines.append(f"\n{field.title()}: {', '.join(candidates)}")
                    else:
                        msg_lines.append(f"\n{field.title()}: (belum diisi)")
                await update.message.reply_text("\n".join(msg_lines))
                return

            if decision in ("execute", "confirm"):
                # Store pending action
                pending_id = await self.store.create_pending(
                    telegram_user_id, chat_id, action, ttl_seconds=900
                )

                if decision == "execute":
                    # Auto-execute high-confidence creates
                    await self._execute_action(update, context, action, api_key, pending_id)
                else:
                    # Ask for confirmation
                    keyboard = [
                        [
                            InlineKeyboardButton("✅ Ya", callback_data=f"confirm:{pending_id}"),
                            InlineKeyboardButton("❌ Batal", callback_data=f"cancel:{pending_id}"),
                        ]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    summary = self._format_action_summary(action)
                    await update.message.reply_text(
                        f"{assistant_msg}\n\n{summary}\n\nLanjutkan?",
                        reply_markup=reply_markup,
                    )

        except FinanceError as e:
            await update.message.reply_text(f"❌ API error: {e.detail}")
        except Exception as e:
            logger.exception("Message handling failed")
            await update.message.reply_text(f"❌ Terjadi kesalahan: {str(e)}")

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle callback queries (confirmations)."""
        query = update.callback_query
        await query.answer()

        telegram_user_id = update.effective_user.id
        data = query.data or ""

        if data.startswith("cancel:"):
            # Cancel action - just acknowledge
            await query.edit_message_text("❌ Dibatalkan.")
            return

        if data.startswith("confirm:"):
            pending_id = data.split(":", 1)[1]
            api_key = await self.store.get_api_key(telegram_user_id)
            if not api_key:
                await query.edit_message_text("❌ Akun belum terhubung.")
                return

            action = await self.store.take_pending(pending_id, telegram_user_id)
            if not action:
                await query.edit_message_text("❌ Aksi kedaluwarsa atau tidak ditemukan.")
                return

            await self._execute_action(update, context, action, api_key, pending_id, is_callback=True)
            return

        await query.edit_message_text("✅ Selesai.")

    async def _handle_query(
        self,
        update: Update,
        action: dict[str, Any],
        api_key: str,
        accounts: list[dict[str, Any]],
    ) -> None:
        """Handle all query intents (query, query_balance, query_transactions)."""
        intent = action.get("intent")
        assistant_msg = action.get("assistant_message", "")
        
        if intent == "query_balance":
            await self._handle_balance_query(update, action, api_key, accounts)
        elif intent == "query_transactions":
            await self._handle_transaction_query(update, action, api_key, accounts)
        else:
            # Generic query - use search
            query_text = action["fields"].get("query", "")
            result = await self.finance.search_ledger(api_key, {"query": query_text})
            entries = result.get("rows", [])
            if not entries:
                await update.message.reply_text("Tidak ada hasil ditemukan.")
            else:
                lines = [f"📊 Hasil pencarian ({len(entries)} entri):"]
                for entry in entries[:10]:
                    date = entry.get("date", "")
                    name = entry.get("transaction_name", "")
                    debit = int(entry.get("debit") or 0)
                    credit = int(entry.get("credit") or 0)
                    acc = entry.get("account_name", "")
                    amount_str = f"+Rp{debit:,}" if debit > 0 else f"-Rp{credit:,}"
                    lines.append(f"• {date[:10]} - {name}: {amount_str} ({acc})")
                await update.message.reply_text("\n".join(lines))

    async def _handle_balance_query(
        self,
        update: Update,
        action: dict[str, Any],
        api_key: str,
        accounts: list[dict[str, Any]],
    ) -> None:
        """Handle query_balance intent."""
        query_accounts = action.get("query_accounts", [])
        assistant_msg = action.get("assistant_message", "")
        
        # Filter accounts based on query
        if query_accounts:
            # Map account names to IDs
            account_map = {acc["account_name"]: acc for acc in accounts}
            filtered_accounts = [account_map[name] for name in query_accounts if name in account_map]
        else:
            # All accounts
            filtered_accounts = accounts
        
        if not filtered_accounts:
            await update.message.reply_text("❌ Akun tidak ditemukan.")
            return
        
        # Format balance response
        lines = ["💰 Saldo Akun:"]
        total = 0
        for acc in filtered_accounts:
            balance = int(acc.get("balance", 0))
            total += balance
            lines.append(f"• {acc['account_name']}: Rp{balance:,}")
        
        if len(filtered_accounts) > 1:
            lines.append(f"\n📊 Total: Rp{total:,}")
        
        await update.message.reply_text("\n".join(lines))

    async def _handle_transaction_query(
        self,
        update: Update,
        action: dict[str, Any],
        api_key: str,
        accounts: list[dict[str, Any]],
    ) -> None:
        """Handle query_transactions intent."""
        query_accounts = action.get("query_accounts", [])
        time_range = action.get("time_range")
        assistant_msg = action.get("assistant_message", "")
        
        # Parse time range
        try:
            from_dt, to_dt = parse_time_range(time_range)
            from_date = from_dt.strftime("%Y-%m-%d")
            to_date = to_dt.strftime("%Y-%m-%d")
        except Exception as e:
            logger.exception("Failed to parse time range")
            await update.message.reply_text("❌ Gagal memproses rentang waktu.")
            return
        
        # Get account IDs if specific accounts requested
        account_ids = None
        if query_accounts:
            account_map = {acc["account_name"]: acc["account_id"] for acc in accounts}
            account_ids = [account_map[name] for name in query_accounts if name in account_map]
        
        # Query transactions
        try:
            result = await self.finance.query_transactions(
                api_key,
                from_date=from_date,
                to_date=to_date,
                account_ids=account_ids,
                limit=50,
            )
        except Exception as e:
            logger.exception("Failed to query transactions")
            await update.message.reply_text("❌ Gagal mengambil data transaksi.")
            return
        
        rows = result.get("rows", [])
        
        if not rows:
            await update.message.reply_text(f"Tidak ada transaksi ditemukan.\n📅 {format_date_range(from_dt, to_dt)}")
            return
        
        # Format response
        lines = [f"📋 Transaksi ({len(rows)} entri)"]
        lines.append(f"📅 {format_date_range(from_dt, to_dt)}")
        lines.append("")
        
        # Show first 20 transactions
        display_limit = 20
        for row in rows[:display_limit]:
            date = row.get("date", "")
            name = row.get("transaction_name", "")
            debit = int(row.get("debit") or 0)
            credit = int(row.get("credit") or 0)
            acc_name = row.get("account_name", "")
            
            # Format amount with direction
            if debit > 0:
                amount_str = f"+Rp{debit:,}"
            else:
                amount_str = f"-Rp{credit:,}"
            
            lines.append(f"• {date[:10]} | {name}: {amount_str} ({acc_name})")
            
        if len(rows) > display_limit:
            lines.append(f"\n... dan {len(rows) - display_limit} transaksi lainnya")
        
        await update.message.reply_text("\n".join(lines))

    async def _execute_single(
        self,
        act: dict[str, Any],
        api_key: str,
        context: ContextTypes.DEFAULT_TYPE,
        update: Update,
        idx: int,
        is_batch: bool,
        is_callback: bool,
    ) -> str:
        """Execute a single action and return a result message string."""
        intent = act.get("intent")
        fields = act.get("fields", {})
        
        try:
            if intent == "create_transaction":
                payload = {
                    "transaction_type": fields.get("transaction_type"),
                    "amount": fields.get("amount"),
                    "transaction_name": fields.get("transaction_name"),
                    "account_id": fields.get("account_id"),
                    "category_id": fields.get("category_id"),
                    "is_cycle_topup": fields.get("is_cycle_topup", False),
                }
                if fields.get("date"):
                    payload["date"] = fields["date"]
                
                result = await self.finance.upsert_transaction(api_key, payload)
                tx_id = result.get("transaction_id")
                
                prefix = f"{idx}. " if is_batch else ""
                msg_item = f"✅ {prefix}[Transaksi] {fields.get('transaction_name')}: Rp{fields.get('amount'):,} ({fields.get('account_name')})"
                
                # Upload receipt if there was an image (only for first transaction or if photo exists)
                if not is_callback and update.message and update.message.photo and idx == 1:
                    photo = update.message.photo[-1]
                    file = await context.bot.get_file(photo.file_id)
                    image_bytes = await file.download_as_bytearray()
                    await self.finance.upload_receipt(
                        api_key, tx_id, bytes(image_bytes), "receipt.jpg", "image/jpeg"
                    )
                    msg_item += " 📎 Struk diunggah"
                
                return msg_item

            elif intent == "create_movement":
                payload = {
                    "amount": fields.get("amount"),
                    "from_account_id": fields.get("account_id"),
                    "to_account_id": fields.get("target_account_id"),
                }
                if fields.get("date"):
                    payload["date"] = fields["date"]
                
                result = await self.finance.create_movement(api_key, payload)
                
                prefix = f"{idx}. " if is_batch else ""
                return f"✅ {prefix}[Transfer] Rp{fields.get('amount'):,}: {fields.get('account_name')} → {fields.get('target_account_name')}"

            elif intent == "update_transaction":
                prefix = f"{idx}. " if is_batch else ""
                return f"⚠️ {prefix}Update transaksi belum diimplementasikan sepenuhnya."

            elif intent == "delete_transaction":
                prefix = f"{idx}. " if is_batch else ""
                return f"⚠️ {prefix}Hapus transaksi belum diimplementasikan sepenuhnya."

            elif intent == "update_movement":
                prefix = f"{idx}. " if is_batch else ""
                return f"⚠️ {prefix}Update transfer belum diimplementasikan sepenuhnya."

            elif intent == "delete_movement":
                prefix = f"{idx}. " if is_batch else ""
                return f"⚠️ {prefix}Hapus transfer belum diimplementasikan sepenuhnya."

            else:
                prefix = f"{idx}. " if is_batch else ""
                return f"❌ {prefix}Intent tidak dikenali."

        except FinanceError as e:
            prefix = f"{idx}. " if is_batch else ""
            return f"❌ {prefix}Gagal: {e.detail}"
        except Exception as e:
            prefix = f"{idx}. " if is_batch else ""
            return f"❌ {prefix}Error: {str(e)}"

    async def _execute_action(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        action: dict[str, Any],
        api_key: str,
        pending_id: str,
        is_callback: bool = False,
    ) -> None:
        """Execute the resolved action or a batch of resolved actions."""
        is_batch = action.get("is_batch", False)
        actions_list = action.get("actions", []) if is_batch else [action]
        
        try:
            if is_batch:
                # Execute all actions in parallel!
                tasks = [
                    self._execute_single(act, api_key, context, update, idx, True, is_callback)
                    for idx, act in enumerate(actions_list, start=1)
                ]
                results = await asyncio.gather(*tasks)
            else:
                # Single action execution
                res = await self._execute_single(actions_list[0], api_key, context, update, 1, False, is_callback)
                results = [res]

            # Generate consolidated response message
            if is_batch:
                header = "📊 **Hasil Batch Eksekusi:**\n\n"
                msg = header + "\n".join(results)
            else:
                msg = results[0] if results else "❌ Tidak ada aksi yang dieksekusi."

            has_error = any(res.startswith("❌") for res in results)

            # Fetch and display final balances for all referenced accounts if not batch and no errors
            if not is_batch and not has_error and len(actions_list) == 1:
                fields = actions_list[0].get("fields", {})
                account_id = fields.get("account_id")
                if account_id:
                    accounts = await self.finance.list_accounts(api_key)
                    account = next((a for a in accounts if a["account_id"] == account_id), None)
                    if account:
                        balance = int(account["balance"])
                        msg += f"\n\n💰 Saldo {account['account_name']}: Rp{balance:,}"

            if is_callback:
                await update.callback_query.edit_message_text(msg)
            else:
                await update.message.reply_text(msg)

        except Exception as e:
            logger.exception("Action execution failed")
            msg = f"❌ Terjadi kesalahan saat mengeksekusi aksi: {e}"
            if is_callback:
                await update.callback_query.edit_message_text(msg)
            else:
                await update.message.reply_text(msg)

    def _format_action_summary(self, action: dict[str, Any]) -> str:
        """Format action for confirmation message."""
        is_batch = action.get("is_batch", False)
        if is_batch:
            summaries = []
            for idx, act in enumerate(action.get("actions", []), start=1):
                summaries.append(f"{idx}. {self._format_single_action_summary(act)}")
            return "\n\n".join(summaries)
        else:
            return self._format_single_action_summary(action)

    def _format_single_action_summary(self, action: dict[str, Any]) -> str:
        """Format a single action for confirmation message."""
        intent = action.get("intent")
        fields = action.get("fields", {})
        
        if intent == "create_transaction":
            tx_type = "Cash in" if fields.get("transaction_type") == "debit" else "Cash out"
            amount = fields.get("amount", 0)
            name = fields.get("transaction_name", "")
            account = fields.get("account_name", "")
            category = fields.get("category_name", "")
            return f"{tx_type}: Rp{amount:,}\n└ {name} ({account}) [{category or 'Tanpa Kategori'}]"
        
        elif intent == "create_movement":
            amount = fields.get("amount", 0)
            from_acc = fields.get("account_name", "")
            to_acc = fields.get("target_account_name", "")
            return f"Transfer: Rp{amount:,}\n└ Dari: {from_acc} → Ke: {to_acc}"
        
        return str(action)


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
        application.add_handler(
            MessageHandler(filters.TEXT | filters.PHOTO, app_instance.handle_message)
        )
        application.add_handler(CallbackQueryHandler(app_instance.handle_callback))

        # Start webhook
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
        
        logger.info("Bot is running!")
        
        # Keep running
        await asyncio.Event().wait()
        
    finally:
        await close_pool()
        await app_instance.http.aclose()


if __name__ == "__main__":
    asyncio.run(main())
