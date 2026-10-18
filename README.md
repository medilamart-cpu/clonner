# 🤖 Telegram Channel & Forum Cloner Bot (Chat Controlled)

Control everything directly inside Telegram! No need to touch backend configs or restart scripts each time.

---

## ✨ Features
1. **Chat-Controlled Interface**:
   - Run `/login` right inside Telegram with phone number & OTP.
   - **Anti-Detection Spaced OTP**: Enter login code with spaces (e.g. `1 2 3 4 5`) so Telegram filters never block or detect OTP sharing!
   - Full 2FA cloud password support.
2. **Channel & Forum Cloning**:
   - Send `/clone` in chat, paste Source & Destination links or `@usernames`.
   - Replicates **Forum Topics** with matching titles.
   - Copies messages into their exact corresponding topics.
3. **Live Progress in Chat**:
   - The bot dynamically updates a status message with live progress counts (`45/120 messages transferred...`).
   - Supports `/status` and `/stop` commands.
4. **Bypasses Forward Restrictions**:
   - Clean copy mode downloads and re-uploads media if saving/forwarding is disabled on the source.

---

## 🚀 Quick Setup (2 Minutes)

### Step 1: Create a Telegram Bot
1. Open [@BotFather](https://t.me/BotFather) on Telegram.
2. Send `/newbot`, give it a name (e.g., `My Cloner Bot`) and a username ending in `bot` (e.g., `my_channel_cloner_bot`).
3. Copy the **HTTP API Bot Token** provided by BotFather.

### Step 2: Add Bot Token to `config.env`
Open [config.env](file:///e:/transfer%20content/config.env) and paste your bot token:

```env
API_ID=5420108
API_HASH=82282b2e41ffefe2874cb648848efb1a
BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
```

*(Leave `OWNER_ID` blank; the bot will automatically lock to your account when you send `/start`).*

### Step 3: Run the Bot
In PowerShell / Command Prompt:

```powershell
python bot.py
```

---

## 📱 How to Use in Telegram

1. Open your bot in Telegram and send:
   ```text
   /start
   ```
2. **Log In Your Account**:
   - Send `/login`
   - Enter your phone number (e.g. `+919876543210`)
   - When Telegram sends the code, send it **with spaces between each digit**:
     ```text
     1 2 3 4 5
     ```
   - (If 2FA is enabled, enter your cloud password).
3. **Start Cloning**:
   - Send `/clone`
   - Paste **Source Chat** (`@channel_name`, invite link `https://t.me/+...`, or `-100...` ID)
   - Paste **Destination Forum** (`@dest_forum`, invite link, or ID)
   - Choose:
     - `1` ➡️ History + Live Forwarding
     - `2` ➡️ Past History Only
     - `3` ➡️ Live Forwarding Only
4. **Monitor / Stop**:
   - Send `/status` to see current progress.
   - Send `/stop` to cancel anytime.
