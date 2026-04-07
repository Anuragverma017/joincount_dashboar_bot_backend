"""
Test Script: Manually trigger reminder for a specific leaved user
Run: python test_reminder.py
"""

import asyncio
import os
import datetime
from dotenv import load_dotenv
from supabase import create_async_client
from telethon import TelegramClient
from telethon.tl.custom import Button

load_dotenv()

SUPABASE_URL = os.environ.get("VITE_SUPABASE_URL") or os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("VITE_SUPABASE_ANON_KEY")
API_ID = int(os.environ.get("TELEGRAM_API_ID", "12345678"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "dummyhash")

# ---- CONFIG: Paste the user/bot details here ----
TARGET_USER_ID    = 8723707681                          # telegram_user_id from your record
TARGET_BOT_ID     = "6dc0ad74-df4f-4494-bafa-814999a65ca8"  # bot_id
TARGET_BOT_TOKEN  = None  # Leave None → auto-fetch from DB

async def main():
    print("=" * 60)
    print("  MANUAL REMINDER TEST SCRIPT")
    print("=" * 60)

    supabase = await create_async_client(SUPABASE_URL, SUPABASE_KEY)

    # 1. Fetch bot token if not provided
    bot_token = TARGET_BOT_TOKEN
    if not bot_token:
        bot_res = await supabase.table('telegram_tracker').select('bot_token').eq('id', TARGET_BOT_ID).execute()
        if not bot_res.data:
            print("❌ Bot not found in DB! Check TARGET_BOT_ID.")
            return
        bot_token = bot_res.data[0]['bot_token']
        print(f"✅ Bot token fetched from DB.")

    # 2. Fetch the user record
    user_res = await supabase.table('bot_join_users')\
        .select('*, link:bot_join_links(*)')\
        .eq('telegram_user_id', str(TARGET_USER_ID))\
        .eq('bot_id', TARGET_BOT_ID)\
        .execute()

    if not user_res.data:
        print(f"❌ User {TARGET_USER_ID} not found in bot_join_users for bot {TARGET_BOT_ID}")
        return

    user_record = user_res.data[0]
    print(f"\n📋 User Record Found:")
    print(f"   Name      : {user_record.get('telegram_first_name')} (@{user_record.get('telegram_username')})")
    print(f"   Status    : {user_record.get('status')}")
    print(f"   left_at   : {user_record.get('left_at')}")
    print(f"   last_reminded_at: {user_record.get('last_reminded_at')}")

    link_config = user_record.get('link')
    if not link_config:
        print("❌ No link_config (bot_join_links) found for this user!")
        return

    # 3. Fetch invite link from channel mapping
    invite_link_str = "https://t.me/"
    if link_config.get('channel_mapping_id'):
        m_res = await supabase.table('bot_channel_mappings')\
            .select('invite_link')\
            .eq('id', link_config['channel_mapping_id'])\
            .execute()
        if m_res.data and m_res.data[0].get('invite_link'):
            invite_link_str = m_res.data[0]['invite_link']
            print(f"   Invite Link: {invite_link_str}")
    
    # 4. Start bot client
    print(f"\n🤖 Starting bot client...")
    client = TelegramClient(f"sessions/bot_{TARGET_BOT_ID}", API_ID, API_HASH)
    await client.start(bot_token=bot_token)
    print(f"✅ Bot client started!")

    # 5. Build reminder message
    user_status = user_record.get('status', 'leaved')
    if user_status == 'leaved':
        reminder_text = (
            "👋 *We miss you!*\n\n"
            "It looks like you left the channel. "
            "Come back and rejoin anytime using the link below!"
        )
    else:
        reminder_text = (
            "🔔 *Reminder!*\n\n"
            "You haven't joined the channel yet. "
            "Click the button below to join now!"
        )

    custom_msg = link_config.get('telegram_message') or ""
    if custom_msg:
        reminder_text = f"{reminder_text}\n\n{custom_msg}"

    keyboard = [[Button.url(
        link_config.get('button_text') or "Join Channel",
        invite_link_str
    )]]

    # 6. Send the reminder
    print(f"\n📤 Sending reminder to user {TARGET_USER_ID}...")
    try:
        if link_config.get('telegram_image_url'):
            await client.send_message(
                TARGET_USER_ID, reminder_text,
                file=link_config.get('telegram_image_url'),
                buttons=keyboard,
                parse_mode='md'
            )
        else:
            await client.send_message(
                TARGET_USER_ID, reminder_text,
                buttons=keyboard,
                parse_mode='md'
            )

        # 7. Update last_reminded_at
        now_iso = datetime.datetime.utcnow().isoformat()
        await supabase.table('bot_join_users')\
            .update({'last_reminded_at': now_iso, 'reminder_sent': True})\
            .eq('id', user_record['id'])\
            .execute()

        print(f"✅ SUCCESS! Reminder sent to @{user_record.get('telegram_username')} ({TARGET_USER_ID})")
        print(f"   last_reminded_at updated to: {now_iso}")

    except Exception as e:
        print(f"❌ FAILED to send reminder: {e}")
        print(f"\n   Common causes:")
        print(f"   - User ne bot ko block kar rakha hai")
        print(f"   - User ka account delete ho gaya")
        print(f"   - Bot token galat hai")

    await client.disconnect()
    print("\n✅ Done!")

if __name__ == "__main__":
    asyncio.run(main())
