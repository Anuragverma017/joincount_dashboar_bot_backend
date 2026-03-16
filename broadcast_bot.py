import os
import asyncio
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
from telethon import TelegramClient, events, Button
from concurrent.futures import ThreadPoolExecutor

# ---- Logging Setup ----
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("BroadcastBot")

load_dotenv()

SUPABASE_URL = os.environ.get("VITE_SUPABASE_URL") or os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("VITE_SUPABASE_ANON_KEY") or os.environ.get("SUPABASE_KEY")
API_ID = int(os.environ.get("TELEGRAM_API_ID", "12345678"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "dummyhash")
BOT_TOKEN = os.environ.get("BROADCAST_BOT_TOKEN")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase URL and Key must be defined")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

async def run_supabase_query(query):
    return await asyncio.to_thread(query.execute)

# {user_id: {'step': 'selecting_channel', 'channel_id': '...', 'channel_name': '...', 'message_data': {...}}}
user_states = {}

async def get_owner_data(tg_user_id):
    # Try profiles first
    query = supabase.table('profiles').select('id').eq('telegram_user_id', tg_user_id)
    res = await run_supabase_query(query)
    if res.data:
        return res.data[0]
    
    # Try subscriptions as fallback
    query = supabase.table('app_user_subscriptions').select('user_id').eq('telegram_user_id', tg_user_id)
    res = await run_supabase_query(query)
    if res.data:
        return {'id': res.data[0]['user_id']}
        
    return None

async def get_owner_channels(user_id):
    bots_res = await run_supabase_query(supabase.table('telegram_tracker').select('id').eq('user_id', user_id))
    if not bots_res or not bots_res.data:
        return []
    bot_ids = [b['id'] for b in bots_res.data]
    mappings_res = await run_supabase_query(supabase.table('bot_channel_mappings').select('channel_id, channel_name').in_('bot_id', bot_ids).eq('status', 'Active'))
    return mappings_res.data if mappings_res and mappings_res.data else []

async def main():
    if not BOT_TOKEN:
        logger.error("BROADCAST_BOT_TOKEN not found in .env")
        return

    # Ensure sessions directory exists relative to the script
    base_dir = os.path.dirname(os.path.abspath(__file__))
    sessions_dir = os.path.join(base_dir, "sessions")
    if not os.path.exists(sessions_dir):
        os.makedirs(sessions_dir)

    session_path = os.path.join(sessions_dir, "broadcast_master")
    
    client = TelegramClient(session_path, API_ID, API_HASH)
    await client.start(bot_token=BOT_TOKEN)
    logger.info("Broadcast Master Bot (@Gapgrowbot) started!")

    @client.on(events.NewMessage)
    async def debug_log_handler(event):
        logger.info(f"Incoming message from {event.sender_id}: {event.text}")

    @client.on(events.NewMessage(pattern=r'^/start(?: (.*))?'))
    async def start_handler(event):
        payload = event.pattern_match.group(1)
        sender = await event.get_sender()
        logger.info(f"Handling /start for {sender.id} with payload: {payload}")

        if payload and payload.lower() != "true":
            logger.info(f"Attempting to link account for UUID: {payload} with TG ID: {sender.id}")
            try:
                # Update profiles
                p_res = await run_supabase_query(
                    supabase.table('profiles')
                    .update({'telegram_user_id': sender.id})
                    .eq('id', payload)
                )
                logger.info(f"Profile update result: {p_res.data if p_res else 'None'}")
                
                # Update subscriptions
                s_res = await run_supabase_query(
                    supabase.table('app_user_subscriptions')
                    .update({'telegram_user_id': sender.id})
                    .eq('user_id', payload)
                )
                logger.info(f"Subscription update result: {s_res.data if s_res else 'None'}")
                
                await event.respond(
                    "✅ **Telegram Account Connected Successfully!**\n\n"
                    "Your account is now linked to the GAP dashboard. You can return to the dashboard to start broadcasting."
                )
                return
            except Exception as e:
                logger.error(f"DETAILED ERROR linking account for payload {payload}: {e}", exc_info=True)
                await event.respond(f"❌ Failed to link account: {str(e)}")
                return

        owner = await get_owner_data(sender.id)
        logger.info(f"Owner data for {sender.id}: {owner}")
        if not owner:
            await event.respond(
                "🚀 **Welcome to GAP Grow Broadcast Bot**\n\n"
                "To start broadcasting, please connect your Telegram account in the dashboard first.\n\n"
                "If you just clicked the 'Start Setup' button, please try again."
            )
            return

        msg = (
            "🚀 **Welcome to GAP Grow Broadcast Bot**\n\n"
            "Use this bot to send messages to all users who joined your channels through our tracking bots.\n\n"
            "**Instructions:**\n"
            "1️⃣ Use `/send` to start building your broadcast.\n"
            "2️⃣ Select the target channel from your connected list.\n"
            "3️⃣ Send the message you want to broadcast.\n"
            "4️⃣ Verify and confirm the broadcast.\n\n"
            "Type `/send` to begin!"
        )
        await event.respond(msg)

    @client.on(events.NewMessage(pattern='/send'))
    async def send_handler(event):
        sender = await event.get_sender()
        owner = await get_owner_data(sender.id)
        if not owner:
            await event.respond("Owner verification failed. Please check your dashboard.")
            return

        channels = await get_owner_channels(owner['id'])
        if not channels:
            await event.respond("No active channels found. Connect a bot to a channel first.")
            return

        seen_ids = set()
        buttons = []
        for ch in channels:
            cid = ch['channel_id']
            if cid not in seen_ids:
                buttons.append([Button.inline(ch['channel_name'] or "Unnamed", data=f"selchan_{cid}")])
                seen_ids.add(cid)

        user_states[sender.id] = {'step': 'selecting_channel'}
        await event.respond("Select the target channel audience:", buttons=buttons)

    @client.on(events.CallbackQuery(data=lambda d: d.decode().startswith('selchan_')))
    async def channel_selection_handler(event):
        channel_id = event.data.decode().split('_')[1]
        sender_id = event.sender_id
        
        res = await run_supabase_query(supabase.table('bot_channel_mappings').select('channel_name').eq('channel_id', channel_id).limit(1))
        channel_name = res.data[0]['channel_name'] if res and res.data else "Unknown"

        user_states[sender_id] = {
            'step': 'awaiting_message',
            'channel_id': channel_id,
            'channel_name': channel_name
        }

        await event.edit(f"✅ **{channel_name}** selected.\n\nNow, send the message (text/media) you want to broadcast.", buttons=[Button.inline("❌ Cancel", data="cancel_broadcast")])

    @client.on(events.NewMessage)
    async def message_input_handler(event):
        sender_id = event.sender_id
        state = user_states.get(sender_id)
        if not state or state.get('step') != 'awaiting_message' or (event.text and event.text.startswith('/')):
            return

        media_path = None
        if event.media:
            # Create a unique filename for this media
            base_dir = os.path.dirname(os.path.abspath(__file__))
            media_dir = os.path.join(base_dir, "broadcast_media")
            if not os.path.exists(media_dir):
                os.makedirs(media_dir)
            
            # Use event.id for a temporary unique name
            media_path = await event.download_media(file=os.path.join(media_dir, f"tmp_{event.id}"))
            logger.info(f"Downloaded media to {media_path}")

        state.update({
            'step': 'verifying',
            'original_msg': event.message,
            'media_path': media_path,
            'message_data': {
                'text': event.text, 
                'media': event.media is not None,
                'raw_text': event.message.message
            }
        })
        
        await event.respond("📝 **Preview of your broadcast message:**")
        preview_msg = await event.message.reply(
            f"Broadcast to audience of **{state['channel_name']}**?",
            buttons=[
                [Button.inline("✅ Send Broadcast", data="confirm_send")],
                [Button.inline("❌ Cancel", data="cancel_broadcast")]
            ]
        )
        state['preview_msg_id'] = preview_msg.id

    @client.on(events.CallbackQuery(data='confirm_send'))
    async def confirm_handler(event):
        sender_id = event.sender_id
        state = user_states.get(sender_id)
        if not state or state.get('step') != 'verifying':
            await event.answer("Session expired. Start again with /send.")
            return

        owner = await get_owner_data(sender_id)
        task_data = {
            'user_id': owner['id'],
            'channel_id': state['channel_id'],
            'message_data': {
                'text': state['original_msg'].text,
                'has_media': state['original_msg'].media is not None,
                'raw_text': state['original_msg'].message,
                'media_path': state.get('media_path')
            },
            'status': 'pending'
        }
        
        res = await run_supabase_query(supabase.table('broadcast_tasks').insert(task_data))
        if res and res.data:
            await event.edit(f"🚀 **Broadcast started!** Task ID: {res.data[0]['id'][:8]}...")
            user_states.pop(sender_id, None)
        else:
            await event.edit("❌ Failed to create task.")

    @client.on(events.CallbackQuery(data='cancel_broadcast'))
    async def cancel_handler(event):
        user_states.pop(event.sender_id, None)
        await event.edit("❌ Broadcast cancelled.")

    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
