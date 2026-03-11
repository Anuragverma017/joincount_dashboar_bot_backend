import os
import asyncio
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from supabase import create_client, Client
from telethon import TelegramClient, events
from telethon.tl.functions.messages import ExportChatInviteRequest
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.errors import UserNotParticipantError
from telethon.tl.custom import Button
import aiohttp

# ---- 1. Logging Setup ----
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("BotManager")

# Load environment variables
load_dotenv()

SUPABASE_URL = os.environ.get("VITE_SUPABASE_URL") or os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("VITE_SUPABASE_ANON_KEY") or os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase URL and Key must be defined in environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Dictionary to store running clients and their tasks
active_clients = {}
running_tasks = {}

# Dictionary to cache the bot configurations from the database (reduces DB calls drastically)
GLOBAL_BOT_CONFIGS = {} 

API_ID = int(os.environ.get("TELEGRAM_API_ID", "12345678"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "dummyhash")


# ---- 2. Supabase Optimization (Thread Pool) ----
# Using a thread pool prevents the asyncio event loop from getting blocked
# by concurrent synchronous HTTP requests to Supabase
supabase_executor = ThreadPoolExecutor(max_workers=20)

async def run_supabase_query(query):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(supabase_executor, query.execute)


# Ensure sessions directory exists
if not os.path.exists("sessions"):
    os.makedirs("sessions")

async def start_bot(token: str, bot_id: str):
    logger.info(f"Starting bot: {bot_id}")
    try:
        # ---- 3. Persistent Sessions ----
        # Using file-based sessions so when the server restarts or the bot disconnects,
        # it doesn't need to do completely new full reconnections every time.
        client = TelegramClient(f"sessions/bot_{bot_id}", API_ID, API_HASH)
        
        await client.start(bot_token=token)
        logger.info(f"Bot {bot_id} started successfully!")
        
        # Handler for /start <ad_id>
        @client.on(events.NewMessage(pattern=r'^/start(?: (.*))?'))
        async def handler(event):
            payload = event.pattern_match.group(1)
            sender = await event.get_sender()
            user_id = sender.id
            
            if payload:
                logger.info(f"Bot {bot_id}: User {user_id} started with payload/ad_id: {payload}")
                
                # 1. Fetch the bot join link configuration
                link_query = supabase.table('bot_join_links').select('*').eq('slug', payload).eq('bot_id', bot_id)
                link_res = await run_supabase_query(link_query)
                
                if link_res.data:
                    link_config = link_res.data[0]
                    link_id = link_config['id']
                    admin_id = link_config['user_id']
                    
                    # Fetch cached bot config to reduce DB calls
                    bot_config = GLOBAL_BOT_CONFIGS.get(bot_id, {})
                    channel_id = bot_config.get('channel_id')
                    
                    # ---- 4. Check if user already joined ----
                    already_joined = False
                    channel_link_str = "https://t.me/" # Fallback
                    
                    if channel_id:
                        try:
                            # Channels in Bot API/Telethon require -100 prefix.
                            full_channel_id = int(f"-100{channel_id}")
                            
                            try:
                                # Active check with Telegram API to see if they are already in the channel
                                participant = await client(GetParticipantRequest(channel=full_channel_id, participant=user_id))
                                if participant:
                                    already_joined = True
                            except UserNotParticipantError:
                                already_joined = False
                            except Exception as e:
                                logger.error(f"Bot {bot_id}: Failed to check participant: {e}")

                            try:
                                # Generate a request-to-join link for the channel (request_needed=True)
                                # This ensures the bot can track who joins by approving requests
                                invite_link = await client(ExportChatInviteRequest(
                                    peer=full_channel_id,
                                    request_needed=True,
                                    title=f"Bot Join Link - {bot_id[:8]}"
                                ))
                                channel_link_str = invite_link.link
                            except Exception as req_err:
                                logger.warning(f"Bot {bot_id}: Could not create request link: {req_err}. Trying normal link...")
                                try:
                                    # Fallback to normal invite link if request_needed is not allowed
                                    invite_link = await client(ExportChatInviteRequest(peer=full_channel_id))
                                    channel_link_str = invite_link.link
                                except Exception as e2:
                                    logger.error(f"Bot {bot_id}: Failed to generate any invite link: {e2}")
                                    # If all fails, attempt to fallback to public channel username
                                    try:
                                        ent = await client.get_entity(full_channel_id)
                                        if getattr(ent, 'username', None):
                                            channel_link_str = f"https://t.me/{ent.username}"
                                    except Exception:
                                        pass
                        except Exception as eOuter:
                            logger.error(f"Bot {bot_id}: Critical error processing channel {channel_id}: {eOuter}")
                    
                    # Log the start event in bot_join_users
                    try:
                        upsert_data = {
                            "user_id": admin_id,
                            "bot_id": bot_id,
                            "link_id": link_id,
                            "telegram_user_id": str(user_id),
                            "telegram_username": getattr(sender, 'username', None),
                            "telegram_first_name": getattr(sender, 'first_name', None),
                            "joined_channel": already_joined
                        }
                        if already_joined:
                            upsert_data["joined_at"] = datetime.datetime.utcnow().isoformat()
                            
                        upsert_query = supabase.table('bot_join_users').upsert(upsert_data, on_conflict="link_id,telegram_user_id")
                        await run_supabase_query(upsert_query)
                    except Exception as log_err:
                        logger.error(f"Failed to log bot start: {log_err}")

                    # Construct customized message to send
                    keyboard = [[Button.url(link_config.get('button_text') or "Join Channel", channel_link_str)]]
                    has_extra = bool(link_config.get('telegram_extra_message'))

                    if link_config.get('telegram_image_url'):
                        await event.respond(
                            link_config.get('telegram_message') or "Click the button below to join the private channel.",
                            file=link_config.get('telegram_image_url'),
                            buttons=None if has_extra else keyboard
                        )
                    else:
                        await event.respond(
                            link_config.get('telegram_message') or "Click the button below to join the private channel.",
                            buttons=None if has_extra else keyboard
                        )
                        
                    # Send Extra message if configured
                    if has_extra:
                        await event.respond(link_config.get('telegram_extra_message'), buttons=keyboard)
                else:
                    await event.respond("Invalid or expired join link.")
            else:
                await event.respond("Welcome to the bot! Use a valid join link to get started.")
                
        # Handle chat member updates for join and leave tracking
        @client.on(events.ChatAction)
        async def chat_handler(event):
            try:
                # Optimized: Use global config instead of querying Supabase
                bot_config = GLOBAL_BOT_CONFIGS.get(bot_id, {})
                channel_id = bot_config.get('channel_id')
                
                if not channel_id:
                    return
                mapped_channel_id = str(channel_id)
                
                chat = await event.get_chat()
                if mapped_channel_id not in str(chat.id):
                    return

                if getattr(event, 'user_joined', False) or getattr(event, 'user_added', False):
                    user_event = await event.get_user()
                    logger.info(f"Bot {bot_id}: User {user_event.id} joined Chat {chat.id}")
                    try:
                        now_iso = datetime.datetime.utcnow().isoformat()
                        update_query = supabase.table('bot_join_users').update({
                            "joined_channel": True,
                            "left_channel": False,
                            "joined_at": now_iso
                        }).eq('bot_id', bot_id).eq('telegram_user_id', str(user_event.id))
                        await run_supabase_query(update_query)
                        logger.info(f"Successfully tracked: User {user_event.id} joined the target channel.")
                    except Exception as log_err:
                        logger.error(f"Failed to update channel join stats: {log_err}")

                elif getattr(event, 'user_left', False) or getattr(event, 'user_kicked', False):
                    user_event = await event.get_user()
                    logger.info(f"Bot {bot_id}: User {user_event.id} left Chat {chat.id}")
                    try:
                        now_iso = datetime.datetime.utcnow().isoformat()
                        update_query = supabase.table('bot_join_users').update({
                            "left_channel": True,
                            "left_at": now_iso
                        }).eq('bot_id', bot_id).eq('telegram_user_id', str(user_event.id))
                        await run_supabase_query(update_query)
                        logger.info(f"Successfully tracked: User {user_event.id} left the target channel.")
                    except Exception as log_err:
                        logger.error(f"Failed to update channel leave stats: {log_err}")
                        
            except Exception as ev_err:
                logger.error(f"Error in chat handler: {ev_err}")

        # Handle channel messages to link channels
        @client.on(events.NewMessage)
        async def channel_message_handler(event):
            # Only process if the message is in a channel
            if event.is_channel and not event.is_group:
                # Optimized: Use global config instead of querying Supabase
                bot_config = GLOBAL_BOT_CONFIGS.get(bot_id, {})
                channel_id = bot_config.get('channel_id')
                
                # If a channel is already mapped, we don't need to listen for new channel messages
                if channel_id:
                    return

                chat = await event.get_chat()
                full_channel_id = chat.id
                
                try:
                    channel_name = getattr(chat, 'title', f"Channel {full_channel_id}")
                    channel_username = getattr(chat, 'username', None)
                    icon_url = None

                    # Try to fetch the real channel photo using the Bot API
                    try:
                        async with aiohttp.ClientSession() as http_session:
                            api_chat_id = f"-100{full_channel_id}"
                            chat_url = f"https://api.telegram.org/bot{token}/getChat?chat_id={api_chat_id}"
                            async with http_session.get(chat_url) as resp:
                                chat_data = await resp.json()
                                if chat_data.get('ok') and 'photo' in chat_data['result']:
                                    file_id = chat_data['result']['photo']['big_file_id']
                                    
                                    file_url = f"https://api.telegram.org/bot{token}/getFile?file_id={file_id}"
                                    async with http_session.get(file_url) as file_resp:
                                        file_data = await file_resp.json()
                                        if file_data.get('ok'):
                                            file_path = file_data['result']['file_path']
                                            download_url = f"https://api.telegram.org/file/bot{token}/{file_path}"
                                            
                                            # Download the actual image and convert to base64
                                            # Telegram file URLs expire after ~1 hour, so we must store the raw data
                                            import base64
                                            async with http_session.get(download_url) as img_resp:
                                                if img_resp.status == 200:
                                                    img_bytes = await img_resp.read()
                                                    b64_encoded = base64.b64encode(img_bytes).decode('utf-8')
                                                    # Telegram profile photos are usually JPEGs
                                                    icon_url = f"data:image/jpeg;base64,{b64_encoded}"
                                                    logger.info(f"Bot {bot_id}: Successfully converted channel image to Base64.")
                    except Exception as photo_err:
                        logger.warning(f"Bot {bot_id}: Could not fetch or convert channel photo: {photo_err}")

                    logger.info(f"Bot {bot_id}: Detected message in Channel '{channel_name}' ({full_channel_id}). Adding to list...")
                    
                    try:
                        upsert_query = supabase.table('bot_detected_channels').upsert({
                            'bot_id': bot_id,
                            'channel_id': str(full_channel_id),
                            'channel_name': channel_name,
                            'channel_username': channel_username,
                            'channel_icon_url': icon_url
                        }, on_conflict='bot_id,channel_id')
                        await run_supabase_query(upsert_query)
                        logger.info(f"Bot {bot_id}: Successfully logged channel '{channel_name}' for manual mapping.")
                    except Exception as db_err:
                        logger.error(f"Bot {bot_id}: Note on DB insert: {db_err}")
                    
                except Exception as e:
                    logger.error(f"Bot {bot_id}: Error fetching channel info: {e}")

        active_clients[bot_id] = client
        
        # ---- 5. Missing state transition handling fixed ----
        # The bot will run continuously without the 5-minute timeout.
        # Since the bot_runner polls Supabase every 15 seconds, your bot instantly starts
        # behaving as 'Active' when the user maps a channel, seamlessly.
        await client.run_until_disconnected()

    except Exception as e:
        logger.error(f"Failed to start bot {bot_id}: {e}")

async def bot_runner():
    logger.info("Bot Manager Started. Polling Supabase every 15 seconds for new or pending bots...")
    while True:
        try:
            # Check for both capitalized and lowercase statuses since the frontend uses lowercase 'active'
            query = supabase.table('telegram_tracker').select('*').in_('status', ['Pending', 'Active', 'pending', 'active'])
            response = await run_supabase_query(query)
            bots = response.data
            
            if bots is None:
                bots = []

            current_bot_ids = set()
            for bot in bots:
                bot_id = bot['id']
                token = bot['bot_token']
                
                # Update global cached configuration for all running bots
                GLOBAL_BOT_CONFIGS[bot_id] = bot
                current_bot_ids.add(bot_id)
                
                if bot_id not in running_tasks:
                    task = asyncio.create_task(start_bot(token, bot_id))
                    running_tasks[bot_id] = task
                    
            # Check for deleted tasks/bots or bots that are no longer Pending/Active
            for bot_id in list(running_tasks.keys()):
                if bot_id not in current_bot_ids:
                    logger.info(f"Bot {bot_id} is no longer active/pending. Stopping...")
                    running_tasks[bot_id].cancel()
                    
                    if bot_id in GLOBAL_BOT_CONFIGS:
                        del GLOBAL_BOT_CONFIGS[bot_id]
                        
                    if bot_id in active_clients:
                        try:
                            fut = active_clients[bot_id].disconnect()
                            if asyncio.iscoroutine(fut) or asyncio.isfuture(fut):
                                await fut
                        except Exception as e:
                            logger.error(f"Failed to disconnect bot {bot_id}: {e}")
                        del active_clients[bot_id]
                        del running_tasks[bot_id]
                        
                        # Wait for telethon's background tasks to fully release the SQLite file lock
                        await asyncio.sleep(1.5)
                        
                        # Delete the session file from disk
                        session_file = f"sessions/bot_{bot_id}.session"
                        if os.path.exists(session_file):
                            try:
                                os.remove(session_file)
                                logger.info(f"Deleted session file for bot {bot_id}")
                            except Exception as e:
                                logger.error(f"Failed to delete session file for bot {bot_id}: {e}")
                                
                        # Delete the SQLite journal file if it exists
                        journal_file = f"sessions/bot_{bot_id}.session-journal"
                        if os.path.exists(journal_file):
                            try:
                                os.remove(journal_file)
                            except Exception as e:
                                pass

        except Exception as e:
            logger.error(f"Error in bot manager loop: {e}")
            
        await asyncio.sleep(15)

if __name__ == "__main__":
    if not os.environ.get("TELEGRAM_API_ID"):
        logger.warning("TELEGRAM_API_ID and TELEGRAM_API_HASH are not set in .env. Bots won't connect unless set.")
    try:
        asyncio.run(bot_runner())
    except KeyboardInterrupt:
        logger.info("Bot Manager manually stopped.")