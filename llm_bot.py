import os
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from supabase import create_client, Client
from telethon import TelegramClient, events
import openai
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

# ---- 1. Logging Setup ----
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("LLMBotManager")

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

# Dictionary to cache the bot configurations from the database
GLOBAL_BOT_CONFIGS = {} 

API_ID = int(os.environ.get("TELEGRAM_API_ID", "12345678"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "dummyhash")

# ---- 2. Supabase Optimization (Thread Pool) ----
supabase_executor = ThreadPoolExecutor(max_workers=20)

async def run_supabase_query(query):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(supabase_executor, query.execute)

# Ensure sessions directory exists
if not os.path.exists("sessions"):
    os.makedirs("sessions")

async def generate_llm_response(bot_id: str, user_message: str) -> str:
    """Generate a response using the configured LLM API key."""
    config = GLOBAL_BOT_CONFIGS.get(bot_id)
    if not config:
        return "Sorry, my configuration is currently unavailable."
    
    if hasattr(config, "get"):
        provider = config.get("provider", "").lower()
        api_key = config.get("api_key")
        business_info = config.get("business_info", "You are a helpful AI support bot.")
        support_name = config.get("support_name", "AI Assistant")
    else:
        provider = getattr(config, "provider", "").lower()
        api_key = getattr(config, "api_key", None)
        business_info = getattr(config, "business_info", "You are a helpful AI support bot.")
        support_name = getattr(config, "support_name", "AI Assistant")
    
    system_prompt = f"Your name is {support_name}. {business_info}"
    
    if not api_key:
        return "⚠️ Setup Error: The API key for this bot has not been configured."
        
    try:
        # ---- OpenAI Handler ----
        if "openai" in provider:
            client = openai.AsyncOpenAI(api_key=api_key)
            response = await client.chat.completions.create(
                model="gpt-3.5-turbo", # Default fast model
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=600
            )
            return response.choices[0].message.content

        # ---- Gemini Handler ----
        elif "gemini" in provider:
            # Re-configure for each call since genai uses global config in older versions,
            # or pass api_key directly to GenerativeModel if supported.
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-1.5-flash',
                                        system_instruction=system_prompt)
            # Disable safety settings which often block legitimate business queries
            safety_settings = {
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
            }
            # Since genai usually runs synchronously, run in executor
            def _generate():
                try:
                    res = model.generate_content(user_message, safety_settings=safety_settings)
                    return res.text
                except Exception as ex:
                    logger.error(f"Gemini error: {ex}")
                    raise
                
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(supabase_executor, _generate)

        else:
            return f"⚠️ Unsupported AI provider: {provider}"
            
    except Exception as e:
        error_msg = str(e).lower()
        if "api key" in error_msg or "unauthorized" in error_msg or "authentication" in error_msg or "invalid_api_key" in error_msg:
            return "⚠️ Setup Error: The provided LLM API key is invalid or has expired. Please update it in the dashboard dashboard."
        elif "quota" in error_msg or "rate limit" in error_msg:
            return "⚠️ Service Error: The LLM provider quota has been exceeded or rate-limited."
        else:
            logger.error(f"LLM Error for bot {bot_id}: {e}")
            return "⚠️ An error occurred while generating a response. Please try again later."

async def start_bot(config: dict):
    bot_id = config['bot_id']
    token = config['bot_token']
    logger.info(f"Starting LLM bot: {bot_id}")
    
    try:
        # Load from the same sessions directory as bot.py
        client = TelegramClient(f"sessions/llm_bot_{bot_id}", API_ID, API_HASH)
        await client.start(bot_token=token)
        logger.info(f"LLM Bot {bot_id} started successfully!")
        
        @client.on(events.NewMessage)
        async def handler(event):
            # Only respond to private messages
            if not event.is_private:
                return

            # Exclude service messages or empty messages
            if event.message.action or not event.message.text:
                return

            user_message = event.message.text
            user_id = event.sender_id
            
            logger.info(f"LLM Bot {bot_id}: Received message from {user_id}: {user_message[:50]}...")
            
            # Show "typing..." status
            async with client.action(event.chat_id, 'typing'):
                response = await generate_llm_response(bot_id, user_message)
                await event.respond(response)

        active_clients[bot_id] = client
        await client.run_until_disconnected()

    except Exception as e:
        logger.error(f"Failed to start LLM bot {bot_id}: {e}")


async def bot_runner():
    logger.info("LLM Bot Manager Started. Polling Supabase every 15 seconds for active chatbot configs...")
    while True:
        try:
            # Join chatbot_configs with telegram_tracker to get the bot_token
            query = supabase.table('chatbot_configs')\
                .select('*, telegram_tracker(bot_token)')\
                .eq('status', 'active')
            
            response = await run_supabase_query(query)
            configs = response.data
            
            if configs is None:
                configs = []
            elif hasattr(configs, "data"):
                # Handle cases where response.data contains another layer of data
                configs = configs.data

            current_bot_ids = set()
            for config in configs:
                bot_id = config['bot_id']
                tracker_data = config.get('telegram_tracker')
                
                # Skip if we couldn't fetch the token
                if not tracker_data or not tracker_data.get('bot_token'):
                    continue
                    
                bot_token = tracker_data['bot_token']
                
                # Merge into a single flat dict
                full_config = {**config, 'bot_token': bot_token}
                
                # Update global cache
                GLOBAL_BOT_CONFIGS[bot_id] = full_config
                current_bot_ids.add(bot_id)
                
                if bot_id not in running_tasks:
                    task = asyncio.create_task(start_bot(full_config))
                    running_tasks[bot_id] = task
                    
            # Check for deleted/paused bots
            for bot_id in list(running_tasks.keys()):
                if bot_id not in current_bot_ids:
                    logger.info(f"LLM Bot {bot_id} is no longer active. Stopping...")
                    running_tasks[bot_id].cancel()
                    
                    if bot_id in GLOBAL_BOT_CONFIGS:
                        GLOBAL_BOT_CONFIGS.pop(bot_id, None)
                        
                    if bot_id in active_clients:
                        try:
                            fut = active_clients[bot_id].disconnect()
                            if asyncio.iscoroutine(fut) or asyncio.isfuture(fut):
                                await fut
                        except Exception as e:
                            logger.error(f"Failed to disconnect bot {bot_id}: {e}")
                        active_clients.pop(bot_id, None)
                        running_tasks.pop(bot_id, None)

        except Exception as e:
            logger.error(f"Error in LLM bot manager loop: {e}")
            
        await asyncio.sleep(15)

if __name__ == "__main__":
    if not os.environ.get("TELEGRAM_API_ID"):
        logger.warning("TELEGRAM_API_ID and TELEGRAM_API_HASH are not set in .env.")
    try:
        asyncio.run(bot_runner())
    except KeyboardInterrupt:
        logger.info("LLM Bot Manager manually stopped.")
