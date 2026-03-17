import os
import asyncio
from supabase import create_async_client

async def check():
    url = os.environ.get('VITE_SUPABASE_URL')
    key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
    if not url or not key: return
    s = await create_async_client(url, key)
    
    print("--- Recent broadcast_tasks ---")
    try:
        res = await s.table('broadcast_tasks').select('*').limit(5).execute()
        for t in res.data:
            print(f"ID: {t.get('id')}, Status: {t.get('status')}, Msg: {t.get('message_data', {}).get('text', 'N/A')[:30] if t.get('message_data') else 'N/A'}")
    except Exception as e:
        print(f"Error: {e}")
        
    print("\n--- Detailed bot_broadcast_progress ---")
    try:
        res = await s.table('bot_broadcast_progress').select('*').limit(5).execute()
        for p in res.data:
            print(f"Task: {p.get('task_id')}, Bot: {p.get('bot_id')}")
            print(f"  Status: {p.get('status')}, Sent: {p.get('sent_count', 0)}/{p.get('total_targeted', 0)}")
            print(f"  Error: {p.get('error_log')}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    asyncio.run(check())
