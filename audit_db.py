import os
import asyncio
from supabase import create_async_client

async def check():
    url = os.environ.get('VITE_SUPABASE_URL')
    key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
    if not url or not key: return
    s = await create_async_client(url, key)
    
    # Check bot_join_users count
    res = await s.table('bot_join_users').select('telegram_user_id', count='exact').limit(1).execute()
    users_total = res.count or 0
    print(f"Total users in bot_join_users: {users_total}")
    
    if users_total > 0:
        # See a few records to see what links they use
        res = await s.table('bot_join_users').select('telegram_user_id, link_id').limit(5).execute()
        for u in res.data:
            print(f"User {u['telegram_user_id']} linked to {u['link_id']}")
            # Find the bot for this link
            l_res = await s.table('bot_join_links').select('bot_id').eq('id', u['link_id']).execute()
            if l_res.data:
                print(f"  Link {u['link_id']} belongs to Bot {l_res.data[0]['bot_id']}")
            else:
                print(f"  Link {u['link_id']} NOT FOUND in bot_join_links")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    asyncio.run(check())
