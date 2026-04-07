-- ============================================================
-- FINAL MISSING FIXES — Run in Supabase SQL Editor
-- ============================================================

-- FIX 1: Add rejoin_count to track how many times user rejoined
ALTER TABLE public.bot_join_users
ADD COLUMN IF NOT EXISTS rejoin_count INTEGER DEFAULT 0;

-- FIX 2: Add is_bot_blocked to skip users who blocked the bot
--        (reminder_sent is BOOLEAN — cannot use it to track "blocked" state)
ALTER TABLE public.bot_join_users
ADD COLUMN IF NOT EXISTS is_bot_blocked BOOLEAN DEFAULT false;

-- FIX 3: Add auto_approve toggle per channel mapping
ALTER TABLE public.bot_channel_mappings
ADD COLUMN IF NOT EXISTS auto_approve BOOLEAN DEFAULT true;

-- ============================================================
-- VERIFY — Run after applying to confirm
-- ============================================================
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'bot_join_users'
  AND table_schema = 'public'
ORDER BY ordinal_position;
