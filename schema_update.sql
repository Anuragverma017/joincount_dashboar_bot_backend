-- ============================================================
-- Migration: Full status tracking + rejoin tracking system
-- Run this in your Supabase SQL Editor
-- ============================================================

-- 1. Add the 'status' column
--    Values: 'bot_started' | 'pending' | 'active' | 'leaved'
ALTER TABLE bot_join_users
ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'bot_started';

-- 2. Add 'last_reminded_at' to track 12-hour reminder intervals
ALTER TABLE bot_join_users
ADD COLUMN IF NOT EXISTS last_reminded_at TIMESTAMP WITH TIME ZONE DEFAULT NULL;

-- 3. Keep 'reminder_sent' for backward-compat flag
ALTER TABLE bot_join_users
ADD COLUMN IF NOT EXISTS reminder_sent BOOLEAN DEFAULT FALSE;

-- 4. Track when a user rejoined after leaving
--    NULL  = never rejoined, OR user left again after rejoining
--    Value = timestamp of most recent rejoin
ALTER TABLE bot_join_users
ADD COLUMN IF NOT EXISTS rejoined_at TIMESTAMP WITH TIME ZONE DEFAULT NULL;

-- 5. Migrate EXISTING rows to the new status column
UPDATE bot_join_users
SET status = CASE
    WHEN left_channel = TRUE                                                       THEN 'leaved'
    WHEN joined_channel = TRUE AND (left_channel = FALSE OR left_channel IS NULL)  THEN 'active'
    ELSE 'bot_started'
END
WHERE status IS NULL OR status = 'bot_started';

-- 6. Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_bot_join_users_reminder
ON bot_join_users (bot_id, status, last_reminded_at);

CREATE INDEX IF NOT EXISTS idx_bot_join_users_rejoined
ON bot_join_users (bot_id, rejoined_at);

-- ============================================================
-- COLUMN REFERENCE:
--
--   status           → current state: bot_started / pending / active / leaved
--   last_reminded_at → timestamp of last reminder sent (12h interval tracking)
--   reminder_sent    → boolean flag (helper, backward compat)
--   rejoined_at      → when user rejoined after leaving
--                      NULL = never rejoined OR left again after rejoining
--
-- STATUS LIFECYCLE:
--   bot_started  → user clicked /start
--   pending      → user sent a join request
--   active       → user joined the channel
--   leaved       → user left the channel
--
-- REJOIN TRACKING (using rejoined_at only):
--   User rejoins after leaving  →  rejoined_at = NOW(), status = 'active'
--   User leaves again           →  rejoined_at = NULL,  status = 'leaved'
--
-- DASHBOARD QUERIES:
--   Users who are currently in rejoined state:
--     SELECT * FROM bot_join_users WHERE rejoined_at IS NOT NULL AND status = 'active';
--
--   Users who left and came back (ever):
--     SELECT * FROM bot_join_users WHERE rejoined_at IS NOT NULL;
-- ============================================================
