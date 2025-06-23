-- Updated table for multiple active challenges
CREATE TABLE IF NOT EXISTS active_slot_challenge (
    challenge_id SERIAL PRIMARY KEY,
    game_identifier TEXT NOT NULL,
    game_name TEXT NOT NULL,
    required_multi FLOAT NOT NULL,
    prize FLOAT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    posted_by BIGINT NOT NULL,
    posted_by_username TEXT NOT NULL,
    message_id BIGINT -- for updating the embed if needed
);

-- Table for challenge logs remains the same
CREATE TABLE IF NOT EXISTS slot_challenge_logs (
    id SERIAL PRIMARY KEY,
    game_identifier TEXT NOT NULL,
    game_name TEXT NOT NULL,
    required_multi FLOAT NOT NULL,
    prize FLOAT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    posted_by BIGINT NOT NULL,
    posted_by_username TEXT NOT NULL,
    winner_uid TEXT,
    winner_username TEXT,
    winner_multiplier FLOAT,
    status TEXT NOT NULL -- 'completed' or 'cancelled'
);
