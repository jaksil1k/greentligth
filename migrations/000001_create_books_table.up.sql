CREATE TABLE IF NOT EXISTS books (
    id bigserial PRIMARY KEY,
    created_at timestamp(0) with time zone NOT NULL DEFAULT NOW(),
    sales integer NOT NULL,
    pages integer NOT NULL,
    title text NOT NULL,
    year integer NOT NULL,
    runtime integer NOT NULL,
    genres text[] NOT NULL,
    version integer NOT NULL DEFAULT 1
    );
