-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ALTER TABLE movies ALTER COLUMN version DROP DEFAULT;
-- ALTER TABLE movies ALTER COLUMN version TYPE uuid USING (uuid_generate_v4());
-- ALTER TABLE movies ALTER COLUMN version SET DEFAULT uuid_generate_v4();