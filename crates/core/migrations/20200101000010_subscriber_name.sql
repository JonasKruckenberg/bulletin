-- A subscriber's display name: used to personalize the digest's greeting ("Good morning, Alice.").
-- Optional — many subscribers are seeded by email alone — so it's nullable and the greeting falls
-- back to the bare time-of-day salutation when it's absent. Additive/expand-contract: the running
-- binary tolerates the new column (it reads/writes it, older binaries simply ignore it).
ALTER TABLE subscriber
    ADD COLUMN name text NULL;
