-- digest_config.corroboration_weight: the priority-boost weight for cross-source corroboration
-- (independent sources corroborating one story). Previously source diversity only chose the Story/Note
-- *format*; this lifts a corroborated story's *priority* too — the strongest "this matters" signal a
-- multi-source aggregator has (design §8.3). Additive with a default that matches the code constant, so
-- a running binary tolerates the new column and the behavior change is opt-out via `--corroboration-weight 0`.
ALTER TABLE digest_config ADD COLUMN corroboration_weight double precision NOT NULL DEFAULT 0.5;
