--SKIP requires-lake-formation-catalog
-- Lake Formation fine-grained access control patterns (epic KAN-88).
-- https://docs.aws.amazon.com/lake-formation/latest/dg/lf-permissions-reference.html
-- Skipped: GRANT/REVOKE enforcement requires a Lake Formation-aware catalog (Glue catalog
-- with Lake Formation authorization), which is tracked separately in the epic.

GRANT SELECT ON TABLE analytics.events TO ROLE bi_reader;

GRANT SELECT (region, amount) ON TABLE analytics.sales TO ROLE finance;

REVOKE SELECT ON TABLE analytics.events FROM ROLE bi_reader;

GRANT ALL PRIVILEGES ON DATABASE analytics TO ROLE etl_writer;

SHOW GRANTS ON TABLE analytics.events;
