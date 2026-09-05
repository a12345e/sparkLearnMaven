-- Exercises the whole stack: the metastore stores the definition, HDFS holds
-- the data, and Tez runs the read. This directory is mounted at /scripts in
-- every container, so from the docker directory:
--
--   docker compose exec -T hiveserver2 \
--     beeline -u jdbc:hive2://localhost:10000/default -f /scripts/smoke-test.sql

DROP TABLE IF EXISTS smoke_test;

CREATE TABLE smoke_test (id INT, name STRING);

INSERT INTO smoke_test VALUES (1, 'ada'), (2, 'grace'), (3, 'edsger');

SELECT COUNT(*) AS row_count FROM smoke_test;

SELECT id, name FROM smoke_test ORDER BY id;

DESCRIBE FORMATTED smoke_test;

DROP TABLE smoke_test;
