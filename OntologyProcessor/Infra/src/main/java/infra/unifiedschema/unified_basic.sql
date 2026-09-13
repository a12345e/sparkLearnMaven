CREATE EXTERNAL TABLE ontology.unified_basic(
  relating_value       STRING COMMENT 'relating object value',
  relating_attributes  MAP<STRING, STRING> COMMENT 'attributes of the relating object when there is more required information',

  related_value        STRING COMMENT 'related object value',
  related_attributes   MAP<STRING, STRING> COMMENT 'attributes of the related object when there is more required information',

  time_start           TIMESTAMP COMMENT 'start time',
  time_end             TIMESTAMP COMMENT 'end time',
  time_events_set      ARRAY<TIMESTAMP> COMMENT 'event set',

  source_dts           STRING[] COMMENT 'relevant only when the tree height is 0, can be null when not relevant',

  attributes           MAP<STRING, STRING> COMMENT 'attributes of the event',
  events_count         INT COMMENT 'number of events'
)
PARTITIONED BY (
  relation   STRING COMMENT 'relation between the relating object and related object',
  product    STRING COMMENT 'string integer of the tree number',
  period     STRING COMMENT 'period_start  hour_2026010100, FOUR_HOUR_2026010108',
  dt TIMETAMP COMMENT 'the creation date 20260101000203, 20260101030405'
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:8020/warehouse/our_data/data_sources';
