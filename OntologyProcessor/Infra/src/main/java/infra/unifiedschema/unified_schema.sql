CREATE EXTERNAL TABLE ontology.unified_basic(
  related   STRUCT<
              value:      STRING,              -- related object value
              attributes: MAP<STRING, STRING>  -- attributes of the related object when there is more required information
            > COMMENT 'the related object',
  relating  STRUCT<
              value:      STRING,              -- relating object value
              attributes: MAP<STRING, STRING>  -- attributes of the relating object when there is more required information
            > COMMENT 'the relating object',
  event     STRUCT<
              `start`:           TIMESTAMP,           -- start time
              `end`:             TIMESTAMP,           -- end time
              count_lower_bound: BIGINT,              -- lower bound of the number of events
              count_upper_bound: BIGINT,              -- upper bound of the number of events
              events_set:        ARRAY<TIMESTAMP>,    -- the event set
              attributes:        MAP<STRING, STRING>  -- attributes of the event
            > COMMENT 'the events of the relation'
)
PARTITIONED BY (
  related_group STRING COMMENT 'related group number -- 0 means there is no related group',
  relation      STRING COMMENT 'relation between the relating object and related object. The relation is a concatenation of <directed relation name>_<period_type>_<related element <category><type><subtype><format>>_<relating element <category><type><subtype><format>>',
  product       STRING COMMENT 'string integer of the tree number',
  period        STRING COMMENT 'period start: TIMESTAMP',
  dt            STRING COMMENT 'when it is just one original source portion then it is the exact dt of the source. In all other cases it is just creation time and then it should be like created_<TIMESTAMP>'
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:8020/warehouse/our_data/data_sources';
