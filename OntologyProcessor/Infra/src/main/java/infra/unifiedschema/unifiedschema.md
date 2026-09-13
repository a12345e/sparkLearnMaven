This is explanation of the fields of a unified table

relating STRUCT<
         value: STRING,
         attributes: MAP<STRING, STRING>
         >

related STRUCT<
        value: STRING,
        attributes: MAP<STRING, STRING>
        >

time STRUCT<
    start: TIMESTAMP,
    end:   TIMESTAMP,

event STRUCT<
       lower_bound: BIGINT,
       upper_bound: BIGINT,
       events_set: ARRAY<TIMESTAMP>,
       attributes: MAP<STRING, STRING>>
           
)
PARTITIONED BY (
relation   'relation between the relating object and related object'
           'The relation is a concatenation of 
                  '<related category<related type><related subtype><related format>'
                  '<directed relation name>'
                  '<relating category<relating type><relating subtype><relating format>'
product    STRING COMMENT 'string integer of the tree number',
period     STRING COMMENT 'period start:  hour_2026010100, FOUR_HOUR_2026010108, MONTH_2026010100',
dt         'when it is just one original source portion then it is the exact dt of the source.'
           'In all other cases it is just creation time and then it should be like  'created_'<TIMESTAMP>'
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:8020/warehouse/our_data/data_sources';
