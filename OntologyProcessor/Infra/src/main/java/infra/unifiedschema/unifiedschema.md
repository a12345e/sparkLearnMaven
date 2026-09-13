This is explanation of the fields of a unified table

relating STRUCT<
         value: STRING,
         attributes: MAP<STRING, STRING>
         >

related STRUCT<
        value: STRING,
        attributes: MAP<STRING, STRING>
        >

event STRUCT<
       start:             TIMESTAMP,
       end:               TIMESTAMP,
       count_lower_bound: BIGINT,
       count_upper_bound: BIGINT,
       events_set:        ARRAY<TIMESTAMP>,
       attributes:        MAP<STRING, STRING>
       >

)
PARTITIONED BY (
related_group 'related group number -- 0 means there is no related group',
relation   'relation between the relating object and related object'
           'The relation is a concatenation of
                  '<directed relation name>'
                  '<period type>'
                  '<related category<related type><related subtype><related format>'
                  '<relating category<relating type><relating subtype><relating format>'
           'the four components are joined with _ , so a period type never holds one:'
           'HOUR, FOURHOUR, EIGHTHOUR, DAY, MONTH, QUARTER, HALFYEAR, YEAR, TWOYEARS'
           'called_HOUR_communicationphonemobilemsisdn_entitypersonsubscribername'
product    STRING COMMENT 'string integer of the tree number',
period     STRING COMMENT 'period start and nothing else, never the period type:'
           '2026010100, 2026010108. The period type is the second component of relation,'
           'so a period start on its own does not say whether it is an hour, a day or a year'
dt         'when it is just one original source portion then it is the exact dt of the source.'
           'In all other cases it is just creation time and then it should be like  'created_'<TIMESTAMP>'
)
STORED AS PARQUET
LOCATION 'hdfs://localhost:8020/warehouse/our_data/data_sources';
