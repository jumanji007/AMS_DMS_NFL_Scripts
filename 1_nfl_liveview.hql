SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
SET mapred.output.compress=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.map.output.compress=true;
SET mapred.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.exec.compress.intermediate=true;
SET mapred.max.split.size=256000000;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapreduce.output.fileoutputformat.compress=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapreduce.reduce.java.opts='-Djava.net.preferIPv4Stack=true -Xmx13000m';
SET mapreduce.reduce.memory.mb=16000;
-- SET mapred.reduce.tasks=110;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=1573741824;
SET hive.merge.smallfiles.avgsize=1573741824;
SET hive.exec.max.created.files=300000;           

DROP TABLE IF EXISTS bihp_sand.cx_nfl_liveview;
CREATE TABLE IF NOT EXISTS bihp_sand.cx_nfl_liveview
STORED AS SEQUENCEFILE
AS
SELECT 
 event_date_local,
 cust_acct_num,
 major_chan_num as channel_id,
 collect_set(chan_long_name) as channel_array,
 collect_set(cast(cast(diff_duration as bigint)/60 as double)) as duration_array,
 count(*) as count_channel_id
from
  bihp_sand.event_allevents_nfl_join_cx
WHERE
    event_type = 'LiveViewEvent'
GROUP BY 
 event_date_local,
 cust_acct_num,
 major_chan_num
ORDER BY 
 event_date_local,
 cust_acct_num,
 channel_id;
