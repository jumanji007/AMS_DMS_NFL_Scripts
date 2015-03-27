#!/bin/sh
load_date=$1
impala-shell -i pxpappa004 -q "use bihp_sand;
	refresh STG_AMSLOGFILE_EVENT_ALLEVENTS_PAR_CX;
	invalidate metadata nfl_chan_obj_mapping;
	refresh nfl_chan_obj_mapping;
	invalidate metadata nfl_cust_list;
	refresh nfl_cust_list;
	set PARQUET_COMPRESSION_CODEC=snappy;
	set PARQUET_FILE_SIZE=1073741824;
	DROP TABLE IF EXISTS event_allevents_non_nfl_join_cx;
	CREATE TABLE IF NOT EXISTS event_allevents_non_nfl_join_cx
	STORED AS PARQUET
	AS
	SELECT 
	a.*,b.*,c.*
	FROM
	(
		select * from stg_amslogfile_event_allevents_par_cx 
		where event_date_local = '${load_date}'
	) a
	INNER JOIN
	(
		select cust_acct_num as nfl_cust_acct_number from nfl_cust_list
	) b
	on cast (a.cust_acct_num as bigint) = cast (b.nfl_cust_acct_number as bigint)
	LEFT OUTER JOIN
	(
		select * from nfl_chan_obj_mapping
	) c
	on cast (a.channel as bigint) = cast (c.chan_obj_id as bigint)
	where chan_obj_id is null
	;"
