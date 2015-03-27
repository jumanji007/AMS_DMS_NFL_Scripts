impala-shell -i pxpappa004 -q "use bihp_sand;
	refresh STG_AMSLOGFILE_EVENT_ALLEVENTS_PAR_CX;
	invalidate metadata nfl_chan_obj_mapping;
	refresh nfl_chan_obj_mapping;
	set PARQUET_COMPRESSION_CODEC=snappy;
	set PARQUET_FILE_SIZE=1073741824;
	DROP TABLE IF EXISTS event_allevents_nfl_join_cx;
	CREATE TABLE IF NOT EXISTS event_allevents_nfl_join_cx
	STORED AS PARQUET
	AS
	SELECT 
	a.*,b.*
	FROM
	stg_amslogfile_event_allevents_par_cx a
	INNER JOIN
	nfl_chan_obj_mapping b
	on cast (a.channel as bigint) = cast (b.chan_obj_id as bigint)
	;"
