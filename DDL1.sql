drop table if exists warehouse.lkp_uds_dim_org;

CREATE  TABLE warehouse.lkp_uds_dim_org
	(
		identity_id string,
		country string,
		market_segment string
)
comment 'Queryable hive table for Organization country and merket segment Details'
;


hive script 

hive-site.xml


cluster