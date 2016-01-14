DROP TABLE IF EXISTS warehouse.ccmusg_snapshot_funnel;

CREATE TABLE warehouse.ccmusg_snapshot_funnel
(
	country_code string,
	entitlement_type string,
	route_to_market string,
	skill string,
	job string,
	purpose string,
	unqualified_members bigint,
	qualified_members bigint,
	paid_members bigint,
	retained_members bigint,
	f2p_members bigint
)

PARTITIONED BY (period_name_desc string)

STORED AS RCFILE;
