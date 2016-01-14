insert overwrite table warehouse.lkp_uds_dim_org
select 			
	sub3.org,
	sub3.country,
	sub3.market
from (
		select 
			sub1.org1 as org,
			sub1.country1 as country,
			sub1.market1 as market
		from(
				select 
					org.identity_id as org1,
					org.country as country1,
					org.market_segment as market1,
					stg.identity_id as org2,
					stg.country as country2,
					stg.market_segment as market2
						from 
							warehouse.lkp_uds_dim_org org 
							FULL OUTER JOIN
							warehouse.stg_lkp_uds_dim_org stg
							ON org.identity_id=stg.identity_id
			)sub1
			
		where org2 IS NULL

union all
  
		select 
			sub2.org2 as org,
			sub2.country2 as country,
			sub2.market2 as market
		from(
				select 
					org.identity_id as org1,
					org.country as country1,
					org.market_segment as market1,
					stg.identity_id as org2,
					stg.country as country2,
					stg.market_segment as market2
						from 
							warehouse.lkp_uds_dim_org org
							FULL OUTER JOIN
							warehouse.stg_lkp_uds_dim_org stg
							ON org.identity_id=stg.identity_id
			)sub2
		where org2 IS NOT NULL
) sub3;