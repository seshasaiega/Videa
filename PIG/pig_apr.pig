register /home/phdftpuser/project/cave/dst/udf/roundEval.jar;

AF = LOAD '/dev/cave/dst/apr/hawq/v_dst_mst_apr_01' USING PigStorage('|') as ( super_sheet_date:chararray, fund_code:int, customer_account_number:chararray, total_shares:chararray, net_asset_value:chararray, proc_id:chararray, insert_ts:chararray);

A = FILTER AF BY fund_code == 731;

PF = LOAD '/dev/prod_hub/hawq/v_mutl_fund_shr' USING PigStorage('|') as (as_of_dt:chararray, shr_rcd_id:chararray, fund_shr_id:int, fund_shr_nm:chararray, fund_shr_clas_cd:chararray, fund_shr_clas_nm:chararray, ims_clas_cd:chararray, fund_shr_trk_id:chararray, fund_shr_rgstrd_cusip_id:chararray, fund_shr_dummy_cusip_id:chararray, ici_shr_cls_b:chararray, mstar_fund_shr_id:chararray, mstar_load_wav_fund_shr_id:chararray, lipr_fund_shr_id:chararray, fund_shr_type_cd:chararray, fund_shr_type_nm:chararray, fund_purp_type_cd:chararray, fund_purp_type_nm:chararray, fund_shr_stat_cd:chararray, fund_shr_stat_nm:chararray, fund_shr_incept_dt:chararray, fund_shr_cmncmt_dt:chararray, fund_shr_clos_dt:chararray, cmsnbl_fl:chararray, max_sales_ld_am:chararray, avail_to_pub_fl:chararray, seed_mny_fl:chararray, seed_mny_am:double, svc_fee_fl:chararray, trsf_agcy_stat_cd:chararray, trsf_agcy_stat_nm:chararray, cef_nav:double, cef_incept_shr_prc:double, cef_alt_trsf_fund_shr_id:chararray, ims_acct_id:chararray, fund_fam_rcd_id:chararray, fund_fam_nm:chararray, fund_fam_nbr_cd:chararray, fund_fam_char_cd:chararray, fund_fam_shrt_nm:chararray, prod_id:chararray, prod_type_cd:chararray, prod_type_nm:chararray, rcd_strt_dt:chararray, rcd_end_dt:chararray, last_chg_dt:chararray, hdfs_insrt_proc_id:chararray, hdfs_insrt_ts:chararray);


CF = LOAD '/dev/prod_hub/hawq/clos_fund_l' USING PigStorage('|') as ( as_of_dt:chararray, fund_cd:int, pay_svc_fee_fl:chararray, last_upd_proc_id:int , last_upd_ts:chararray, fund_fam_nm:chararray, hdfs_proc_id:chararray, hdfs_insrt_ts:chararray);


F = FILTER PF BY rcd_end_dt == 'null' or rcd_end_dt is NULL;


PB = FOREACH ( JOIN F by fund_shr_id RIGHT OUTER, A by fund_code) GENERATE A::super_sheet_date,A::fund_code,A::customer_account_number,A::total_shares,A::net_asset_value ,(A::fund_code==990 ? 'Partnership' : F::fund_shr_nm) as FUND_FAM_NM,'APR','DST_TA2000_FANMAIL',A::proc_id,A::insert_ts ;


PB1 = FOREACH ( JOIN CF by fund_cd RIGHT OUTER, PB by fund_code) GENERATE A::super_sheet_date,A::fund_code,A::customer_account_number,A::total_shares,A::net_asset_value ,roundEval.Round(A::total_shares,A::net_asset_value,2) as AM_2,roundEval.Round(A::total_shares,A::net_asset_value,4) as AM_4,(PB::FUND_FAM_NM is NULL ? CF::fund_fam_nm : PB::FUND_FAM_NM) as FUND_FAM_NM,'APR','DST_TA2000_FANMAIL',A::proc_id,A::insert_ts ;


B = FOREACH PB1 GENERATE super_sheet_date,fund_code,customer_account_number,total_shares,net_asset_value ,AM_2 ,AM_4,(FUND_FAM_NM is NULL ? 'NA' : FUND_FAM_NM) as FUND_FAM_NM,'APR','DST_TA2000_FANMAIL',A::proc_id,A::insert_ts ;


STORE B INTO '/dev/cave/dst/apr/transform/v_dst_mst_apr_01' USING PigStorage('|');

D = FOREACH (GROUP B ALL) GENERATE COUNT(B);

STORE D INTO '/dev/cave/dst/apr/pigscript/tranform_count';
