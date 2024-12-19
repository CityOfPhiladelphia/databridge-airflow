# update_pin_master:
upsert_pin_master_sql = '''
insert into {pin_master_table_schema_name} (pin 
 ,opa_account_num 
 ,mapreg 
 ,pwd_parcel_id 
 ,tags 
 ,pin_type 
 ,pin_status
)
select 
 pin 
 ,opa_account_num 
 ,mapreg 
 ,pwd_parcel_id 
 ,tags 
 ,pin_type 
 ,pin_status
from {pin_master_view_schema_name}
on conflict on constraint pin_master_pin_pkey
do
update
set 
 pin = EXCLUDED.pin
 ,opa_account_num = EXCLUDED.opa_account_num 
 ,mapreg = EXCLUDED.mapreg 
 ,pwd_parcel_id = EXCLUDED.pwd_parcel_id 
 ,tags = EXCLUDED.tags 
 ,pin_type = EXCLUDED.pin_type 
 ,pin_status = EXCLUDED.pin_status
;
'''


# insert DOR records into stage_parcel:
upsert_er_stage_transaction_sql = '''
insert into property.er_stage_transaction
(title, transfer_date, document_date, recording_date, receipt_date, document_type, workflow_type, number_of_parcels, partial_interest, dor_received_date)
select title, transfer_date, document_date, recording_Date, receipt_date, document_type, workflow_type, number_of_parcels, partial_interest, dor_received_date
from property.vw_er_stage_transaction_v4
on conflict on constraint er_stage_transaction_title_pkey
do
update
set
title = EXCLUDED.title,
transfer_date =  EXCLUDED.transfer_date,
document_date =  EXCLUDED.document_date,
recording_date =  EXCLUDED.recording_date,
receipt_date =  EXCLUDED.receipt_date,
document_type =  EXCLUDED.document_type,
workflow_type =  EXCLUDED.workflow_type,
number_of_parcels =  EXCLUDED.number_of_parcels,
partial_interest =  EXCLUDED.partial_interest,
dor_received_date =  EXCLUDED.dor_received_date
;
'''

upsert_er_stage_parties_sql = '''
insert into property.er_stage_parties (title, name, role, instance)
select title::bigint as title, name, role, instance::integer as instance
from property.vw_er_stage_parties_v3
on conflict on constraint er_stage_parties_title_role_instance_pkey
do
update
set
title = EXCLUDED.title,
name = EXCLUDED.name,
role = EXCLUDED.role,
instance = EXCLUDED.instance
;
'''
upsert_er_stage_parcel_sql = '''
insert into property.er_stage_parcel (title,instance,record_type,pin,concatenated_address,base_address,house_number,house_num_suffix,house_num_range,street_dir_suffix,street_name,street_type,street_dir,condo_unit,legal_remarks,condo_name,reg_map_id,received_date,resolution_type)
select
title
,instance
,record_type
,pin
,concatenated_address
,base_address
,house_number
,house_num_suffix
,house_num_range
,street_dir_suffix
,street_name
,street_type
,street_dir
,condo_unit
,legal_remarks
,condo_name
,reg_map_id
,received_date
,resolution_type
from property.vw_er_stage_parcel_v4
on conflict on constraint er_stage_parcel_title_instance_pkey
do
update
set
title = EXCLUDED.title
,instance = EXCLUDED.instance
,record_type = EXCLUDED.record_type
,pin = EXCLUDED.pin
,concatenated_address = EXCLUDED.concatenated_address
,base_address = EXCLUDED.base_address
,house_number = EXCLUDED.house_number
,house_num_suffix = EXCLUDED.house_num_suffix
,house_num_range = EXCLUDED.house_num_range
,street_dir_suffix = EXCLUDED.street_dir_suffix
,street_name = EXCLUDED.street_name
,street_type = EXCLUDED.street_type
,street_dir = EXCLUDED.street_dir
,condo_unit = EXCLUDED.condo_unit
,legal_remarks = EXCLUDED.legal_remarks
,condo_name = EXCLUDED.condo_name
,reg_map_id = EXCLUDED.reg_map_id
,received_date = EXCLUDED.received_date
,resolution_type = EXCLUDED.resolution_type
;
'''
# upsert_er_stage_parcel_sql = '''
# insert into property.er_stage_parcel (title,instance,record_type,pin,concatenated_address,base_address,house_number,house_num_suffix,house_num_range,street_dir_suffix,street_name,street_type,street_dir,condo_unit,legal_remarks,condo_name,reg_map_id,received_date)
# select
# title
# ,instance
# ,record_type
# ,pin
# ,concatenated_address
# ,base_address
# ,house_number
# ,house_num_suffix
# ,house_num_range
# ,street_dir_suffix
# ,street_name
# ,street_type
# ,street_dir
# ,condo_unit
# ,legal_remarks
# ,condo_name
# ,reg_map_id
# ,received_date
# from property.vw_er_stage_parcel_v4
# on conflict on constraint er_stage_parcel_title_instance_pkey
# do
# update
# set
# title = EXCLUDED.title
# ,instance = EXCLUDED.instance
# ,record_type = EXCLUDED.record_type
# ,pin = EXCLUDED.pin
# ,concatenated_address = EXCLUDED.concatenated_address
# ,base_address = EXCLUDED.base_address
# ,house_number = EXCLUDED.house_number
# ,house_num_suffix = EXCLUDED.house_num_suffix
# ,house_num_range = EXCLUDED.house_num_range
# ,street_dir_suffix = EXCLUDED.street_dir_suffix
# ,street_name = EXCLUDED.street_name
# ,street_type = EXCLUDED.street_type
# ,street_dir = EXCLUDED.street_dir
# ,condo_unit = EXCLUDED.condo_unit
# ,legal_remarks = EXCLUDED.legal_remarks
# ,condo_name = EXCLUDED.condo_name
# ,reg_map_id = EXCLUDED.reg_map_id
# ,received_date = EXCLUDED.received_date
# ;
# '''


upsert_er_stage_parcel_pin_matching_sql = '''
insert into {er_stage_parcel_pin_matching_table_schema_name} (pin	
	,pin_type	
	,tags	
	,opa_account_num	
	,title	
	,instance	
	,document_date	
	,recording_date	
	,etl_modified_date	
	,reg_map_id	
	,base_address	
	,concatenated_address	
	,house_number	
	,house_num_suffix	
	,house_num_range	
	,street_dir	
	,street_name	
	,street_type	
	,street_dir_suffix	
	,condo_unit	
	,condo_name	
	,rtt_summary_base_address	
	,rtt_summary_unit_num	
	,dor_parcel_base_address	
	,dor_parcel_unit_num	
	,dor_parcel_cleanup_base_address	
	,dor_parcel_cleanup_unit_num	
	,pwd_parcel_base_address	
	,pwd_parel_unit_num	
	,cama_base_address	
	,cama_unit_num	
	,brt_properties_base_address	
	,brt_properties_unit_num	
	,tips_base_address	
	,tips_unit_num	
	,grantor_grantee_match	
	,grantors	
	,grantees	
	,rtt_title	
	,rtt_summary_owners	
	,cama_title	
	,cama_owners	
	,tips_title	
	,tips_owners	
	,permit_number	
	,permit_issue_date	
	,permit_description	
	,workflow_indicator	
	,is_parent
	,splitno	
	,rtt_owner_match	
	,cama_owner_match	
	,tips_owner_match	
	,rtt_address_match	
	,cama_address_match	
	,tips_address_match	
	,number_of_parcels	
	,document_type	
	,no_address	
	,no_mapreg	
	,num_parcels_w_address	
	,num_parcels_w_mapreg	
	,intersecting_seg_id	
	,research_tags	
	,match_type	
	,in_easement	
	,in_row	
	,pcu_id
        ,std_street_address
	)
	select
	pin	
	,pin_type	
	,tags	
	,opa_account_num	
	,title	
	,instance	
	,document_date	
	,recording_date	
	,etl_modified_date	
	,reg_map_id	
	,base_address	
	,concatenated_address	
	,house_number	
	,house_num_suffix	
	,house_num_range	
	,street_dir	
	,street_name	
	,street_type	
	,street_dir_suffix	
	,condo_unit	
	,condo_name	
	,rtt_summary_base_address	
	,rtt_summary_unit_num	
	,dor_parcel_base_address	
	,dor_parcel_unit_num	
	,dor_parcel_cleanup_base_address	
	,dor_parcel_cleanup_unit_num	
	,pwd_parcel_base_address	
	,pwd_parel_unit_num	
	,cama_base_address	
	,cama_unit_num	
	,brt_properties_base_address	
	,brt_properties_unit_num	
	,tips_base_address	
	,tips_unit_num	
	,grantor_grantee_match	
	,grantors	
	,grantees	
	,rtt_title	
	,rtt_summary_owners	
	,cama_title	
	,cama_owners	
	,tips_title	
	,tips_owners	
	,permit_number	
	,permit_issue_date	
	,permit_description	
	,workflow_indicator	
	,is_parent
	,splitno	
	,rtt_owner_match	
	,cama_owner_match	
	,tips_owner_match	
	,rtt_address_match	
	,cama_address_match	
	,tips_address_match	
	,number_of_parcels	
	,document_type	
	,no_address	
	,no_mapreg	
	,num_parcels_w_address	
	,num_parcels_w_mapreg	
	,intersecting_seg_id	
	,research_tags	
	,match_type	
	,in_easement	
	,in_row	
	,pcu_id
        ,std_street_address
	from {er_stage_parcel_pin_matching_view_schema_name}
	on conflict on constraint er_stage_parcel_pin_matching_title_instance_pkey
do
update
set
         pin = EXCLUDED.pin	
	,pin_type = EXCLUDED.pin_type	
	,tags = EXCLUDED.tags
	,opa_account_num = EXCLUDED.opa_account_num	
	,title = EXCLUDED.title
	,instance = EXCLUDED.instance
	,document_date = EXCLUDED.document_date
	,recording_date = EXCLUDED.recording_date
	,etl_modified_date = EXCLUDED.etl_modified_date
	,reg_map_id = EXCLUDED.reg_map_id
	,base_address = EXCLUDED.base_address
	,concatenated_address = EXCLUDED.concatenated_address
	,house_number = EXCLUDED.house_number
	,house_num_suffix = EXCLUDED.house_num_suffix
	,house_num_range = EXCLUDED.house_num_range
	,street_dir = EXCLUDED.street_dir
	,street_name = EXCLUDED.street_name
	,street_type = EXCLUDED.street_type
	,street_dir_suffix = EXCLUDED.street_dir_suffix
	,condo_unit = EXCLUDED.condo_unit
	,condo_name = EXCLUDED.condo_name
	,rtt_summary_base_address = EXCLUDED.rtt_summary_base_address	
	,rtt_summary_unit_num = EXCLUDED.rtt_summary_unit_num
	,dor_parcel_base_address = EXCLUDED.dor_parcel_base_address
	,dor_parcel_unit_num = EXCLUDED.dor_parcel_unit_num
	,dor_parcel_cleanup_base_address = EXCLUDED.dor_parcel_cleanup_base_address	
	,dor_parcel_cleanup_unit_num = EXCLUDED.dor_parcel_cleanup_unit_num
	,pwd_parcel_base_address = EXCLUDED.pwd_parcel_base_address
	,pwd_parel_unit_num = EXCLUDED.pwd_parel_unit_num
	,cama_base_address = EXCLUDED.cama_base_address
	,cama_unit_num = EXCLUDED.cama_unit_num
	,brt_properties_base_address = EXCLUDED.brt_properties_base_address	
	,brt_properties_unit_num = EXCLUDED.brt_properties_unit_num
	,tips_base_address = EXCLUDED.tips_base_address
	,tips_unit_num = EXCLUDED.tips_unit_num
	,grantor_grantee_match = EXCLUDED.grantor_grantee_match
	,grantors = EXCLUDED.grantors
	,grantees = EXCLUDED.grantees
	,rtt_title = EXCLUDED.rtt_title
	,rtt_summary_owners = EXCLUDED.rtt_summary_owners
	,cama_title = EXCLUDED.cama_title
	,cama_owners = EXCLUDED.cama_owners
	,tips_title = EXCLUDED.tips_title
	,tips_owners = EXCLUDED.tips_owners
	,permit_number = EXCLUDED.permit_number
	,permit_issue_date = EXCLUDED.permit_issue_date
	,permit_description = EXCLUDED.permit_description
	,workflow_indicator = EXCLUDED.workflow_indicator
	,is_parent = EXCLUDED.is_parent
	,splitno = EXCLUDED.splitno
	,rtt_owner_match = EXCLUDED.rtt_owner_match
	,cama_owner_match = EXCLUDED.cama_owner_match
	,tips_owner_match = EXCLUDED.tips_owner_match
	,rtt_address_match = EXCLUDED.rtt_address_match
	,cama_address_match = EXCLUDED.cama_address_match
	,tips_address_match = EXCLUDED.tips_address_match
	,number_of_parcels = EXCLUDED.number_of_parcels
	,document_type = EXCLUDED.document_type
	,no_address = EXCLUDED.no_address
	,no_mapreg = EXCLUDED.no_mapreg
	,num_parcels_w_address = EXCLUDED.num_parcels_w_address	
	,num_parcels_w_mapreg = EXCLUDED.num_parcels_w_mapreg
	,intersecting_seg_id = EXCLUDED.intersecting_seg_id
	,research_tags = EXCLUDED.research_tags
	,match_type = EXCLUDED.match_type
	,in_easement = EXCLUDED.in_easement
	,in_row = EXCLUDED.in_row
	,pcu_id = EXCLUDED.pcu_id
        ,std_street_address = EXCLUDED.std_street_address
;
'''

delete_er_stage_parcel_pin_matching_sql = '''
delete from {er_stage_parcel_pin_matching_table_schema_name} esppm
using {er_stage_parcel_table_schema_name} esp
where esppm.title != esp.title and esppm.instance != esppm.instance
;
'''

upsert_pin_source_address_plus_std_sql = '''
insert into {pin_source_address_plus_std_table_schema_name} (pin_type	
	,pin_status	
	,pin	
	,address_source	
	,street_address	
	,base_address	
	,address_low	
	,address_low_suffix	
	,address_low_frac	
	,address_high	
	,address_high_suffix	
	,address_high_frac	
	,street_code	
	,street_predir	
	,street_name	
	,street_suffix	
	,street_postdir	
	,unit_type	
	,unit_num
	,std_address_low
	,std_address_low_suffix
	,std_address_high
	,std_street_predir
	,std_street_name
	,std_street_suffix
	,std_street_postdir
	,std_unit_type
	,std_unit_num
	,std_street_address
	,std_street_code
	,std_seg_id
	,cl_addr_match
        ,std_zipcode
        ,std_zip4
        ,std_address_low_fractional
        ,std_base_address)
select 
	pin_type	
	,pin_status	
	,pin	
	,address_source	
	,street_address	
	,base_address	
	,address_low	
	,address_low_suffix	
	,address_low_frac	
	,address_high	
	,address_high_suffix	
	,address_high_frac	
	,street_code	
	,street_predir	
	,street_name	
	,street_suffix	
	,street_postdir	
	,unit_type	
	,unit_num
	,std_address_low
	,std_address_low_suffix
	,std_address_high
	,std_street_predir
	,std_street_name
	,std_street_suffix
	,std_street_postdir
	,std_unit_type
	,std_unit_num
	,std_street_address
	,std_street_code
	,std_seg_id
	,cl_addr_match
        ,std_zipcode
        ,std_zip4
        ,std_address_low_fractional
        ,std_base_address

from {pin_source_address_plus_std_view_schema_name}
on conflict on constraint pin_source_address_plus_std_pin_address_source_pkey
do
update
set
	pin_type = EXCLUDED.pin_type	
	,pin_status = EXCLUDED.pin_status
	,pin = EXCLUDED.pin	
	,address_source = EXCLUDED.address_source	
	,street_address = EXCLUDED.street_address	
	,base_address = EXCLUDED.base_address	
	,address_low = EXCLUDED.address_low	
	,address_low_suffix = EXCLUDED.address_low_suffix	
	,address_low_frac = EXCLUDED.address_low_frac	
	,address_high = EXCLUDED.address_high	
	,address_high_suffix = EXCLUDED.address_high_suffix	
	,address_high_frac = EXCLUDED.address_high_frac	
	,street_code = EXCLUDED.street_code	
	,street_predir = EXCLUDED.street_predir	
	,street_name = EXCLUDED.street_name	
	,street_suffix = EXCLUDED.street_suffix	
	,street_postdir = EXCLUDED.street_postdir	
	,unit_type = EXCLUDED.unit_type	
	,unit_num = EXCLUDED.unit_num
	,std_address_low = EXCLUDED.std_address_low
	,std_address_low_suffix = EXCLUDED.std_address_low_suffix
	,std_address_high = EXCLUDED.std_address_high
	,std_street_predir = EXCLUDED.std_street_predir
	,std_street_name = EXCLUDED.std_street_name
	,std_street_suffix = EXCLUDED.std_street_suffix
	,std_street_postdir = EXCLUDED.std_street_postdir
	,std_unit_type = EXCLUDED.std_unit_type
	,std_unit_num = EXCLUDED.std_unit_num
	,std_street_address = EXCLUDED.std_street_address
	,std_street_code = EXCLUDED.std_street_code
	,std_seg_id = EXCLUDED.std_seg_id
	,cl_addr_match = EXCLUDED.cl_addr_match
        ,std_zipcode = EXCLUDED.std_zipcode
        ,std_zip4 = EXCLUDED.std_zip4
        ,std_address_low_fractional = EXCLUDED.std_address_low_fractional
        ,std_base_address = EXCLUDED.std_base_address
'''

upsert_property_deeds_new_sql = '''
insert into {property_deeds_new_table_schema_name} (pin,
document_date,
recording_date,
document_id,
instance,
reg_map_id,
total_consideration,
grantee_1,
grantee_2,
grantor_1,
grantor_2,
care_of_name,
attention_name,
property_address,
street_address,
city,
state,
zip_code,
zip_4,
document_type,
sale_type,
property_count,
workflow_indicator,
partial_interest_indicator,
pin_type,
pin_match_type,
opa_queue,
property_address_std,
cama_owners,
cama_owner_match,
tips_title,
tips_owners,
tips_owner_match,
cama_base_address,
cama_unit_num,
cama_address_match,
prop_on_prior_unprocessed_deed
)
select 
pin,
document_date,
recording_date,
document_id,
instance,
reg_map_id,
total_consideration,
grantee_1,
grantee_2,
grantor_1,
grantor_2,
care_of_name,
attention_name,
property_address,
street_address,
city,
state,
zip_code,
zip_4,
document_type,
sale_type,
property_count,
workflow_indicator,
partial_interest_indicator,
pin_type,
pin_match_type,
opa_queue,
property_address_std,
cama_owners,
cama_owner_match,
tips_title,
tips_owners,
tips_owner_match,
cama_base_address,
cama_unit_num,
cama_address_match,
prop_on_prior_unprocessed_deed
from {property_deeds_view_schema_name}
on conflict on constraint property_deeds_new_document_id_instance_pkey
do
update
set 
pin = EXCLUDED.pin,
document_date = EXCLUDED.document_date,
recording_date = EXCLUDED.recording_date,
document_id = EXCLUDED.document_id,
instance = EXCLUDED.instance,
reg_map_id = EXCLUDED.reg_map_id,
total_consideration = EXCLUDED.total_consideration,
grantee_1 = EXCLUDED.grantee_1,
grantee_2 = EXCLUDED.grantee_2,
grantor_1 = EXCLUDED.grantor_1,
grantor_2 = EXCLUDED.grantor_2,
care_of_name = EXCLUDED.care_of_name,
attention_name = EXCLUDED.attention_name,
property_address = EXCLUDED.property_address,
street_address = EXCLUDED.street_address,
city = EXCLUDED.city,
state = EXCLUDED.state,
zip_code = EXCLUDED.zip_code,
zip_4 = EXCLUDED.zip_4,
document_type = EXCLUDED.document_type,
sale_type = EXCLUDED.sale_type,
property_count = EXCLUDED.property_count,
workflow_indicator = EXCLUDED.workflow_indicator,
partial_interest_indicator = EXCLUDED.partial_interest_indicator,
pin_type = EXCLUDED.pin_type,
pin_match_type = EXCLUDED.pin_match_type,
opa_queue = EXCLUDED.opa_queue,
property_address_std = EXCLUDED.property_address_std,
cama_owners = EXCLUDED.cama_owners,
cama_owner_match = EXCLUDED.cama_owner_match,
tips_title = EXCLUDED.tips_title,
tips_owners = EXCLUDED.tips_owners,
tips_owner_match = EXCLUDED.tips_owner_match,
cama_base_address = EXCLUDED.cama_base_address,
cama_unit_num = EXCLUDED.cama_unit_num,
cama_address_match = EXCLUDED.cama_address_match,
prop_on_prior_unprocessed_deed = EXCLUDED.prop_on_prior_unprocessed_deed
'''

# insert DOR records into stage_transaction:
stage_transaction_insert_dor_stmt = '''
    insert into property.stage_transaction{test_suffix} (
        with opa_queue_prep as (
            select distinct r_num, max(opa_queue) as opa_queue from property.vw_stage_parcel_matching{test_suffix} group by r_num
        )
	select distinct st.r_num, st.transfer_date, st.document_date, st.recording_date, st.receipt_date,
	st.document_type, st.workflow_type, opp.opa_queue, '' as mapping_queue, st.number_of_parcels,
        upper(st.partial_interest) as partial_interest, dor_received_date, st.status
	from (
	    select * from property.vw_stage_transaction{test_suffix} where upper(document_type) like '%DEED%'
	) st
        inner join (
			select r_num, transfer_date, document_date, recording_date, receipt_date, document_type, workflow_type, number_of_parcels, coalesce(upper(partial_interest), '')
                        from property.vw_stage_transaction{test_suffix} where upper(document_type) like '%DEED%'
			except
			select r_num, transfer_date, document_date, recording_date, receipt_date, document_type, workflow_type, number_of_parcels, coalesce(upper(partial_interest), '')
                        from property.stage_transaction{test_suffix}
		) new_transactions on new_transactions.r_num = st.r_num
	left join opa_queue_prep opp on opp.r_num = st.r_num
	order by r_num
)
'''

upsert_deed_feed_for_pwd_sql = '''
insert into water.deed_feed_for_pwd (title,
instance,
document_type, 
document_date, 
recording_date, 
concatenated_address, 
reg_map_id, 
permit_number, 
permit_issue_date, 
permit_description, 
splitno,
shape,
property_count 
)
select 
title,
instance,
document_type, 
document_date, 
recording_date, 
concatenated_address, 
reg_map_id, 
permit_number, 
permit_issue_date, 
permit_description, 
splitno,
shape,
property_count 
from water.deed_feed_for_pwd
on conflict on constraint deed_feed_for_pwd_title_instance_pkey
do
update
set 
title=EXCLUDED.title,
instance=EXCLUDED.instance,
document_type=EXCLUDED.document_type, 
document_date=EXCLUDED.document_date, 
recording_date=EXCLUDED.recording_date, 
concatenated_address=EXCLUDED.concatenated_address, 
reg_map_id=EXCLUDED.reg_map_id, 
permit_number=EXCLUDED.permit_number, 
permit_issue_date=EXCLUDED.permit_issue_date, 
permit_description=EXCLUDED.permit_description, 
splitno=EXCLUDED.splitno,
shape=EXCLUDED.shape,
property_count=EXCLUDED.property_count 
'''


# # Update stage transaction status for records sent to opa queue:
# update_stage_transaction_status_for_opa_queued_records_stmt = '''
#     update property.stage_transaction{test_suffix} set status = 'opa queue'
#     where status = 'pin matching' and r_num in
#     (
#         select distinct r_num from property.vw_new_records_for_cama{test_suffix}
#     )

# '''


# Update stage transaction status for pinned records:
update_stage_transaction_status_for_pinned_records_stmt = '''
    update property.stage_transaction{test_suffix} set status = 'pinned'
    where status = 'opa queue' and r_num in
    (
        select distinct r_num from property.vw_update_pinned_stage_parcels{test_suffix}
    )

'''


# set research queue for parcels with PIN tags:
research_queue_pin_tags_stmt = '''
    update property.stage_transaction{test_suffix} set mapping_queue = 'research', status = 'research queue' 
    where r_num in ( select distinct sp.r_num from (
        select *
	from property.stage_parcel{test_suffix}
	where pcu_id is not null and record_type = 'OPA'
	) sp
        inner join property.stage_transaction{test_suffix} st on st.r_num = sp.r_num and st.status = 'pinned' 
        inner join property.pin_master pm on pm.pin = sp.pin and pm.tags in (
			'Block Remap', 'Back Lot', 'Fairmount', 'Waterfront', 'Missing DOR',
			'DOR in ROW', 'Missing PWD', 'Adjacent Overlap', 'Vacated Road',
			'Rail', 'Consolidation-City Owned', 'Consolidation-Commonwealth',
			'Consolidation-RDA’s', 'Consolidation-Federal', 'Check PWD',
			'Relationship Untagged', 'Elevated Highway', 'Multi-Match', 'Uncertain'
		   )
)
'''

#research queue for parcels which did not have 1-1 matches during pinning:
research_queue_non_one_to_one_parcels_stmt = '''
    update property.stage_transaction{test_suffix} set mapping_queue = 'research', status = 'research queue'
    where r_num in (
        select distinct sp.r_num
        from (
	    select *
	    from property.stage_parcel{test_suffix}
	    where pcu_id is not null and record_type = 'OPA'
	) sp
        inner join property.stage_transaction{test_suffix} st on st.r_num = sp.r_num and st.status = 'pinned' and st.workflow_type = 'DIRECT TRANSFER'
        inner join property.pin_master pm on pm.pin = sp.pin and pm.matchtype != '1-1'
    )
'''

#mapping queue for records not in research queue which are not direct transfers:

mapping_queue_non_dt_parcels_not_in_research_queue_stmt = '''
    update property.stage_transaction{test_suffix} set mapping_queue = 'mapping', status = 'mapping queue'
    where r_num in (
        select distinct sp.r_num from (
	    select *
	    from property.stage_parcel{test_suffix}
	    where pcu_id is not null and record_type = 'OPA'
	    ) sp
        inner join property.stage_transaction{test_suffix} st on st.r_num = sp.r_num and st.status = 'pinned' and
	st.workflow_type != 'DIRECT TRANSFER' and st.mapping_queue in (null, '')
    )
'''

#mapping queue for direct transfers whose PIN has a tag not captured thus far:
mapping_queue_dt_parcels_with_pin_tags_stmt = '''
    update property.stage_transaction{test_suffix} set mapping_queue = 'mapping', status = 'mapping queue'
    where r_num in (
        select distinct sp.r_num from (
            select *
	    from property.stage_parcel{test_suffix}
	    where pcu_id is not null and record_type = 'OPA'
	) sp
        inner join property.stage_transaction{test_suffix} st on st.r_num = sp.r_num and st.status = 'pinned' and
        st.workflow_type = 'DIRECT TRANSFER' and st.mapping_queue in (null, '')
        inner join property.pin_master pm on pm.pin = sp.pin and pm.tags not in (null, '')
    )
'''

#update status to PIN Master for direct transfers whose PIN has no tag:
update_status_to_pin_master_for_dt_with_no_tags_stmt = '''
    update property.stage_transaction{test_suffix} set mapping_queue = 'NA', status = 'PIN master'
    where r_num in (
        select distinct sp.r_num from (
            select *
	    from property.stage_parcel{test_suffix}
	    where pcu_id is not null and record_type = 'OPA'
	) sp
        inner join property.stage_transaction{test_suffix} st on st.r_num = sp.r_num and st.status = 'pinned' and
	st.workflow_type = 'DIRECT TRANSFER' and st.mapping_queue in (null, '')
    )
'''

#update er_stage_transaction status to 'pinned' where status = 'opa_queue' and deed has been completely integrated into cama
# relaxing this so any deed with docid recording in sales.book is considered procssed, 
# as well as any SP/MG deed that has at least one child property that is active 
update_er_stage_transaction_status_pinned_sql = '''update property.er_stage_transaction
set status = 'pinned'
where status = 'opa_queue' and title in (
select distinct document_id from cama.vw_processed_deeds
)
'''

update_stage_transaction_status_deed_parsing_sql = '''update property.er_stage_transaction erst
set status = 'deed_parsing'
where status is null'''

# run pin matching

#update er_stage_parcel pin_type from pin_master for pin from matching
update_er_stage_parcel_pin_type_sql = '''update property.er_stage_parcel esp
set pin = matched.pin,
pin_type = matched.pin_type
from (
        select distinct title, instance, pin,
        pin_type
        from property.er_stage_parcel_pin_matching
        ) matched where matched.title = esp.title and matched.instance = esp.instance and (matched.pin != esp.pin or matched.pin_type != esp.pin_type)
'''

# send to OPA (2 tables, 1 non-queue 9, 1 queue 9)
select_property_deeds_new_non_queue_9_stmt = '''
select pdn.pin,
pdn.document_date,
pdn.recording_date,
pdn.document_id,
pdn.instance,
pdn.reg_map_id,
pdn.total_consideration,
pdn.grantee_1,
pdn.grantee_2,
pdn.grantor_1,
pdn.grantor_2,
pdn.care_of_name,
pdn.attention_name,
pdn.property_address,
pdn.street_address,
pdn.city,
pdn.state,
pdn.zip_code,
pdn.zip_4,
pdn.document_type,
pdn.sale_type,
pdn.property_count,
pdn.workflow_indicator,
pdn.partial_interest_indicator,
pdn.pin_type,
pdn.pin_match_type,
pdn.opa_queue,
pdn.matched_mapreg,
pdn.property_address_std,
pdn.cama_owners,
pdn.cama_owner_match,
pdn.tips_title,
pdn.tips_owners,
pdn.tips_owner_match,
pdn.cama_base_address,
pdn.cama_unit_num,
pdn.cama_address_match,
new_deeds.etl_modified_timestamp
from cama.property_deeds_new pdn
inner join (
select distinct on (document_id, instance) document_id, instance, etl_modified_timestamp
        from (
select (new_val->'document_id')::text::bigint as document_id, (new_val->'instance')::text::integer AS instance, etl_modified_timestamp
from audit.pin_history
where tabname = 'property_deeds_new'
and etl_modified_timestamp > '{last_update_date}'
and lower(operation) != 'delete'
) prep order by document_id, instance, etl_modified_timestamp desc
) new_deeds on new_deeds.document_id = pdn.document_id and new_deeds.instance = pdn.instance and pdn.opa_queue not in (9, 10)
order by document_id, instance
'''

select_property_deeds_new_queue_9_stmt = '''
select pdn.*,
case when ed.document_id is not null then
'http://rec31vprdapp01/recorder/eagleweb/viewDoc.jsp?node=' || ed.document_id
else null end as philadox_uri,
st_astext(espa.shape) as shape,
new_deeds.etl_modified_timestamp
from cama.property_deeds_new pdn
inner join (
select distinct on (document_id, instance) document_id, instance, etl_modified_timestamp
        from (
select (new_val->'document_id')::text::bigint as document_id, (new_val->'instance')::text::integer AS instance, etl_modified_timestamp
from audit.pin_history
where tabname = 'property_deeds_new'
and etl_modified_timestamp > '{last_update_date}'
and lower(operation) != 'delete'
) prep order by document_id, instance, etl_modified_timestamp desc
) new_deeds on new_deeds.document_id = pdn.document_id and new_deeds.instance = pdn.instance and pdn.opa_queue = 9
  left join property.er_stage_parcel_ais espa on espa.concatenated_address = pdn.property_address
  left join dor.er_document ed on ed.title = pdn.document_id
order by document_id, instance
'''


#after sending, update er_stage_parcel sent date
update_er_stage_parcel_sent_date_sql = '''
update property.er_stage_parcel ersp
set sent_date = current_timestamp
from ( select distinct * from (
        select pdn.*
        from cama.property_deeds_new pdn
        inner join property.er_stage_transaction est on est.status = 'deed_parsing'
        and est.title = pdn.document_id
		union
		select pdn.*
		from cama.property_deeds_new pdn
		inner join property.er_stage_parcel esp on esp.title = pdn.document_id  and esp.instance = pdn.instance
		and esp.resolution_type = 'opa-q9-override' and esp.sent_date is null
		) prep 
) sent where sent.document_id = ersp.title and sent.instance = ersp.instance
'''

# Update stage transaction opa_queue for updated property_deeds:
update_er_stage_transaction_opa_queue_sql = '''
update property.er_stage_transaction  est
set opa_queue = prep.opa_Queue
from (select distinct on (document_id, opa_Queue) document_id, opa_queue
from cama.property_deeds_new
order by document_id, opa_queue desc ) prep
where prep.document_id::text = est.title::text and
(est.status = 'deed_parsing' OR (est.status = 'opa_queue' AND est.opa_queue = '9'))
AND lower(est.document_type) !~~ '%misc%'
'''


# & update er_stage_transaction status to 'opa_queue'
update_er_stage_transaction_status_opa_queue_sql = '''
update property.er_stage_transaction erst
set status = 'opa_queue'
where status = 'deed_parsing'
'''


upsert_parcel_cleanup_pin_queue_update_sql = '''
insert into dor.parcel_cleanup_pin_queue_update (
base_address
,brt_properties_base_address
,brt_properties_unit_num
,cama_address_match
,cama_base_address
,cama_owner_match
,cama_owners
,cama_title
,cama_unit_num
,concatenated_address
,condo_name
,condo_unit
,document_date
,document_type
,dor_parcel_base_address
,dor_parcel_cleanup_base_addr
,dor_parcel_cleanup_unit_num
,dor_parcel_unit_num
,etl_modified_date
,grantees
,grantor_grantee_match
,grantors
,house_number
,house_num_range
,house_num_suffix
,in_easement
,in_row
,instance
,intersecting_seg_id
,is_parent
,match_type
,no_address
,no_mapreg
,number_of_parcels
,num_parcels_w_address
,num_parcels_w_mapreg
,opa_account_num
,pcu_id
,permit_description
,permit_issue_date
,pin
,pin_type
,pwd_parcel_base_address
,pwd_parel_unit_num
,recording_date
,reg_map_id
,research_tags
,review_comments
,review_partial_interest
,review_pin
,review_status
,review_transaction_type
,rtt_address_match
,rtt_owner_match
,rtt_summary_base_address
,rtt_summary_owners
,rtt_summary_unit_num
,rtt_title
,street_dir
,street_dir_suffix
,street_name
,street_type
,tags
,tips_address_match
,tips_base_address
,tips_owner_match
,tips_owners
,tips_title
,tips_unit_num
,title
,workflow_indicator
)
select 
base_address
,brt_properties_base_address
,brt_properties_unit_num
,cama_address_match
,cama_base_address
,cama_owner_match
,cama_owners
,cama_title
,cama_unit_num
,concatenated_address
,condo_name
,condo_unit
,document_date
,document_type
,dor_parcel_base_address
,dor_parcel_cleanup_base_addr
,dor_parcel_cleanup_unit_num
,dor_parcel_unit_num
,etl_modified_date
,grantees
,grantor_grantee_match
,grantors
,house_number
,house_num_range
,house_num_suffix
,in_easement
,in_row
,instance
,intersecting_seg_id
,is_parent
,match_type
,no_address
,no_mapreg
,number_of_parcels
,num_parcels_w_address
,num_parcels_w_mapreg
,opa_account_num
,pcu_id
,permit_description
,permit_issue_date
,pin
,pin_type
,pwd_parcel_base_address
,pwd_parel_unit_num
,recording_date
,reg_map_id
,research_tags
,review_comments
,review_partial_interest
,review_pin
,review_status
,review_transaction_type
,rtt_address_match
,rtt_owner_match
,rtt_summary_base_address
,rtt_summary_owners
,rtt_summary_unit_num
,rtt_title
,street_dir
,street_dir_suffix
,street_name
,street_type
,tags
,tips_address_match
,tips_base_address
,tips_owner_match
,tips_owners
,tips_title
,tips_unit_num
,title
,workflow_indicator
from dor.vw_parcel_cleanup_pin_queue_update
on conflict on constraint parcel_cleanup_pin_queue_update_pkey
do
update
set
base_address=EXCLUDED.base_address
,brt_properties_base_address=EXCLUDED.brt_properties_base_address
,brt_properties_unit_num=EXCLUDED.brt_properties_unit_num
,cama_address_match=EXCLUDED.cama_address_match
,cama_base_address=EXCLUDED.cama_base_address
,cama_owner_match=EXCLUDED.cama_owner_match
,cama_owners=EXCLUDED.cama_owners
,cama_title=EXCLUDED.cama_title
,cama_unit_num=EXCLUDED.cama_unit_num
,concatenated_address=EXCLUDED.concatenated_address
,condo_name=EXCLUDED.condo_name
,condo_unit=EXCLUDED.condo_unit
,document_date=EXCLUDED.document_date
,document_type=EXCLUDED.document_type
,dor_parcel_base_address=EXCLUDED.dor_parcel_base_address
,dor_parcel_cleanup_base_addr=EXCLUDED.dor_parcel_cleanup_base_addr
,dor_parcel_cleanup_unit_num=EXCLUDED.dor_parcel_cleanup_unit_num
,dor_parcel_unit_num=EXCLUDED.dor_parcel_unit_num
,etl_modified_date=EXCLUDED.etl_modified_date
,grantees=EXCLUDED.grantees
,grantor_grantee_match=EXCLUDED.grantor_grantee_match
,grantors=EXCLUDED.grantors
,house_number=EXCLUDED.house_number
,house_num_range=EXCLUDED.house_num_range
,house_num_suffix=EXCLUDED.house_num_suffix
,in_easement=EXCLUDED.in_easement
,in_row=EXCLUDED.in_row
,intersecting_seg_id=EXCLUDED.intersecting_seg_id
,is_parent=EXCLUDED.is_parent
,match_type=EXCLUDED.match_type
,no_address=EXCLUDED.no_address
,no_mapreg=EXCLUDED.no_mapreg
,number_of_parcels=EXCLUDED.number_of_parcels
,num_parcels_w_address=EXCLUDED.num_parcels_w_address
,num_parcels_w_mapreg=EXCLUDED.num_parcels_w_mapreg
,opa_account_num=EXCLUDED.opa_account_num
,pcu_id=EXCLUDED.pcu_id
,permit_description=EXCLUDED.permit_description
,permit_issue_date=EXCLUDED.permit_issue_date
,pin=EXCLUDED.pin
,pin_type=EXCLUDED.pin_type
,pwd_parcel_base_address=EXCLUDED.pwd_parcel_base_address
,pwd_parel_unit_num=EXCLUDED.pwd_parel_unit_num
,recording_date=EXCLUDED.recording_date
,reg_map_id=EXCLUDED.reg_map_id
,research_tags=EXCLUDED.research_tags
,review_comments=EXCLUDED.review_comments
,review_partial_interest=EXCLUDED.review_partial_interest
,review_pin=EXCLUDED.review_pin
,review_status=EXCLUDED.review_status
,review_transaction_type=EXCLUDED.review_transaction_type
,rtt_address_match=EXCLUDED.rtt_address_match
,rtt_owner_match=EXCLUDED.rtt_owner_match
,rtt_summary_base_address=EXCLUDED.rtt_summary_base_address
,rtt_summary_owners=EXCLUDED.rtt_summary_owners
,rtt_summary_unit_num=EXCLUDED.rtt_summary_unit_num
,rtt_title=EXCLUDED.rtt_title
,street_dir=EXCLUDED.street_dir
,street_dir_suffix=EXCLUDED.street_dir_suffix
,street_name=EXCLUDED.street_name
,street_type=EXCLUDED.street_type
,tags=EXCLUDED.tags
,tips_address_match=EXCLUDED.tips_address_match
,tips_base_address=EXCLUDED.tips_base_address
,tips_owner_match=EXCLUDED.tips_owner_match
,tips_owners=EXCLUDED.tips_owners
,tips_title=EXCLUDED.tips_title
,tips_unit_num=EXCLUDED.tips_unit_num
,workflow_indicator=EXCLUDED.workflow_indicator
'''


update_er_property_matching_sql = '''
insert into {er_property_matching_table_schema_name}
(title, instance, street_address, pin, opa_account_num, mapreg, grantors, grantees, cama_owners, 
grantor_11_similarity,
grantor_12_similarity,
grantor_21_similarity,
grantor_22_similarity,
grantee_11_similarity,
grantee_12_similarity,
grantee_21_similarity,
grantee_22_similarity,
opa_owner_similarity_score
)
select 
title, instance, street_address, pin, opa_account_num, mapreg, grantors, grantees, cama_owners, 
grantor_11_similarity,
grantor_12_similarity,
grantor_21_similarity,
grantor_22_similarity,
grantee_11_similarity,
grantee_12_similarity,
grantee_21_similarity,
grantee_22_similarity,
opa_owner_similarity_score
from {er_property_matching_view_schema_name} 
on conflict on constraint er_property_matching_title_instance_pkey
do
update
set 
title = EXCLUDED.title, 
instance = EXCLUDED.instance, 
street_address = EXCLUDED.street_address, 
pin = EXCLUDED.pin, 
opa_account_num = EXCLUDED.opa_account_num, 
mapreg = EXCLUDED.mapreg, 
grantors = EXCLUDED.grantors, 
grantees = EXCLUDED.grantees, 
cama_owners = EXCLUDED.cama_owners, 
grantor_11_similarity = EXCLUDED.grantor_11_similarity,
grantor_12_similarity = EXCLUDED.grantor_12_similarity,
grantor_21_similarity = EXCLUDED.grantor_21_similarity,
grantor_22_similarity = EXCLUDED.grantor_22_similarity,
grantee_11_similarity = EXCLUDED.grantee_11_similarity,
grantee_12_similarity = EXCLUDED.grantee_12_similarity,
grantee_21_similarity = EXCLUDED.grantee_21_similarity,
grantee_22_similarity = EXCLUDED.grantee_22_similarity,
opa_owner_similarity_score = EXCLUDED.opa_owner_similarity_score;
 '''

update_er_rtt_complete_sql = '''
BEGIN;
truncate table {er_rtt_complete_table_schema_name};
insert into {er_rtt_complete_table_schema_name} (select * from {er_rtt_complete_view_schema_name});
COMMIT;
 '''

update_deeds_for_revenue_sql = ''' 
insert into {deeds_for_revenue_table_schema_name}(
document_number,
instance,
document_type,
display_date,
street_address,
zip_code,
grantors,
grantees,
cash_consideration,
other_consideration,
total_consideration,
assessed_value,
common_level_ratio,
fair_market_value,
state_tax_amount,
state_tax_percent,
local_tax_amount,
local_tax_percent,
document_date,
condo_name,
unit_num,
address_low,
address_low_suffix,
address_low_frac,
address_high,
street_predir,
street_name,
street_suffix,
street_postdir,
reg_map_id,
opa_account_num,
pin,
legal_remarks,
discrepancy,
property_count,
opa_owner_similarity_score,
document_id,
return2_address1,
return2_address2,
return2_city,
return2_country,
return2_state,
return2_zip
)
select
document_number,
instance,
document_type,
display_date,
street_address,
zip_code,
grantors,
grantees,
cash_consideration,
other_consideration,
total_consideration,
assessed_value,
common_level_ratio,
fair_market_value,
state_tax_amount,
state_tax_percent,
local_tax_amount,
local_tax_percent,
document_date,
condo_name,
unit_num,
address_low,
address_low_suffix,
address_low_frac,
address_high,
street_predir,
street_name,
street_suffix,
street_postdir,
reg_map_id,
opa_account_num,
pin,
legal_remarks,
discrepancy,
property_count,
opa_owner_similarity_score,
document_id,
return2_address1,
return2_address2,
return2_city,
return2_country,
return2_state,
return2_zip
from {deeds_for_revenue_view_schema_name}
on conflict on constraint deeds_for_revenue_document_number_instance_pkey
do update
set
document_number = EXCLUDED.document_number,
instance = EXCLUDED.instance,
document_type = EXCLUDED.document_type,
display_date = EXCLUDED.display_date,
street_address = EXCLUDED.street_address,
zip_code = EXCLUDED.zip_code,
grantors = EXCLUDED.grantors,
grantees = EXCLUDED.grantees,
cash_consideration = EXCLUDED.cash_consideration,
other_consideration = EXCLUDED.other_consideration,
total_consideration = EXCLUDED.total_consideration,
assessed_value = EXCLUDED.assessed_value,
common_level_ratio = EXCLUDED.common_level_ratio,
fair_market_value = EXCLUDED.fair_market_value,
state_tax_amount = EXCLUDED.state_tax_amount,
state_tax_percent = EXCLUDED.state_tax_percent,
local_tax_amount = EXCLUDED.local_tax_amount,
local_tax_percent = EXCLUDED.local_tax_percent,
document_date = EXCLUDED.document_date,
condo_name = EXCLUDED.condo_name,
unit_num = EXCLUDED.unit_num,
address_low = EXCLUDED.address_low,
address_low_suffix = EXCLUDED.address_low_suffix,
address_low_frac = EXCLUDED.address_low_frac,
address_high = EXCLUDED.address_high,
street_predir = EXCLUDED.street_predir,
street_name = EXCLUDED.street_name,
street_suffix = EXCLUDED.street_suffix,
street_postdir = EXCLUDED.street_postdir,
reg_map_id = EXCLUDED.reg_map_id,
opa_account_num = EXCLUDED.opa_account_num,
pin = EXCLUDED.pin,
legal_remarks = EXCLUDED.legal_remarks,
discrepancy = EXCLUDED.discrepancy,
property_count = EXCLUDED.property_count,
opa_owner_similarity_score = EXCLUDED.opa_owner_similarity_score,
document_id = EXCLUDED.document_id,
return2_address1 = EXCLUDED.return2_address1,
return2_address2 = EXCLUDED.return2_address2,
return2_city = EXCLUDED.return2_city,
return2_country = EXCLUDED.return2_country,
return2_state = EXCLUDED.return2_state,
return2_zip = EXCLUDED.return2_zip
;

'''

get_deeds_for_revenue_updates_for_sftp_sql = '''
with last_sftp_update_ts as (
select max(last_etl_timestamp_processed) as last_update from audit.file_upload_history where schema_name = 'dor' and table_name = 'deeds_for_revenue'
)
select d.document_number
,d.instance
,d.document_type
,d.display_date
,d.street_address
,d.zip_code
,d.grantors
,d.grantees
,d.cash_consideration
,d.other_consideration
,d.total_consideration
,d.assessed_value
,d.common_level_ratio
,d.fair_market_value
,d.state_tax_amount
,d.state_tax_percent
,d.local_tax_amount
,d.local_tax_percent
,d.document_date
,d.condo_name
,d.unit_num
,d.address_low
,d.address_low_suffix
,d.address_low_frac
,d.address_high
,d.street_predir
,d.street_name
,d.street_suffix
,d.street_postdir
,d.reg_map_id
,d.matched_regmap
,d.opa_account_num
,d.pin
,d.legal_remarks
,d.discrepancy
,d.property_count
,d.opa_owner_similarity_score
,d.document_id
,d.return2_address1
,d.return2_address2
,d.return2_city
,d.return2_country
,d.return2_state
,d.return2_zip
from dor.deeds_for_revenue d
join
(select distinct (new_val->'document_number')::text::bigint as document_number, (new_val->'instance')::text::integer as instance
from audit.change_history_for_revenue, last_sftp_update_ts
where schemaname = 'dor' and  tabname = 'deeds_for_revenue' and operation = 'INSERT' and etl_modified_timestamp > coalesce(last_sftp_update_ts.last_update, '-infinity')
) updates on updates.document_number = d.document_number and updates.instance = d.instance
join dor.er_document ed on ed.title = d.document_number and ed.recording_date > '2019-12-09'
'''

get_deeds_for_revenue_for_basis2_updates_for_sftp_sql = '''
with last_sftp_update_ts as (
select max(last_etl_timestamp_processed) as last_update from audit.file_upload_history where schema_name = 'dor' and table_name = 'deeds_for_revenue_for_basis2'
)
select d.document_number
,d.instance
,d.document_type
,d.display_date
,d.street_address
,d.zip_code
,d.grantors
,d.grantees
,d.cash_consideration
,d.other_consideration
,d.total_consideration
,d.assessed_value
,d.common_level_ratio
,d.fair_market_value
,d.state_tax_amount
,d.state_tax_percent
,d.local_tax_amount
,d.local_tax_percent
,d.document_date
,d.condo_name
,d.unit_num
,d.address_low
,d.address_low_suffix
,d.address_low_frac
,d.address_high
,d.street_predir
,d.street_name
,d.street_suffix
,d.street_postdir
,d.reg_map_id
,d.matched_regmap
,d.opa_account_num
,d.pin
,d.legal_remarks
,d.discrepancy
,d.property_count
,d.opa_owner_similarity_score
,d.document_id
,d.return2_address1
,d.return2_address2
,d.return2_city
,d.return2_country
,d.return2_state
,d.return2_zip
from dor.deeds_for_revenue d
join
(select distinct (new_val->'document_number')::text::bigint as document_number, (new_val->'instance')::text::integer as instance
from audit.change_history_for_revenue, last_sftp_update_ts
where schemaname = 'dor' and  tabname = 'deeds_for_revenue' and operation = 'INSERT' and etl_modified_timestamp > coalesce(last_sftp_update_ts.last_update, '-infinity')
) updates on updates.document_number = d.document_number and updates.instance = d.instance
join dor.er_document ed on ed.title = d.document_number and ed.recording_date > '2019-12-09'
'''

update_splcom_for_revenue_sql = '''
insert into cama.splcom_for_revenue
(old_pin, new_pin, old_opa_account_num, new_opa_account_num, tax_year, splcom_num, status, xref_code, record_date, transaction_num)
select
old_pin, new_pin, old_opa_account_num, new_opa_account_num, tax_year, splcom_num, status, xref_code, record_date, transaction_num
from cama.vw_splcom_for_revenue
on conflict on constraint splcom_for_revenue_splcom_num_old_pin_new_pin_pkey
do update
set
old_pin = EXCLUDED.old_pin,
new_pin = EXCLUDED.new_pin,
old_opa_account_num = EXCLUDED.old_opa_account_num,
new_opa_account_num = EXCLUDED.new_opa_account_num,
tax_year = EXCLUDED.tax_year,
splcom_num = EXCLUDED.splcom_num,
status = EXCLUDED.status,
xref_code = EXCLUDED.xref_code,
record_date = EXCLUDED.record_date,
transaction_num = EXCLUDED.transaction_num
;
'''


get_splcom_for_revenue_updates_for_sftp_sql = ''' 
with last_sftp_update_ts as (
select max(last_etl_timestamp_processed) as last_update from audit.file_upload_history where schema_name = 'cama' and table_name = 'splcom_for_revenue'
)
select
s.old_pin,
s.new_pin,
s.old_opa_account_num,
s.new_opa_account_num,
s.tax_year,
s.splcom_num,
status,
xref_code,
record_date,
transaction_num,
updates.etl_modified_timestamp
from cama.splcom_for_revenue s
join
(select distinct on (new_val->'old_pin', new_val->'new_pin', new_val->'splcom_num') replace((new_val->'old_pin')::text, '"', '') as old_pin, 
 replace((new_val->'new_pin')::text, '"', '') as new_pin, 
 (new_val->'splcom_num')::text::integer as splcom_num,
 etl_modified_timestamp
from audit.change_history_for_revenue, last_sftp_update_ts
where schemaname = 'cama' and tabname = 'splcom_for_revenue' 
 and etl_modified_timestamp > coalesce(last_sftp_update_ts.last_update, '-infinity')
order by new_val->'old_pin', new_val->'new_pin', new_val->'splcom_num', etl_modified_timestamp desc nulls last
) updates on updates.old_pin::text = s.old_pin::text and updates.new_pin::text = s.new_pin::text and s.splcom_num = updates.splcom_num

'''

update_assessment_and_property_updates_for_revenue_sql = ''' insert into cama.assessment_and_property_updates_for_revenue (address_high, address_low, address_low_suffix, asmt_exmpt_val, asmt_mkt_val, asmt_tax_val, category_code, 
cert_action_date, cert_reason_code, exempt_building, exempt_code, exempt_land, land_use_code, mailing_address_1, mailing_address_2, mailing_care_of, mailing_city, mailing_state, mailing_street_address, 
mailing_zip, opa_account_num, owner_1, owner_2, pin, street_address, street_code, structure_code, style_code, taxable_building, taxable_land, tax_year, transaction_num, unit_num, zip) select address_high, address_low, 
address_low_suffix, asmt_exmpt_val, asmt_mkt_val, asmt_tax_val, category_code, cert_action_date, cert_reason_code, exempt_building, exempt_code, exempt_land, land_use_code, mailing_address_1, 
mailing_address_2, mailing_care_of, mailing_city, mailing_state, mailing_street_address, mailing_zip, opa_account_num, owner_1, owner_2, pin, street_address, street_code, structure_code, style_code, taxable_building, 
taxable_land, tax_year, transaction_num, unit_num, zip from cama.vw_assessment_and_property_updates_for_revenue on conflict on constraint assessment_and_property_updates_for_revenue_pin_tax_year_pkey do update set 
address_high=EXCLUDED.address_high, address_low=EXCLUDED.address_low, address_low_suffix=EXCLUDED.address_low_suffix, asmt_exmpt_val=EXCLUDED.asmt_exmpt_val, asmt_mkt_val=EXCLUDED.asmt_mkt_val, asmt_tax_val=EXCLUDED.asmt_tax_val, 
category_code=EXCLUDED.category_code, cert_action_date=EXCLUDED.cert_action_date, cert_reason_code=EXCLUDED.cert_reason_code, exempt_building=EXCLUDED.exempt_building, 
exempt_code=EXCLUDED.exempt_code, exempt_land=EXCLUDED.exempt_land, land_use_code=EXCLUDED.land_use_code, mailing_address_1=EXCLUDED.mailing_address_1, mailing_address_2=EXCLUDED.mailing_address_2, 
mailing_care_of=EXCLUDED.mailing_care_of, mailing_city=EXCLUDED.mailing_city, mailing_state=EXCLUDED.mailing_state, mailing_street_address=EXCLUDED.mailing_street_address, mailing_zip=EXCLUDED.mailing_zip, 
opa_account_num=EXCLUDED.opa_account_num, owner_1=EXCLUDED.owner_1, owner_2=EXCLUDED.owner_2, pin=EXCLUDED.pin, street_address=EXCLUDED.street_address, street_code=EXCLUDED.street_code, structure_code=EXCLUDED.structure_code, 
style_code=EXCLUDED.style_code, taxable_building=EXCLUDED.taxable_building, taxable_land=EXCLUDED.taxable_land, tax_year=EXCLUDED.tax_year, transaction_num=EXCLUDED.transaction_num, unit_num=EXCLUDED.unit_num, zip=EXCLUDED.zip;
'''



get_assessment_and_property_updates_for_revenue_for_sftp_sql = '''with last_sftp_update_ts as (
select max(last_etl_timestamp_processed) as last_update from audit.file_upload_history where schema_name = 'cama' and table_name = 'assessment_and_property_updates_for_revenue'
)
select
address_high,
address_low,
address_low_suffix,
asmt_exmpt_val,
asmt_mkt_val,
asmt_tax_val,
category_code,
cert_action_date,
cert_reason_code,
exempt_building,
exempt_code,
exempt_land,
land_use_code,
mailing_address_1,
mailing_address_2,
mailing_care_of,
mailing_city,
mailing_state,
mailing_street_address,
mailing_zip,
opa_account_num,
owner_1,
owner_2,
s.pin,
street_address,
street_code,
structure_code,
style_code,
taxable_building,
taxable_land,
s.tax_year,
transaction_num,
unit_num,
zip,
updates.etl_modified_timestamp
from cama.assessment_and_property_updates_for_revenue s
join
(
select distinct on (new_val->'pin', new_val->'tax_year') replace((new_val->'pin')::text, '"', '') as pin,
 replace((new_val->'tax_year')::text, '"', '') as tax_year,
 etl_modified_timestamp
from audit.change_history_for_revenue, last_sftp_update_ts
where schemaname = 'cama' and tabname = 'assessment_and_property_updates_for_revenue'
 and etl_modified_timestamp > coalesce(last_sftp_update_ts.last_update, '-infinity')
 and operation in ('INSERT', 'UPDATE')
order by new_val->'pin', new_val->'tax_year', etl_modified_timestamp desc nulls last
) updates on updates.pin::text = s.pin::text and updates.tax_year::text = s.tax_year::text 
'''

update_notary_fraud_affidavit_deeds_for_revenue_sql = '''
insert into {notary_fraud_affidavit_deeds_for_revenue_table_schema_name} (nfa_document_number, document_number, document_date, pin)
select nfa_document_number, document_number, document_date, pin
from {notary_fraud_affidavit_deeds_for_revenue_view_schema_name}
on conflict on constraint notary_fraud_affidavit_deeds_for_revenue_document_number_pin_pkey
do update 
set 
nfa_document_number=EXCLUDED.nfa_document_number,
document_number=EXCLUDED.document_number,
document_date=EXCLUDED.document_date,
pin=EXCLUDED.pin
;
'''

get_notary_fraud_affidavit_deeds_for_revenue_updates_for_sftp_sql = '''
with last_sftp_update_ts as (
select max(last_etl_timestamp_processed) as last_update from audit.file_upload_history where schema_name = 'dor' and table_name = 'notary_fraud_affidavit_deeds_for_revenue'
)
select d.*
from dor.notary_fraud_affidavit_deeds_for_revenue d
join
(select distinct (new_val->'document_number')::text::bigint as document_number, (new_val->'pin')::text::integer as pin
from audit.change_history_for_revenue, last_sftp_update_ts
where schemaname = 'dor' and  tabname = 'notary_fraud_affidavit_deeds_for_revenue' and operation = 'INSERT' and etl_modified_timestamp > coalesce(last_sftp_update_ts.last_update, '-infinity')
) updates on updates.document_number = d.document_number and updates.pin = d.pin
join dor.er_document ed on ed.title = d.document_number 

'''

upsert_property_parcel_sql = '''
insert into property.parcel (pin, shape, parcel_source, status)
select distinct pin, shape, parcel_source, status
from property.vw_parcel where pin is not null
on conflict on constraint parcel_pin_source_pkey
do
update
set
 pin = EXCLUDED.pin,
 shape = EXCLUDED.shape,
 parcel_source = EXCLUDED.parcel_source,
 status = 'active'
 ;
'''

update_property_parcel_status_for_inactives_sql = '''
update property.parcel
set status = 'inactive' where (pin, parcel_source) in (
select pin, parcel_source from property.parcel
except
select pin, parcel_source from property.vw_parcel
)
'''

update_dor_rtt_summary_sql = '''
BEGIN;
truncate table dor.rtt_summary;
insert into dor.rtt_summary (
	document_id
	,document_type
	,display_date
	,street_address
	,zip_code
	,grantors
	,grantees
	,cash_consideration
	,other_consideration
	,total_consideration
	,assessed_value
	,common_level_ratio
	,fair_market_value
	,state_tax_amount
	,state_tax_percent
	,local_tax_amount
	,local_tax_percent
	,document_date
	,condo_name
	,unit_num
	,address_low
	,address_low_suffix
	,address_low_frac
	,address_high
	,street_predir
	,street_name
	,street_suffix
	,street_postdir
	,reg_map_id
	,matched_regmap
	,opa_account_num
	,legal_remarks
	,discrepancy
	,property_count
	,shape
	,record_id
	,recording_date
	,receipt_date
	,receipt_num
	,adjusted_assessed_value
	,adjusted_cash_consideration
	,adjusted_fair_market_value
	,adjusted_local_tax_amount
	,adjusted_other_consideration
	,adjusted_state_tax_amount
	,adjusted_total_consideration
	,ward
)
select
	document_id
	,document_type
	,display_date
	,street_address
	,zip_code
	,grantors
	,grantees
	,cash_consideration
	,other_consideration
	,total_consideration
	,assessed_value
	,common_level_ratio
	,fair_market_value
	,state_tax_amount
	,state_tax_percent
	,local_tax_amount
	,local_tax_percent
	,document_date
	,condo_name
	,unit_num
	,address_low
	,address_low_suffix
	,address_low_frac
	,address_high
	,street_predir
	,street_name
	,street_suffix
	,street_postdir
	,reg_map_id
	,matched_regmap
	,opa_account_num
	,legal_remarks
	,discrepancy
	,property_count
	,shape
	,record_id
	,recording_date
	,receipt_date
	,receipt_num
	,adjusted_assessed_value
	,adjusted_cash_consideration
	,adjusted_fair_market_value
	,adjusted_local_tax_amount
	,adjusted_other_consideration
	,adjusted_state_tax_amount
	,adjusted_total_consideration
	,ward
from dor.vw_rtt_summary;
COMMIT;
'''

#update_dor_rtt_summary_sql = '''
#insert into dor.rtt_summary (document_id,
#    document_type,
#    display_date,
#    street_address,
#    zip_code,
#    grantors,
#    grantees,
#    cash_consideration,
#    other_consideration,
#    total_consideration,
#    assessed_value,
#    common_level_ratio,
#    fair_market_value,
#    state_tax_amount,
#    state_tax_percent,
#    local_tax_amount,
#    local_tax_percent,
#    document_date,
#    condo_name,
#    unit_num,
#    address_low,
#    address_low_suffix,
#    address_low_frac,
#    address_high,
#    street_predir,
#    street_name,
#    street_suffix,
#    street_postdir,
#    reg_map_id,
#    matched_regmap,
#    opa_account_num,
#    legal_remarks,
#    discrepancy,
#    property_count,
#    shape,
#    record_id,
#    recording_date,
#    receipt_date,
#    receipt_num,
#    adjusted_Assessed_value,
#    adjusted_cash_consideration,
#    adjusted_fair_market_value,
#    adjusted_local_tax_amount,
#    adjusted_other_consideration,
#    adjusted_state_tax_amount,
#    adjusted_total_consideration,
#    ward
#)
#select
#    document_id,
#    document_type,
#    display_date,
#    street_address,
#    zip_code,
#    grantors,
#    grantees,
#    cash_consideration,
#    other_consideration,
#    total_consideration,
#    assessed_value,
#    common_level_ratio,
#    fair_market_value,
#    state_tax_amount,
#    state_tax_percent,
#    local_tax_amount,
#    local_tax_percent,
#    document_date,
#    condo_name,
#    unit_num,
#    address_low,
#    address_low_suffix,
#    address_low_frac,
#    address_high,
#    street_predir,
#    street_name,
#    street_suffix,
#    street_postdir,
#    reg_map_id,
#    matched_regmap,
#    opa_account_num,
#    legal_remarks,
#    discrepancy,
#    property_count,
#    shape,
#    record_id,
#    recording_date,
#    receipt_date,
#    receipt_num,
#    adjusted_Assessed_value,
#    adjusted_cash_consideration,
#    adjusted_fair_market_value,
#    adjusted_local_tax_amount,
#    adjusted_other_consideration,
#    adjusted_state_tax_amount,
#    adjusted_total_consideration,
#    ward
#    from dor.vw_rtt_summary
#    on conflict on constraint er_rtt_summary_document_id_street_address_pkey
#    DO UPDATE
#    set
#    document_id=EXCLUDED.document_id,
#    document_type=EXCLUDED.document_type,
#    display_date=EXCLUDED.display_date,
#    street_address=EXCLUDED.street_address,
#    zip_code=EXCLUDED.zip_code,
#    grantors=EXCLUDED.grantors,
#    grantees=EXCLUDED.grantees,
#    cash_consideration=EXCLUDED.cash_consideration,
#    other_consideration=EXCLUDED.other_consideration,
#    total_consideration=EXCLUDED.total_consideration,
#    assessed_value=EXCLUDED.assessed_value,
#    common_level_ratio=EXCLUDED.common_level_ratio,
#    fair_market_value=EXCLUDED.fair_market_value,
#    state_tax_amount=EXCLUDED.state_tax_amount,
#    state_tax_percent=EXCLUDED.state_tax_percent,
#    local_tax_amount=EXCLUDED.local_tax_amount,
#    local_tax_percent=EXCLUDED.local_tax_percent,
#    document_date=EXCLUDED.document_date,
#    condo_name=EXCLUDED.condo_name,
#    unit_num=EXCLUDED.unit_num,
#    address_low=EXCLUDED.address_low,
#    address_low_suffix=EXCLUDED.address_low_suffix,
#    address_low_frac=EXCLUDED.address_low_frac,
#    address_high=EXCLUDED.address_high,
#    street_predir=EXCLUDED.street_predir,
#    street_name=EXCLUDED.street_name,
#    street_suffix=EXCLUDED.street_suffix,
#    street_postdir=EXCLUDED.street_postdir,
#    reg_map_id=EXCLUDED.reg_map_id,
#    matched_regmap=EXCLUDED.matched_regmap,
#    opa_account_num=EXCLUDED.opa_account_num,
#    legal_remarks=EXCLUDED.legal_remarks,
#    discrepancy=EXCLUDED.discrepancy,
#    property_count=EXCLUDED.property_count,
#    shape=EXCLUDED.shape,
#    record_id=EXCLUDED.record_id,
#    recording_date=EXCLUDED.recording_date,
#    receipt_date=EXCLUDED.receipt_date,
#    receipt_num=EXCLUDED.receipt_num,
#    adjusted_assessed_value=EXCLUDED.adjusted_assessed_value,
#    adjusted_cash_consideration=EXCLUDED.adjusted_cash_consideration,
#    adjusted_fair_market_value=EXCLUDED.adjusted_fair_market_value,
#    adjusted_local_tax_amount=EXCLUDED.adjusted_local_tax_amount,
#    adjusted_other_consideration=EXCLUDED.adjusted_other_consideration,
#    adjusted_state_tax_amount=EXCLUDED.adjusted_state_tax_amount,
#    adjusted_total_consideration=EXCLUDED.adjusted_total_consideration,
#    ward=EXCLUDED.ward;
#'''

select_permits_for_cama_stmt = '''
	select pfc.*,
	updated_permits.etl_modified_timestamp
	from lni.permits_for_cama pfc
	inner join (
	select distinct on (permit_number) permit_number, etl_modified_timestamp
			from (
	select replace((new_val->'permit_number')::text, '"', '') as permit_number, etl_modified_timestamp
	from audit.pin_history
	where tabname = 'permits_for_cama'
	and etl_modified_timestamp > '{last_update_date}'
	and lower(operation) != 'delete'
	) prep order by permit_number, etl_modified_timestamp desc
	) updated_permits on updated_permits.permit_number = pfc.permit_number 
	order by permit_number
'''

update_er_stage_transaction_q9_review_stmt = '''
update property.er_stage_transaction est
set workflow_type = fin.transaction_type,
partial_interest = fin.partial,
number_of_parcels = fin.number_of_parcels
from 
(
	select one.doc_id, one.transaction_type, one.partial, one.number_of_parcels from (
	select doc_id, transaction_type, partial, count(*) as number_of_parcels
	from (
		select distinct doc_id, transaction_type, partial, pin 
        from cama.vw_queue_9_review_findings_prod
		order by doc_id
		) qrf 
	group by doc_id, transaction_type, partial order by count(*) desc
	) one
	join (
		select doc_id, count(*)
		from (
			select distinct doc_id, transaction_type, partial
			from cama.vw_queue_9_review_findings_prod qrf 
		) prep 
		group by doc_id
		) amb on amb.count = 1 and amb.doc_id = one.doc_id
) fin where fin.doc_id::integer = est.title and 
(
    (coalesce(est.workflow_type, '') != coalesce(fin.transaction_type, '')) or
	(coalesce(est.partial_interest, '') != coalesce(fin.partial, '')) or
	(coalesce(est.number_of_parcels::text, '') != coalesce(fin.number_of_parcels::text, ''))
)
'''

update_er_stage_parcel_q9_resolution_overridden_stmt = '''
update property.er_stage_parcel esp
set resolution_type = 'overridden'
from (
	 SELECT distinct esp.*
   	FROM property.er_stage_parcel esp
     JOIN property.vw_er_stage_parcel_q9_review_overrides vespqro ON vespqro.title = esp.title
     where coalesce(esp.resolution_type, '') not in ('opa-q9-override', 'overridden')) prep 
    where prep.title = esp.title and prep.instance = esp.instance 
'''

insert_overridden_records_into_er_stage_parcel_overridden_stmt = '''
insert into property.er_stage_parcel_overridden (select * from property.er_stage_parcel where resolution_type = 'overridden')
'''

delete_overridden_records_from_er_stage_parcel_stmt = '''
delete from property.er_stage_parcel where resolution_type = 'overridden'
'''

# upsert_er_stage_parcel_q9_review_overrides_stmt = '''
# insert into property.er_stage_parcel 
# (title
# ,instance
# ,record_type
# ,pin
# ,pin_type
# ,concatenated_address
# ,base_address
# ,house_number
# ,house_num_suffix
# ,house_num_range
# ,street_dir_suffix
# ,street_name
# ,street_type
# ,street_dir
# ,condo_unit
# ,condo_name
# ,reg_map_id
# ,received_date
# ,resolution_type)
# select distinct
# vespqro.title
# ,vespqro.instance
# ,record_type
# ,vespqro.pin
# ,vespqro.pin_type
# ,vespqro.concatenated_address
# ,vespqro.base_address
# ,vespqro.house_number
# ,vespqro.house_number_suffix
# ,vespqro.house_num_range
# ,vespqro.street_dir_suffix
# ,vespqro.street_name
# ,vespqro.street_type
# ,vespqro.street_dir
# ,vespqro.condo_unit
# ,vespqro.condo_name
# ,vespqro.reg_map_id
# ,vespqro.edit_date as received_date
# ,resolution_type 
# from property.vw_er_stage_parcel_q9_review_overrides vespqro
# on conflict on constraint er_stage_parcel_bu_title_instance_pkey
# do
# update
# set
# title = EXCLUDED.title
# ,instance = EXCLUDED.instance
# ,record_type = EXCLUDED.record_type
# ,pin = EXCLUDED.pin
# ,pin_type = EXCLUDED.pin_type
# ,concatenated_address = EXCLUDED.concatenated_address
# ,base_address = EXCLUDED.base_address
# ,house_number = EXCLUDED.house_number
# ,house_num_suffix = EXCLUDED.house_num_suffix
# ,house_num_range = EXCLUDED.house_num_range
# ,street_dir_suffix = EXCLUDED.street_dir_suffix
# ,street_name = EXCLUDED.street_name
# ,street_type = EXCLUDED.street_type
# ,street_dir = EXCLUDED.street_dir
# ,condo_unit = EXCLUDED.condo_unit
# ,condo_name = EXCLUDED.condo_name
# ,reg_map_id = EXCLUDED.reg_map_id
# ,received_date = EXCLUDED.received_date 
# ,resolution_type = EXCLUDED.resolution_type
# '''

upsert_er_stage_parcel_test_sql = '''
insert into property.er_stage_parcel (title,instance,record_type,pin,concatenated_address,base_address,house_number,house_num_suffix,house_num_range,street_dir_suffix,street_name,street_type,street_dir,condo_unit,legal_remarks,condo_name,reg_map_id,received_date,resolution_type)
select
title
,instance
,record_type
,pin
,concatenated_address
,base_address
,house_number
,house_num_suffix
,house_num_range
,street_dir_suffix
,street_name
,street_type
,street_dir
,condo_unit
,legal_remarks
,condo_name
,reg_map_id
,received_date
,resolution_type
from property.vw_er_stage_parcel_v4
on conflict on constraint er_stage_parcel_title_instance_pkey
do
update
set
title = EXCLUDED.title
,instance = EXCLUDED.instance
,record_type = EXCLUDED.record_type
,pin = EXCLUDED.pin
,concatenated_address = EXCLUDED.concatenated_address
,base_address = EXCLUDED.base_address
,house_number = EXCLUDED.house_number
,house_num_suffix = EXCLUDED.house_num_suffix
,house_num_range = EXCLUDED.house_num_range
,street_dir_suffix = EXCLUDED.street_dir_suffix
,street_name = EXCLUDED.street_name
,street_type = EXCLUDED.street_type
,street_dir = EXCLUDED.street_dir
,condo_unit = EXCLUDED.condo_unit
,legal_remarks = EXCLUDED.legal_remarks
,condo_name = EXCLUDED.condo_name
,reg_map_id = EXCLUDED.reg_map_id
,received_date = EXCLUDED.received_date
,resolution_type = EXCLUDED.resolution_type
;
'''

# insert DOR records into stage_parcel:
upsert_er_stage_transaction_test_sql = '''
insert into property.er_stage_transaction
(title, transfer_date, document_date, recording_date, receipt_date, document_type, workflow_type, number_of_parcels, partial_interest, dor_received_date)
select title, transfer_date, document_date, recording_Date, receipt_date, document_type, workflow_type, number_of_parcels, partial_interest, dor_received_date
from property.vw_er_stage_transaction_v4
on conflict on constraint er_stage_transaction_title_pkey
do
update
set
title = EXCLUDED.title,
transfer_date =  EXCLUDED.transfer_date,
document_date =  EXCLUDED.document_date,
recording_date =  EXCLUDED.recording_date,
receipt_date =  EXCLUDED.receipt_date,
document_type =  EXCLUDED.document_type,
workflow_type =  EXCLUDED.workflow_type,
number_of_parcels =  EXCLUDED.number_of_parcels,
partial_interest =  EXCLUDED.partial_interest,
dor_received_date =  EXCLUDED.dor_received_date
;
'''
