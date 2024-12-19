#update er_stage_transaction status to 'pinned' where status = 'opa_queue' and deed has been completely integrated into cama 
update_er_stage_transaction_status_pinned_sql = '''update property.er_stage_transaction 
set status = 'pinned'
where status = 'opa_queue' and title in (
select distinct title::integer from cama.vw_complete_integrated_sales
) 
'''

update_stage_transaction_status_deed_parsing_sql = '''update property.er_stage_transaction erst
set status = 'deed_parsing'
where status is null'''

# run pin matching

#update er_stage_parcel pin_type from pin_master for pin from matching
update_er_stage_parcel_pin_type_sql = '''update property.er_stage_parcel esp
set pin = matched.pin,
pin_type = matched.pin_type,
--pin_match_method = matched.pin_match_method
from (
	select distinct title, instance, pin, 
	pin_type 
	from property.er_stage_parcel_pin_matching
	) matched where matched.title = esp.title and matched.instance = esp.instance
'''

# send to OPA (2 tables, 1 non-queue 9, 1 queue 9)

#after sending, update er_stage_parcel sent date
update_er_stage_parcel_sent_date_sql = '''update property.er_stage_parcel ersp
set pin_Type = sent.pin_type,
sent_Date = sent.etl_modified_timestamp
from (
	select *
	from cama.property_deeds_new_backlog_first_batch
) sent where sent.document_id = ersp.title and sent.instance = ersp.instance
'''

# & update er_stage_transaction status to 'opa_queue'
update_er_stage_transaction_status_opa_queue_sql = '''update property.er_stage_transaction erst
set status = 'opa_queue'
where status is 'deed_parsing'
'''

