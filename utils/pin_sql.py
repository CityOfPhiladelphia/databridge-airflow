# refresh_pin_master:
refresh_pin_master_stmt = '''
BEGIN;
drop index property.pin_master_cama_concatenated_address_idx;
drop index property.pin_master_pin_idx;
truncate table property.pin_master{test_suffix};
insert into property.pin_master{test_suffix} (select *, current_timestamp as etl_modified_timestamp from property.vw_pin_master{test_suffix});
CREATE INDEX pin_master_cama_concatenated_address_idx
    ON property.pin_master{test_suffix} USING btree
    (cama_concatenated_address COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX pin_master_pin_idx
    ON property.pin_master{test_suffix} USING btree
    (pin COLLATE pg_catalog."default")
    TABLESPACE pg_default;
COMMIT;
'''

# insert DOR records into stage_parcel:
stage_parcel_insert_dor_stmt = '''
    insert into property.stage_parcel{test_suffix} (
        select distinct spm.r_num, spm.record_type,
                case when spm.pin = '' then pm.master_pin else spm.pin end as pin,
                spm.condo_master_pin, spm.pin_type, spm.concatenated_address, spm.house_number, spm.house_num_suffix,
                spm.house_num_range, spm.street_dir, spm.street_name, spm.street_type, spm.street_dir_suffix, spm.condo_unit,
                spm.legal_remarks, spm.condo_name, spm.reg_map_id, spm.opa_id, spm.pcu_id, spm.received_date,
                current_timestamp as sent_date,
                spm.resolution_type,
                spm.recommended_action
        from (
         select sp.*
         from property.vw_stage_parcel{test_suffix} sp
         inner join (select * from property.vw_stage_transaction{test_suffix} where upper(document_type) like '%DEED%') st
         on st.r_num = sp.r_num
		 inner join (
				select r_num, coalesce(pin, '') as pin,coalesce(concatenated_address, '') as concatenated_address, coalesce(legal_remarks, '') as legal_remarks, coalesce(reg_map_id, '') as reg_map_id
				from property.vw_stage_parcel{test_suffix}  vspt
				except
				select r_num, coalesce(pin, '') as pin,coalesce(concatenated_address, '') as concatenated_address, coalesce(legal_remarks, '') as legal_remarks, coalesce(reg_map_id, '') as reg_map_id
				from property.stage_parcel{test_suffix}  spt
			) new_records on new_records.pin = sp.pin
    ) spm
        left join property.vw_stage_parcel_matching{test_suffix} pm
                on pm.r_num = spm.r_num and
                pm.concatenated_address = spm.concatenated_address

    )
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

# Update stage transaction status for records sent to opa queue:
update_stage_transaction_status_for_opa_queued_records_stmt = '''
    update property.stage_transaction{test_suffix} set status = 'opa queue'
    where status = 'pin matching' and r_num in
    (
        select distinct r_num from property.vw_new_records_for_cama{test_suffix}
    )

'''


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
        inner join property.pin_master_test2 pm on pm.pin = sp.pin and pm.tags in (
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
        inner join property.pin_master_test2 pm on pm.pin = sp.pin and pm.matchtype != '1-1'
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
        inner join property.pin_master_test2 pm on pm.pin = sp.pin and pm.tags not in (null, '')
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
