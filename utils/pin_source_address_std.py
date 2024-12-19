import re
import petl as etl
import geopetl
import psycopg2
from passyunk.parser import PassyunkParser

parser = PassyunkParser(MAX_RANGE=9999999999999999999)



def standardize_nulls(val):
    if type(val) == str:
        return None if val.strip() == '' else val
    else:
        return None if val == 0 else val


def check_address_comps(**kwargs):
    file_path = kwargs['templates_dict']['csv_path']
    infile = file_path + '/' + kwargs['templates_dict']['infile_name']
    outfile_suffix = kwargs['templates_dict']['outfile_suffix']
    outfile = infile.replace('.csv', outfile_suffix + '.csv')

    etl.fromcsv(infile) \
    .addfield('parsed_comps', lambda p: parser.parse(p['street_address'])) \
    .addfield('std_address_low', lambda a: a['parsed_comps']['components']['address']['low_num']) \
    .addfield('std_address_low_suffix', lambda a: a['parsed_comps']['components']['address']['addr_suffix']) \
    .addfield('std_address_low_fractional', lambda a: a['parsed_comps']['components']['address']['fractional']) \
    .addfield('std_address_high', lambda a: a['parsed_comps']['components']['address']['high_num']) \
    .addfield('std_street_predir', lambda a: a['parsed_comps']['components']['street']['predir']) \
    .addfield('std_street_name', lambda a: a['parsed_comps']['components']['street']['name']) \
    .addfield('std_street_suffix', lambda a: a['parsed_comps']['components']['street']['suffix']) \
    .addfield('std_street_postdir', lambda a: a['parsed_comps']['components']['street']['postdir']) \
    .addfield('std_unit_type', lambda a: a['parsed_comps']['components']['address_unit']['unit_type']) \
    .addfield('std_unit_num', lambda a: a['parsed_comps']['components']['address_unit']['unit_num']) \
    .addfield('std_street_address', lambda a: a['parsed_comps']['components']['output_address']) \
    .addfield('std_street_code', lambda a: a['parsed_comps']['components']['street']['street_code']) \
    .addfield('std_seg_id', lambda a: a['parsed_comps']['components']['cl_seg_id']) \
    .addfield('cl_addr_match', lambda a: a['parsed_comps']['components']['cl_addr_match']) \
    .addfield('std_zipcode', lambda a: a['parsed_comps']['components']['mailing']['zipcode']) \
    .addfield('std_zip4', lambda a: a['parsed_comps']['components']['mailing']['zip4']) \
    .addfield('std_base_address', lambda a: a['parsed_comps']['components']['base_address']) \
    .cutout('parsed_comps') \
    .tocsv(outfile, write_header=True)
