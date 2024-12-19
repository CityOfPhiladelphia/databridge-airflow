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


def concatenate_dor_address(source_comps):
    # Get attributes
    address_low = source_comps['house']
    address_low_suffix = source_comps['suf']
    address_high = source_comps['stex']
    street_predir = source_comps['stdir']
    street_name = source_comps['stnam']
    street_suffix = source_comps['stdes']
    street_postdir = source_comps['stdessuf']
    unit_num = source_comps['unit']
    unit_type = source_comps['unit_type']
    source_address = None
    street_full = ''
    # Make street full
    if street_name:
        street_comps = [street_predir, street_name, street_suffix, \
                        street_postdir]
        street_full = ' '.join([x for x in street_comps if x])

    # Only accept numeric address_low_suffixes = 2 for transformation to 1/2; discard other numeric suffixes
    address_low_fractional = None
    try:
        address_low_suffix_int = int(address_low_suffix)
        if address_low_suffix_int == 2:
            address_low_fractional = '1/2'
        address_low_suffix = None
    except:
        pass

    address_full = None
    if address_low:
        address_full = str(address_low)
        if address_low_suffix:
            address_full += address_low_suffix
        if address_low_fractional:
            address_full += ' ' + address_low_fractional
        if address_high:
            address_full += '-' + str(address_high)

    # Get unit
    unit_full = None
    if standardize_nulls(unit_num):
        if standardize_nulls(unit_type):
            unit_full = '{} {}'.format(unit_type, unit_num)
        else:
            unit_full = '# {}'.format(unit_num)

    if address_full and street_full:
        source_address_comps = [address_full, street_full, unit_full]
        source_address = ' '.join([x for x in source_address_comps if x])

    return source_address if source_address != None else ''


def check_address_comps(**kwargs):
    file_path = kwargs['csv_path']
    infile = file_path + '/' + kwargs['infile_name'] + '.csv'
    outfile_suffix = kwargs['outfile_suffix']
    outfile = infile.replace('.csv', outfile_suffix + '.csv')

    etl.fromcsv(infile).convert('stex', lambda a: a if int(a) > 0 else None) \
    .addfield('concatenated_address', lambda c: concatenate_dor_address(
    {'house': c['house'], 'suf': c['suf'], 'stex': c['stex'], 'stdir': c['stdir'], 'stnam': c['stnam'],
     'stdes': c['stdes'], 'stdessuf': c['stdessuf'], 'unit': c['unit'], 'unit_type': c['unit_type'], 'stcod': c['stcod']})) \
    .addfield('parsed_comps', lambda p: parser.parse(p['concatenated_address'])) \
    .addfield('std_address_low', lambda a: a['parsed_comps']['components']['address']['low_num']) \
    .addfield('std_address_low_suffix', lambda a: a['parsed_comps']['components']['address']['addr_suffix'] if
a['parsed_comps']['components']['address']['addr_suffix'] else a['parsed_comps']['components']['address']['fractional']) \
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
    .cutout('parsed_comps') \
    .addfield('no_address',
              lambda a: 1 if standardize_nulls(a['stnam']) is None or standardize_nulls(a['house']) is None else None) \
    .addfield('change_stcod', lambda a: 1 if a['no_address'] != 1 and str(standardize_nulls(a['stcod'])) != str(
    standardize_nulls(a['std_street_code'])) else None) \
    .addfield('change_house', lambda a: 1 if a['no_address'] != 1 and str(standardize_nulls(a['house'])) != str(
    standardize_nulls(a['std_address_low'])) else None) \
    .addfield('change_suf', lambda a: 1 if a['no_address'] != 1 and str(standardize_nulls(a['suf'])) != str(
    standardize_nulls(a['std_address_low_suffix'])) else None) \
    .addfield('change_unit', lambda a: 1 if a['no_address'] != 1 and str(standardize_nulls(a['unit'])) != str(
    standardize_nulls(a['std_unit_num'])) else None) \
    .addfield('change_unit_type', lambda a: 1 if a['no_address'] != 1 and str(standardize_nulls(a['unit_type'])) != str(
    standardize_nulls(a['std_unit_type'])) else None) \
    .addfield('change_stex', lambda a: 1 if a['no_address'] != 1 and str(standardize_nulls(a['stex'])) != str(
    standardize_nulls(a['std_address_high'])) else None) \
    .addfield('change_stdir', lambda a: 1 if a['no_address'] != 1 and str(standardize_nulls(a['stdir'])) != str(
    standardize_nulls(a['std_street_predir'])) else None) \
    .addfield('change_stnam', lambda a: 1 if a['no_address'] != 1 and str(standardize_nulls(a['stnam'])) != str(
    standardize_nulls(a['std_street_name'])) else None) \
    .addfield('change_stdes', lambda a: 1 if a['no_address'] != 1 and str(standardize_nulls(a['stdes'])) != str(
    standardize_nulls(a['std_street_suffix'])) else None) \
    .addfield('change_stdessuf',
              lambda a: 1 if a['no_address'] != 1 and str(standardize_nulls(a['stdessuf'])) != str(
                  standardize_nulls(a['std_street_postdir'])) else 0) \
    .cutout('no_address') \
    .tocsv(outfile, write_header=True)
