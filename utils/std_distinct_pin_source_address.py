import petl as etl
from passyunk.parser import PassyunkParser

parser = PassyunkParser(MAX_RANGE=9999999999999999999)


def standardize_nulls(val):
    if type(val) == str:
        return None if val.strip() == '' else val
    else:
        return None if val == 0 else val


def check_address_comps(**kwargs):
    file_path = kwargs['csv_path']
    infile = file_path + '/' + kwargs['infile_name'] + '.csv'
    outfile_suffix = kwargs['outfile_suffix']
    outfile = infile.replace('.csv', outfile_suffix + '.csv')

    etl.fromcsv(infile).addfield('parsed_comps', lambda p: parser.parse(p['street_address'])) \
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
