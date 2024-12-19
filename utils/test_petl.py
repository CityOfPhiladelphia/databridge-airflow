import petl as etl
from test_doroem_parcel_report import check_address_comps

csv_path = '/home/ubuntu/databridge-airflow/utils'
infile_name = 'dor_parcels_stex_test'
outfile_suffix = '_fmt'
check_address_comps(csv_path=csv_path, infile_name=infile_name, outfile_suffix=outfile_suffix)

#$rows = etl.fromcsv(csv_path + '/' + infile_name + '.csv').select(lambda s: int(s.stex) > 0)
#rows.tocsv('dor_parcels_stex_test.csv')



