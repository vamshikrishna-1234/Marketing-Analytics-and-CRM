import pandas as pd
import sys
import numpy as np


sys.path.append('python/')
from data_utils.dbConnector import dbConnector
from stat_utils.get_stats import get_stats
from data_utils.excel_writer import write_to_excel

pd.set_option('future.no_silent_downcasting', True)
pd.set_option('display.max_columns', 100)

project_id='gcp-abs-udco-mkbz-prod-prj-01'
population_table_name = \
    'gcp-abs-udco-mkbz-prod-prj-01.udco_ds_dem.kd_christmas_catchall_tvc_funnel_w_dimensions'

dimension_variables = ['constant', 'facts_segment_name', 'division_id_tvc',
                       'email_flag', 'push_flag', 'sms_flag', 'avg_weekly_spend_bucket']
measure_variable = 'net_sales_hh'
target_group_column_name = 'target_group_tvc'
test_values = ['Test']
control_value = 'Control'

dimension_variables = ['constant', 'facts_seg_tvc','division_id_tvc']


#df = pd.read_csv('C:/Users/System276/Downloads/bq-results-20241220-122524-1734697579536.csv')
db_connector = dbConnector(project_id=project_id)
#df = db_connector.get_df_from_table_name(population_table_name)
cols_query = ','.join([target_group_column_name,measure_variable,'net_sales_hh']+\
                      [x for x in dimension_variables if x!='constant'])
df = db_connector.get_df_from_query(f'''
select {cols_query}
 from {population_table_name} tvc
 left join gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.final_population_CC fp on tvc.household_id=fp.household_id
''')
df = db_connector.get_df_from_query('''
select
tvc.*,
COALESCE(fp.avg_weekly_spend,0) as avg_weekly_spend,
facts_seg,
avg_weekly_spend_bucket,
pd_reds_bucket
from gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.PZN_christmas_2024_CATCHALL tvc
left join gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.final_population_CC fp on tvc.household_id=fp.household_id
''')

df['constant'] = 1
df['facts_seg_combined_1'] = np.where(df['facts_seg_tvc'].isin(['2.Best','1.Elite']),'B&E',
                                    df['facts_seg_tvc'])
df['facts_seg_combined_2'] = np.where(df['facts_seg_tvc'].isin(['2.Best','1.Elite','3.Good']),'B&E&G',
                                    df['facts_seg_tvc'])


dimension_variables = ['avg_weekly_spend_bucket',]
stats = get_stats(df, dimension_variables, test_values, measure_variable,
                  target_group_column_name=target_group_column_name,
                  control_value=control_value,
                  MDE=None)

write_to_excel(stats, path='christmas_output_current_split.xlsx')


