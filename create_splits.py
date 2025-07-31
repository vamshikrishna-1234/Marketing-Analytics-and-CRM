
import time
import pandas as pd
import sys
from sklearn.model_selection import train_test_split
import numpy as np

import random

sys.path.append('python/')
from data_utils.dbConnector import dbConnector
from stat_utils.get_stats import get_stats
from data_utils.excel_writer import write_to_excel

start_time = time.time()

seed = 2000

output_ver = 11

print('Seed:', seed)
print('Output Version:', output_ver)

#define variables to balance on
balance_variables = [
    'email_flag',
    'push_flag',
    'sms_flag',
    # 'high_volume_flag',
    # 'digitally_engaged_occasional_flag',
    'division_id',
    'facts_seg',
    # 'ecom_ind',
    # 'b4u_status',
    'pd_reds_bucket',
    'avg_weekly_spend_bucket',
    'my_needs_segment_id',
    'true_price_segment_id'
]

# pd.set_option('future.no_silent_downcasting', True)
pd.set_option('display.max_columns', 100)

project_id='gcp-abs-udco-bsvw-prod-prj-01'
population_table_name = 'gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.soc_august_2025_final_population'


random.seed(seed)
target_groups = 2
create_internal_holdout = True
target_group_ratio = [0.88888,0.111112] #the formula to calculate this is test_size/desired ratio in the overall set. So if you
                                # want 80/10/10 split, test_size=0.9 and first group weight would be 0.8/0.9.

#target_group_ratio = [0.5,0.5] #the formula to calculate this is test_size/desired ratio in the overall set. So if you
                                # want 80/10/10 split, test_size=0.9 and first group weight would be 0.8/0.9.


target_group_column_name='target_group'
control_value = 'Control'
target_value = 'Test'
test_size = 0.9


measure_variable = 'avg_weekly_spend'

########
group = list(range(0, target_groups))

# df = pd.read_csv('C:/Users/System276/Downloads/bq-results-20241220-122524-1734697579536.csv')
db_connector = dbConnector(project_id=project_id)
df = db_connector.get_df_from_table_name(population_table_name)

# df.to_pickle('input/august_btd_tvc_population_iter1.pkl')
# df = pd.read_pickle('input/august_btd_tvc_population_iter1.pkl')

download_time = time.time()

#to rensure re-producible splits as long as the base table does not change
df = df.sort_values('household_id')
for var in ['household_id','division_id']:
    if df[var].isnull().any():
        if var=='household_id':
            raise ValueError('Household has nulls. check code.')
        print(f'Filling nulls for {var}')
        df[var] = df[var].fillna(-1)
    df[var] = df[var].astype('Int64')

if 'target_group' in df.columns:
    print('Renaming target group...')
    df.rename(columns={'target_group':'target_group_original'}, inplace=True)


#define variable to measure across groups
dimension_variables = ['constant'] + balance_variables


#start processing
df['constant'] = 1
if target_groups>1:
    df['group'] = random.choices(group, k=df.shape[0], weights=target_group_ratio)
else:
    df['group'] = 1

#add group to balance incase more than one group
balance_variables = balance_variables + ['group']

df['group_count'] = df.groupby(balance_variables, dropna=False).transform('size')
df_big_groups = df[df['group_count']>=50]
df_small_groups = df[df['group_count']<50]


control_big,target_big = train_test_split(df_big_groups,
                                          test_size=test_size,
                                          stratify=df_big_groups[balance_variables],
                                          random_state=seed)
# control_big['avg_weekly_spend'] = control_big['avg_weekly_spend'] * 1.005
control_small,target_small = train_test_split(df_small_groups,
                                              test_size=test_size,
                                              random_state=seed)

control = pd.concat([control_big,control_small])
target = pd.concat([target_big,target_small])

balancing_time = time.time()

control[target_group_column_name] = control_value

if target_groups > 1:
    target[target_group_column_name] = target_value + target['group'].astype('str')
else:
    target[target_group_column_name] = target_value

test_values = target.target_group.unique()

df_split = pd.concat([control, target]).reset_index(drop=True)
if create_internal_holdout:
    df_split[target_group_column_name+'_internal'] = df_split[target_group_column_name].copy()
    df_split[target_group_column_name] = np.where(df_split[target_group_column_name].isin(test_values),target_value,
                                                  control_value)
    stats_internal = get_stats(df_split,
                      dimension_variables,
                      test_values,
                      measure_variable,
                      control_value=control_value,
                      target_group_column_name=target_group_column_name+'_internal',
                      MDE=0.005)

    #overwrite so that overall report can be generated
    test_values = [target_value]
else:
    pass

stats_time_internal = time.time()

#generate stats w.r.t. target_group internal
stats = get_stats(df_split,
                  dimension_variables,
                  test_values,
                  measure_variable,
                  control_value=control_value,
                  target_group_column_name=target_group_column_name,
                  MDE=0.005)

# stats_high_volume = get_stats(df_split.loc[df_split['high_volume_flag']==1],
#                               dimension_variables,
#                               test_values,
#                               measure_variable,
#                               control_value=control_value,
#                               target_group_column_name=target_group_column_name,
#                               MDE=0.005)

# stats_digitally_engaged_occasional = get_stats(df_split.loc[df_split['digitally_engaged_occasional_flag']==1],
#                                               dimension_variables,
#                                               test_values,
#                                               measure_variable,
#                                               control_value=control_value,
#                                               target_group_column_name=target_group_column_name,
#                                               MDE=0.005)

write_to_excel(stats,
                path=f'C:/Users/System162L/Downloads/python2/soc_august_ver{output_ver}.xlsx')
write_to_excel(stats_internal,
                path=f'C:/Users/System162L/Downloads/python2/internal_soc_august_ver{output_ver}.xlsx')
# write_to_excel(stats_high_volume,
#                 path=f'output/bnc_tvc_output_high_volume_balancing_ver{output_ver}.xlsx')
# write_to_excel(stats_digitally_engaged_occasional,
#                 path=f'output/bnc_tvc_output_digitally_engaged_occasional_balancing_ver{output_ver}.xlsx')

# del df
# del df_split


stats_time = time.time()

# df_split.to_pickle(f'output/august_btd_tvc_ver{output_ver}.pkl')
# df_split = pd.read_pickle(f'output/august_btd_tvc_ver{output_ver}.pkl')


db_connector.upload_to_bq(dat=df_split,
                          destination_table='gcp-abs-udco-bsvw-prod-prj-01.udco_ds_bizops.SOC_August_2025_CATCHALL')


upload_time = time.time()

print(f"Download time: {download_time-start_time:.4f} seconds")
print(f"Balancing time: {balancing_time-download_time:.4f} seconds")
print(f"Get Stats Internal time: {stats_time_internal-balancing_time:.4f} seconds")
print(f"Get Stats time: {stats_time-stats_time_internal:.4f} seconds")
print(f"Upload time: {upload_time-stats_time:.4f} seconds")
print(f"Total time: {upload_time-start_time:.4f} seconds")



