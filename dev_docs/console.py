import os

os.getcwd()

os.chdir('national_freight')

from activitysim.core import inject_defaults

from activitysim.core import tracing
from activitysim.core import pipeline
from activitysim.core import inject
from activitysim.core.config import setting
import afreight

tracing.config_logger()

pipeline.open_pipeline('_')

# pipeline.preload_injectables()


df = pipeline.get_table("table1").to_frame()


import pandas as pd

file_path = "/Users/jeff.doyle/work/afreight/national_freight/regression_data/results/firm_sim_types/outputs/Firms.csv"
rfirms = pd.read_csv(file_path, comment='#'). \
    rename(columns={'BusID': 'bus_id', 'TAZ1': 'TAZ', 'Model_EmpCat': 'model_emp_cat'}). \
    set_index('bus_id')

rfirms.groupby(['TAZ', 'SCTG']).bus_id.count()

new_firms = firms[firms.TAZ1>7655665]

firms[firms.TAZ1==TRACE_TAZ][['BusID','Model_EmpCat', 'Emp', 'TAZ1']]


'%s firms, %s producers, %s makers' % (rfirms.shape[0], rfirms.Producer.sum(), rfirms.Maker.sum())
'97025 firms, 31873 producers, 31561 makers'

'97025 firms, 31873 producers, 31561 makers'

file_path = "/Users/jeff.doyle/work/afreight/national_freight/regression_data/results/firm_sim_iopairs/outputs/x_iopairs.csv"
firm_input_output_pairs = pd.read_csv(file_path, comment='#')

    . \
    rename(columns={'BusID': 'bus_id', 'TAZ1': 'TAZ', 'Model_EmpCat': 'model_emp_cat'}). \
    set_index('bus_id')


file_path = "/Users/jeff.doyle/work/afreight/national_freight/regression_data/results/firm_sim_producers/outputs/producers.csv"
producers = pd.read_csv(file_path, comment='#')

file_path = "/Users/jeff.doyle/work/afreight/national_freight/regression_data/results/firm_sim_consumers/outputs/consumers.csv"
consumers = pd.read_csv(file_path, comment='#')


file_path = "/Users/jeff.doyle/work/afreight/national_freight/regression_data/results/firm_sim_producers/outputs/x_producers.csv"
xproducers = pd.read_csv(file_path, comment='#')
