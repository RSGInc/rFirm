# aFreight
# See full license in LICENSE.txt.

import logging
import os

import pandas as pd
import numpy as np

from activitysim.core import inject
from activitysim.core import pipeline

from activitysim.core.config import setting

from afreight.util import read_csv_table


logger = logging.getLogger(__name__)


def create_sample_data():

    FIPS_NH = 33
    FIPS_VT = 50
    FIPS_ME = 23
    STATE_FIPS_SAMPLE = [FIPS_ME, FIPS_NH, FIPS_VT]

    data_dir = setting('data_dir', inject.get_injectable('data_dir'))
    input_tables = setting('input_tables')

    # - corresp_taz_fips
    table_info = input_tables['corresp_taz_fips']
    data_file_name = table_info['filename']
    data_file_path = os.path.join(data_dir, table_info['filename'])
    FIPS_NUMA = pd.read_csv(data_file_path, comment='#')
    # slice by state FIPS
    FIPS_NUMA = FIPS_NUMA[FIPS_NUMA['StateFIPS'].isin(STATE_FIPS_SAMPLE)]
    inject.add_table(os.path.splitext(data_file_name)[0], FIPS_NUMA)

    # - corresp_taz_faf4
    table_info = input_tables['corresp_taz_faf4']
    data_file_name = table_info['filename']
    data_file_path = os.path.join(data_dir, data_file_name)
    FAF_NUMA = pd.read_csv(data_file_path, comment='#')
    # slice by taz list from FIPS_NUMA
    FAF_NUMA = FAF_NUMA[FAF_NUMA['TAZ'].isin(FAF_NUMA.TAZ)]
    inject.add_table(os.path.splitext(data_file_name)[0], FAF_NUMA)

    # - cbp
    table_info = input_tables['cbp']
    data_file_name = table_info['filename']
    data_file_path = os.path.join(data_dir, data_file_name)
    cbp = pd.read_csv(data_file_path, comment='#')
    # slice by state FIPS
    cbp = cbp[cbp['fipstate'].isin(STATE_FIPS_SAMPLE)]
    inject.add_table(os.path.splitext(data_file_name)[0], cbp)


def geo_crosswalk():

    # - geo_crosswalk

    FIPS_NUMA = read_csv_table('corresp_taz_fips')
    FIPS_NUMA = FIPS_NUMA.fillna(0)
    FIPS_NUMA = FIPS_NUMA.astype(int)

    FAF_NUMA = read_csv_table('corresp_taz_faf4')
    FAF_NUMA = FAF_NUMA.fillna(0)
    FAF_NUMA = FAF_NUMA.astype(int)

    df = pd.merge(
        left=FIPS_NUMA,
        right=FAF_NUMA,
        left_index=True,
        right_index=True
    )

    return df


def county_business_patterns():

    df = read_csv_table('cbp')

    df = df[
        (df['county_FIPS'] != 999) & df['naics'].str.match('[0-9]{6}')
        ]

    # FIXME - naics col as string or int?
    df['naics'] = df['naics'].astype('str')

    # convert nulls in str columns to empty strings
    for c in ['empflag', 'emp_nf', 'qp1_nf', 'ap_nf']:
        df[c] = df[c].fillna('')

    df['FIPS'] = 1000*df['state_FIPS'] + df['county_FIPS']

    return df


@inject.step()
def input_pre_processor():

    # create_sample_data()

    # # - geo_crosswalk
    df = geo_crosswalk()
    inject.add_table('geo_crosswalk', df)

    # - cbp
    cbp = county_business_patterns()
    inject.add_table('cbp', cbp)

    # - NAICS_to_NAICSio
    df = read_csv_table('NAICS_to_NAICSio')
    inject.add_table('NAICS_to_NAICSio', df)

    # - NAICSio_to_SCTG
    df = read_csv_table('NAICSio_to_SCTG')
    inject.add_table('NAICSio_to_SCTG', df)

    # - SUSB_Firms
    df = read_csv_table('SUSB_firms')
    df = df.fillna(0)
    df = df.astype(int)
    inject.add_table('SUSB_Firms', df)

    # - FIPS_to_FAFZone
    df = read_csv_table('FIPS_to_FAFZone')
    inject.add_table('FIPS_to_FAFZone', df)

    """
    ############# from NFM_Firmsynthesis.R
    #
    # #naics-SCTG makers list
    # NAICS2007_to_NAICS2007io	#from the rFreight package
    # NAICS2007io_to_SCTG       #from the rFreight package (only includes n2<52) ##TODO: Check this?
    #
    # #labels for naics
    # NAICS2007                 #from the rFreight package
    """

    for table_name in pipeline.orca_dataframe_tables():
        df = inject.get_table(table_name, None).to_frame()
        print ("\n%s\n" % table_name), df.dtypes
