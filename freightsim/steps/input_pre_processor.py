# PopulationSim
# See full license in LICENSE.txt.

import logging
import os

import pandas as pd
import numpy as np

from activitysim.core import inject
from activitysim.core import pipeline

from activitysim.core.config import setting


logger = logging.getLogger(__name__)


@inject.step()
def input_pre_processor():

    # alternate table list name may have been provided as a model argument
    table_list_name = inject.get_step_arg('table_list', default='input_table_list')
    table_list = setting(table_list_name)
    assert table_list is not None, "table list '%s' not in settings." % table_list_name

    # - allow settings to override injectable
    data_dir = setting('data_dir', inject.get_injectable('data_dir'))

    for table_info in table_list:

        tablename = table_info['tablename']

        logger.info("input_pre_processor processing %s" % tablename)

        # read the csv file
        data_filename = table_info.get('filename', None)
        data_file_path = os.path.join(data_dir, data_filename)
        if not os.path.exists(data_file_path):
            raise RuntimeError("input_pre_processor %s - input file not found: %s"
                               % (tablename, data_file_path, ))

        logger.info("Reading csv file %s" % data_file_path)
        df = pd.read_csv(data_file_path, comment='#')

        # rename columns
        column_map = table_info.get('column_map', None)
        if column_map:
            df.rename(columns=column_map, inplace=True)

        # set index
        index_col = table_info.get('index_col', None)
        if index_col is not None:
            if index_col in df.columns:
                df.set_index(index_col, inplace=True)
            else:
                df.index.names = [index_col]

        logger.info("adding table %s" % tablename)

        inject.add_table(tablename, df)
