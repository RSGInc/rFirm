# aFreight
# See full license in LICENSE.txt.

import os
import logging

import pandas as pd

from activitysim.core import inject
from activitysim.core.config import setting

logger = logging.getLogger(__name__)


def read_csv_table(tablename, data_dir=None):

    # - allow settings to override injectable
    if data_dir is None:
        data_dir = setting('data_dir', inject.get_injectable('data_dir'))

    input_tables = setting('input_tables')
    if input_tables is None:
        raise "I expected to find 'input_tables' with table_info for '%s' in settings." % tablename

    table_info = input_tables[tablename]

    if tablename not in input_tables:
        raise "I expected to find table_info for '%s' in 'input_tables' in settings." % tablename

    logger.info("read_csv_table %s" % tablename)

    # read the csv file
    data_filename = table_info.get('filename', None)
    data_file_path = os.path.join(data_dir, data_filename)
    if not os.path.exists(data_file_path):
        raise RuntimeError("input_pre_processor %s - input file not found: %s"
                           % (tablename, data_file_path,))

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

    return df
