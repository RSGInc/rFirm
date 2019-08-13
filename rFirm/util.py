# rFirm
# See full license in LICENSE.txt.

import os
import logging

import numpy as np
import pandas as pd

from activitysim.core import inject
from activitysim.core.config import setting

logger = logging.getLogger(__name__)


def scenario_dir():

    scenarios_dir = setting('scenarios_dir')
    assert scenarios_dir is not None, "scenarios_dir not defined in settings file"

    if not os.path.exists(scenarios_dir):
        raise RuntimeError("scenarios_dir not found: %s" % scenarios_dir)

    scenario_name = setting('scenario_name')
    assert scenario_name is not None, "scenario_name not defined in settings file"

    scenario_dir_path = os.path.join(scenarios_dir, scenario_name)
    assert os.path.exists(scenario_dir_path), "scenario_dir not found: %s" % scenario_dir_path

    return scenario_dir_path


def read_table(table_name, table_info, data_dir):

    logger.info("read_table read %s" % table_name)

    # read the csv file
    data_filename = table_info.get('filename', '%s.csv' % table_name)
    data_file_path = os.path.join(data_dir, data_filename)
    if not os.path.exists(data_file_path):
        raise RuntimeError("read_table %s - input file not found: %s"
                           % (table_name, data_file_path,))

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


def load_tables(table_list_name, data_dir=None):

    table_list = setting(table_list_name)
    if table_list is None:
        raise "I expected to find table list '%s' with table_info in settings." % table_list_name

    if data_dir is None:
        data_dir = setting('data_dir', inject.get_injectable('data_dir'))

    for table_name, table_info in table_list.iteritems():

        df = read_table(table_name, table_info, data_dir)
        inject.add_table(table_name, df)


def bucket_round(x, threshold=0.5):

    n = len(x)
    vecf = x.astype(int)
    vecd = x % 1.0
    veca = [0] * n
    adj = 0
    for i in range(len(x)):
        adj += vecd[i]
        if adj >= threshold:
            veca[i] = 1
            adj -= 1

    return (vecf + veca)


def target_round(weights, target_sum=None):
    """
    Round weights while ensuring (as far as possible that result sums to target_sum)

    Parameters
    ----------
    weights : numpy.ndarray(float)
    target_sum : int

    Returns
    -------
    rounded_weights : numpy.ndarray array of ints
    """

    if target_sum is None:
        target_sum = round(weights.sum())

    assert target_sum == int(target_sum)

    target_sum = int(target_sum)

    # integer part of numbers to round (astype both copies and coerces)
    rounded_weights = weights.astype(int)
    resid_weights = weights % 1.0

    # find number of residuals that we need to round up
    int_shortfall = target_sum - rounded_weights.sum()

    # clip to feasible, in case target was not achievable by rounding
    int_shortfall = np.clip(int_shortfall, 0, len(resid_weights))

    # Order the residual weights and round at the tipping point where target_sum is achieved
    if int_shortfall > 0:
        # indices of the int_shortfall highest resid_weights
        i = np.argsort(resid_weights)[-int_shortfall:]

        # add 1 to the integer weights that we want to round upwards
        rounded_weights[i] += 1

    assert rounded_weights.sum() == target_sum

    return rounded_weights


def round_preserve_threshold(x, threshold=1):
    x = np.round(np.cumsum(x))
    prev_value = 0
    borrow_value = 0
    new_x = np.array(x)
    for index in range(len(new_x)):
        # print borrow_value
        if index > 0:
            cur_value = x[index]
            if borrow_value == 0:
                if cur_value == prev_value:
                    cur_value = cur_value + threshold
                    borrow_value = borrow_value + threshold
                    prev_value = cur_value
                else:
                    prev_value = cur_value
            else:
                if cur_value < prev_value:
                    borrow_value = borrow_value + prev_value - cur_value + threshold
                    cur_value = prev_value + threshold
                    prev_value = cur_value
                elif cur_value == prev_value:
                    cur_value = cur_value + threshold
                    borrow_value = borrow_value + threshold
                    prev_value = cur_value
                elif cur_value > (prev_value+threshold):
                    cur_value = cur_value - threshold
                    borrow_value = borrow_value - threshold
                    prev_value = cur_value
                else:
                    prev_value = cur_value
        else:
            cur_value = x[index]
            prev_value = cur_value
        new_x[index] = cur_value
    new_x = np.diff(np.insert(new_x, 0, 0))
    return new_x.astype(int)
