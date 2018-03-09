# rFirm
# See full license in LICENSE.txt.

import logging
import os

import pandas as pd
import numpy as np

from activitysim.core import inject
from activitysim.core import pipeline

from activitysim.core.config import setting

from rFirm.util import load_tables
from rFirm.util import scenario_dir

logger = logging.getLogger(__name__)


@inject.step()
def input_pre_processor():

    # - load generic data
    data_dir = setting('data_dir', inject.get_injectable('data_dir'))
    load_tables('input_tables', data_dir)

    # - load scenario input data
    scenario_input_dir = os.path.join(scenario_dir(), 'inputs')
    load_tables('scenario_input_tables', scenario_input_dir)

    for table_name in pipeline.orca_dataframe_tables():
        df = inject.get_table(table_name, None).to_frame()
