import os

import pandas as pd
import orca

from activitysim.core import inject_defaults
from activitysim.core import tracing
from activitysim.core import pipeline
from activitysim.core import inject
from activitysim.core.config import setting

from afreight import steps


def teardown_function(func):
    orca.clear_cache()
    inject.reinject_decorated_tables()


def test_full_run1():

    configs_dir = os.path.join(os.path.dirname(__file__), 'configs')
    orca.add_injectable("configs_dir", configs_dir)

    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    orca.add_injectable("data_dir", data_dir)

    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    orca.add_injectable("output_dir", output_dir)

    orca.clear_cache()

    tracing.config_logger()

    # run list from settings file is dict with list of 'steps' and optional 'resume_after'
    run_list = setting('run_list')
    assert 'steps' in run_list, "Did not find steps in run_list"

    # list of steps and possible resume_after in run_list
    steps = run_list.get('steps')

    pipeline.run(models=steps, resume_after=None)

    assert os.path.exists(os.path.join(output_dir, 'sampledata.csv'))

    # tables will no longer be available after pipeline is closed
    pipeline.close_pipeline()

    orca.clear_cache()
