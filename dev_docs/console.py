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
