# aFreight
# See full license in LICENSE.txt.

import logging

import pandas as pd

from activitysim.core import pipeline

from activitysim.core import inject
from activitysim.core.config import setting

from afreight.util import read_csv_table

logger = logging.getLogger(__name__)


# @inject.table()
# def geo_crosswalk():
#
#     # - geo_crosswalk
#
#     FIPS_NUMA = read_csv_table('FIPS_NUMA')
#     FIPS_NUMA = FIPS_NUMA.fillna(0)
#     FIPS_NUMA = FIPS_NUMA.astype(int)
#
#     FAF_NUMA = read_csv_table('FAF_NUMA')
#     FAF_NUMA = FAF_NUMA.fillna(0)
#     FAF_NUMA = FAF_NUMA.astype(int)
#
#     geo_crosswalk = pd.merge(
#         left=FIPS_NUMA,
#         right=FAF_NUMA,
#         left_index=True,
#         right_index=True
#     )
#
#     return geo_crosswalk
