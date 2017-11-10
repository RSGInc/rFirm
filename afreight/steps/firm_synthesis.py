# aFreight
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
def firm_synthesis():

    FIPS_NUMA = pipeline.get_table('FIPS_NUMA')

    # drop rows with any null values
    FIPS_NUMA = FIPS_NUMA[FIPS_NUMA.notnull().all(axis=1)]

    pipeline.replace_table('FIPS_NUMA', FIPS_NUMA)

    cbp14co = pipeline.get_table('cbp14co')

    cbp = cbp14co[
        (cbp14co['county_FIPS'] != 999) & cbp14co['naics'].str.match('[0-9]{6}')
    ]

    print "cbp14co len", len(cbp14co.index)
    print "cbp len", len(cbp.index)
    print "\ncbp.dtypes\n", cbp.dtypes

    inject.add_table('cbp', cbp)
