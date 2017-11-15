# aFreight
# See full license in LICENSE.txt.

import logging
import os

import pandas as pd
import numpy as np

from activitysim.core import inject
from activitysim.core import pipeline

from activitysim.core.config import setting
from activitysim.core.util import quick_loc_df


logger = logging.getLogger(__name__)


@inject.step()
def firm_synthesis():

    cbp = pipeline.get_table('cbp')

    FIPS_to_FAFZone = pipeline.get_table('FIPS_to_FAFZone')

    # Summarize employment and establishments by naics and taz
    # add the FAFZONE and then the TAZ for areas outside of model area
    # cbp[County_to_FAFZone, FAF4 := i.FAFZone_4.2, on = c("StateFIPS", "CountyFIPS"), nomatch = NA]

    cbp['FAF4'] = quick_loc_df(cbp['FIPS'], FIPS_to_FAFZone, 'FAFZone_4_2')
    assert not cbp['FAF4'].isnull().any()

    # join on the TAZ (one per FAFZONE)
    # cbp[FIPS_NUMA, TAZ: = i.TAZ, on = c("StateFIPS", "CountyFIPS"), nomatch = NA]

    cbp['TAZ'] = quick_loc_df(cbp['FIPS'], FIPS_to_FAFZone, 'FAFZone_4_2')
    assert not cbp['TAZ'].isnull().any()

    pipeline.replace_table('cbp', cbp)
