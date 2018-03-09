# ActivitySim
# See full license in LICENSE.txt.

import os
import logging

from activitysim.core import inject
from activitysim.core.config import setting

logger = logging.getLogger(__name__)


@inject.injectable()
def scenarios_dir():

    scenarios_dir = setting('scenarios_dir', 'scenarios')

    if not os.path.exists(scenarios_dir):
        raise RuntimeError("scenarios_dir: directory does not exist")
    return scenarios_dir
