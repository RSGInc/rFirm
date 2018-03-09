# rFirm
# See full license in LICENSE.txt.

import logging
import os

from activitysim.core import pipeline
from activitysim.core import inject

from activitysim.core.config import setting

logger = logging.getLogger(__name__)


@inject.step()
def write_data_dictionary(output_dir):

    output_tables = pipeline.checkpointed_tables()

    # write data dictionary for all checkpointed_tables
    with open(os.path.join(output_dir, 'data_dict.csv'), 'a') as file:
        for table_name in output_tables:
            df = inject.get_table(table_name, None).to_frame()
            print >> file, "\n### %s (%s)\n" % (table_name, df.shape), df.dtypes


@inject.step()
def write_tables(output_dir):

    output_tables_settings_name = 'output_tables'

    output_tables_settings = setting(output_tables_settings_name)

    output_tables = pipeline.checkpointed_tables()

    if output_tables_settings is not None:

        action = output_tables_settings.get('action')
        tables = output_tables_settings.get('tables')

        if action not in ['include', 'skip']:
            raise "expected %s action '%s' to be either 'include' or 'skip'" % \
                  (output_tables_settings_name, action)

        if action == 'include':
            output_tables = tables
        elif action == 'skip':
            output_tables = [t for t in output_tables if t not in tables]

    # should provide option to also write checkpoints?
    # output_tables.append("checkpoints.csv")

    for table_name in output_tables:
        table = inject.get_table(table_name, None)

        if table is None:
            logger.warn("Skipping '%s': Table not found." % table_name)
            continue

        df = table.to_frame()
        file_name = "%s.csv" % table_name
        logger.info("writing output file %s" % file_name)
        file_path = os.path.join(output_dir, file_name)
        write_index = df.index.name is not None
        df.to_csv(file_path, index=write_index)
