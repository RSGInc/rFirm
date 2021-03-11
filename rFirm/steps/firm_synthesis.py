# rFirm
# See full license in LICENSE.txt.

import logging
import os

import math
import collections
import itertools

import pandas as pd
import numpy as np
import scipy.interpolate as interpolate

from activitysim.core import inject
from activitysim.core import pipeline

from activitysim.core.tracing import print_elapsed_time

from activitysim.core.config import setting
from activitysim.core.util import reindex

from ortools.sat.python import cp_model

from rFirm.util import read_table
from rFirm.util import round_preserve_threshold
from rFirm.util import bucket_round

import rFirm.base_variables as base_variables

logger = logging.getLogger(__name__)


def est_sim_load_establishments(NAICS2012_to_NAICS2007):
    """
    For now, just read Establishments.cvs file created by dev/NFM_Establishmentsynthesis.R
    """

    data_dir = setting('data_dir', inject.get_injectable('data_dir'))

    table_list_name = 'est_synthesis_tables'
    table_list = setting(table_list_name)
    assert table_list is not None

    est = read_table('ests_establishments', table_list['ests_establishments'], data_dir)

    # FIXME for regression
    est = est.sort_index()

    # The ests table has 2012 naics code
    est['NAICS2012'] = est.naics

    assert est.NAICS2012.isin(NAICS2012_to_NAICS2007.NAICS2012).all()

    # make sure nobody is using this unawares
    est.rename(columns={'naics': 'pnaics'}, inplace=True)

    assert est.index.is_unique

    return est


def est_sim_enumerate(
        est,
        NAICS2012_to_NAICS2007io,
        employment_categories,
        naics_industry,
        industry_10_5):
    """

    enumerate ests

    """

    # - Look up NAICS2007io classifications
    t0 = print_elapsed_time()

    # Only use domestic ests
    est = est[est.FAF4 < 800].copy()

    # Merge in the single-NAICSio naics
    est['NAICS6_make'] = reindex(
        NAICS2012_to_NAICS2007io[NAICS2012_to_NAICS2007io.proportion == 1].
        set_index('NAICS').NAICSio, est.NAICS2012
    )

    t0 = print_elapsed_time("Merge in the single-NAICSio naics", t0, debug=True)

    # random select NAICSio 2007 code for multi 2012 naics code
    # based on proportions (probabilities)
    multi_NAICS2012_to_NAICS2007io = \
        NAICS2012_to_NAICS2007io[NAICS2012_to_NAICS2007io.proportion < 1]
    multi_naics2007io_naics2012_codes = multi_NAICS2012_to_NAICS2007io.NAICS.unique()
    multi_naics2007io_ests = est[est.NAICS2012.isin(multi_naics2007io_naics2012_codes)]

    prng = pipeline.get_rn_generator().get_global_rng()
    for naics, naics_ests in multi_naics2007io_ests.groupby('NAICS2012'):
        # slice the NAICS2012_to_NAICS2007io rows for this naics 2012 code
        naics_naicsio = \
            multi_NAICS2012_to_NAICS2007io[multi_NAICS2012_to_NAICS2007io.NAICS == naics]

        # choose a random NAICS2007io code for each business with this naics 2012 code
        naicsio = prng.choice(naics_naicsio.NAICSio.values,
                              size=len(naics_ests),
                              p=naics_naicsio.proportion.values,
                              replace=True)

        est.loc[naics_ests.index, 'NAICS6_make'] = naicsio

    t0 = print_elapsed_time("choose 2007 NAICSio for multi-2012 naics", t0, debug=True)

    if est.NAICS6_make.isnull().any():
        logger.error("%s null NAICS6_make codes in ests" % est.NAICS6_make.isnull().sum())

    # - Derive 2, 3, and 4 digit NAICS codes
    est['n4'] = (est.NAICS2012 / 100).astype(int)
    est['n3'] = (est.n4 / 10).astype(int)
    est['n2'] = (est.n3 / 10).astype(int)
    assert ~est.n2.isnull().any()

    # - recode esizecat map from str employment_categories.esizecat to int employment_categories.id
    est['esizecat'] = reindex(employment_categories.set_index('esizecat').id, est.esizecat)
    est['emp_range'] = reindex(employment_categories.set_index('id').emp_range, est.esizecat)
    est['low_emp'] = reindex(employment_categories.set_index('id').low_threshold, est.esizecat)

    # - Add 10 level industry classification
    est['industry10'] = reindex(naics_industry.industry, est.n3).astype(str)

    if est.industry10.isnull().any():
        unmatched_naics = est[est.industry10.isnull()].n3.unique()
        logger.error("%s unmatched n3 naics codes in naics_industry" % len(unmatched_naics))
        print "\nnon matching n3 naics codes\n", unmatched_naics
        est.industry10.fillna('', inplace=True)

    # - Add 5 level industry classification
    est['industry5'] = reindex(industry_10_5.industry5, est.industry10)

    if est.industry5.isnull().any():
        unmatched_industry_10 = est[est.industry5.isnull()].n3.unique()
        logger.error("%s unmatched industry_10 codes in industry_10_5" % len(unmatched_industry_10))
        print "\nnon matching industry_10 codes\n", unmatched_industry_10
        est.industry10.fillna('', inplace=True)

    assert est.index.is_unique

    return est


def est_sim_enumerate_foreign(est,
                              NAICS2012_to_NAICS2007io,
                              NAICS2007io_to_SCTG,
                              employment_categories,
                              naics_industry,
                              industry_10_5,
                              foreign_prod_values, foreign_cons_values):
    # Merge the IO codes
    foreign_prod_values = pd.merge(foreign_prod_values, NAICS2012_to_NAICS2007io.set_index('NAICS'), how='inner',
                                   left_on='NAICS6', right_index=True)
    foreign_cons_values = pd.merge(foreign_cons_values, NAICS2012_to_NAICS2007io.set_index('NAICS'), how='inner',
                                   left_on='NAICS6', right_index=True)

    # Update production and consumption values based on proportion observed
    foreign_prod_values['pro_val'] = foreign_prod_values['pro_val']*foreign_prod_values['proportion']
    foreign_cons_values['con_val'] = foreign_cons_values['con_val']*foreign_cons_values['proportion']

    # Reset the index
    foreign_prod_values = foreign_prod_values.reset_index(drop=True)
    foreign_cons_values = foreign_cons_values.reset_index(drop=True)

    # Both for_prod and for_cons include foreign public production/consumption value.
    # Reallocate within each country to the remaining privately owned industries in
    # proportion to their prod/cons value
    # In the future, research commodity shares for public production and consumption and
    # allocate in a more detailed way

    # Foreign production
    no_pub_codes = [910000, 920000, 980000, 990000]
    foreign_prod_sum = foreign_prod_values.groupby(['FAF4', 'TAZ']).agg({'pro_val': sum})
    # Private transactions do not appear to exist in the foreign trade data
    foreign_prod_sum_private = foreign_prod_values[~foreign_prod_values.NAICS6.isin(no_pub_codes)].\
        groupby(['FAF4', 'TAZ']).agg({'pro_val': sum})
    foreign_prod_sum = pd.merge(foreign_prod_sum, foreign_prod_sum_private,
                                how='outer', left_index=True, right_index=True,
                                suffixes=['', '_private'])
    foreign_prod_sum.pro_val_private.fillna(0, inplace=True)
    foreign_prod_sum = foreign_prod_sum.assign(prod_scale=lambda df: np.where(
        df.pro_val_private > 0,
        df.pro_val / df.pro_val_private,
        0))

    # Account for countries with no non-public production by reallocating within FAF ZONE
    # so FAF ZONE production is conserved
    foreign_prod_faf = foreign_prod_sum.reset_index().groupby(['FAF4']). \
        apply(lambda dfg: dfg.pro_val.sum() / dfg.pro_val_private.sum()).to_frame('prod_scale')

    # Do the scaling for foreign production
    foreign_prod_values = foreign_prod_values[~foreign_prod_values.NAICS6.isin(no_pub_codes)].copy()
    foreign_prod_values['pro_val_new'] = (foreign_prod_values.pro_val.values *
                                          foreign_prod_sum.reset_index('FAF4', drop=True).
                                          loc[foreign_prod_values.TAZ.values,
                                              'prod_scale'].values *
                                          foreign_prod_faf.loc[foreign_prod_values.FAF4.values,
                                                               'prod_scale'].values)

    # Foreign consumption
    foreign_cons_sum = foreign_cons_values.groupby(['FAF4', 'TAZ']).agg({'con_val': sum})
    # Private transactions do not appear to exist in the foreign trade data
    foreign_cons_sum_private = \
        foreign_cons_values[~foreign_cons_values.NAICS6.isin(no_pub_codes)]. \
        groupby(['FAF4', 'TAZ']).agg({'con_val': sum})
    foreign_cons_sum = pd.merge(foreign_cons_sum, foreign_cons_sum_private,
                                how='outer', left_index=True, right_index=True,
                                suffixes=['', '_private'])
    foreign_cons_sum.con_val_private.fillna(0, inplace=True)
    foreign_cons_sum = \
        foreign_cons_sum.assign(con_scale=lambda df: np.where(df.con_val_private > 0,
                                                              df.con_val / df.con_val_private,
                                                              0))

    # Account for countries with no non-public consumption by reallocating within FAF ZONE
    # so FAF ZONE consumption is conserved
    foreign_cons_faf = foreign_cons_sum.reset_index().groupby(['FAF4']). \
        apply(lambda dfg: dfg.con_val.sum() / dfg.con_val_private.sum()).to_frame('con_scale')

    # Do the scaling for foreign consumption
    foreign_cons_values = foreign_cons_values[~foreign_cons_values.NAICS6.isin(no_pub_codes)].copy()
    foreign_cons_values['con_val_new'] = (foreign_cons_values.con_val.values *
                                          foreign_cons_sum.reset_index('FAF4', drop=True).
                                          loc[foreign_cons_values.TAZ.values,
                                              'con_scale'].values *
                                          foreign_cons_faf.loc[foreign_cons_values.FAF4.values,
                                                               'con_scale'].values)

    # Enumerate foreign producers and consumers
    # Create one agent per country per commodity
    # Remove any records where the country and FAF zone are not known
    foreign_producers = foreign_prod_values[~foreign_prod_values.FAF4.isnull()][
        ['NAICS6', 'NAICSio', 'pro_val_new', 'TAZ', 'FAF4']
    ].rename(columns={'pro_val_new': 'prod_val'}).assign(prod_val=lambda df: df.prod_val / 1e6)
    foreign_consumers = foreign_cons_values[~foreign_cons_values.FAF4.isnull()][
        ['NAICS6', 'NAICSio', 'con_val_new', 'TAZ', 'FAF4']
    ].rename(columns={'con_val_new': 'prod_val'}).assign(prod_val=lambda df: df.prod_val / 1e6)

    # # - Derive 2, 3, and 4 digit NAICS codes
    # foreign_producers['n4'] = (foreign_producers.NAICS6 / 100).astype(int)
    # foreign_producers['n3'] = (est.n4 / 10).astype(int)
    # foreign_producers['n2'] = (est.n3 / 10).astype(int)

    # foreign_consumers['n4'] = (foreign_consumers.NAICS6 / 100).astype(int)
    # foreign_consumers['n3'] = (est.n4 / 10).astype(int)
    # foreign_consumers['n2'] = (est.n3 / 10).astype(int)

    # Merge in the I/O NAICS codes and SCTG codes
    # Merge in the single-SCTG naics
    foreign_producers['SCTG'] = reindex(
        NAICS2007io_to_SCTG[NAICS2007io_to_SCTG.proportion == 1].set_index('NAICSio').SCTG,
        foreign_producers.NAICSio
    )
    foreign_consumers['SCTG'] = reindex(
        NAICS2007io_to_SCTG[NAICS2007io_to_SCTG.proportion == 1].set_index('NAICSio').SCTG,
        foreign_consumers.NAICSio
    )
    multi_NAICS2007io_to_SCTG = NAICS2007io_to_SCTG[NAICS2007io_to_SCTG.proportion < 1]
    multi_sctg_naics_codes = multi_NAICS2007io_to_SCTG.NAICSio.unique()
    multi_sctg_producers = foreign_producers[foreign_producers.NAICSio.isin(multi_sctg_naics_codes)]
    multi_sctg_consumers = foreign_consumers[foreign_consumers.NAICSio.isin(multi_sctg_naics_codes)]

    prng = pipeline.get_rn_generator().get_global_rng()
    for naics, naics_ests in multi_sctg_producers.groupby('NAICSio'):
        # slice the NAICS2007io_to_SCTG rows for this naics
        naics_sctgs = multi_NAICS2007io_to_SCTG[multi_NAICS2007io_to_SCTG.NAICSio == naics]

        # choose a random SCTG code for each business with this naics code
        sctgs = prng.choice(naics_sctgs.SCTG.values,
                            size=len(naics_ests),
                            p=naics_sctgs.proportion.values,
                            replace=True)

        foreign_producers.loc[naics_ests.index, 'SCTG'] = sctgs

    for naics, naics_ests in multi_sctg_consumers.groupby('NAICSio'):
        # slice the NAICS2007io_to_SCTG rows for this naics
        naics_sctgs = multi_NAICS2007io_to_SCTG[multi_NAICS2007io_to_SCTG.NAICSio == naics]
        
        # choose a random SCTG code for each business with this naics code
        sctgs = prng.choice(naics_sctgs.SCTG.values,
                            size=len(naics_ests),
                            p=naics_sctgs.proportion.values,
                            replace=True)

        foreign_consumers.loc[naics_ests.index, 'SCTG'] = sctgs

    foreign_producers['n4'] = (foreign_producers.NAICS6 / 100).astype(int)
    foreign_consumers['n4'] = (foreign_consumers.NAICS6 / 100).astype(int)

    KeyMap = collections.namedtuple('KeyMap',
                                    ['key_col', 'key_value', 'target', 'target_values', 'probs'])
    # Identify ests who make 2+ commodities (especially wholesalers) and simulate a specific
    # commodity for them based on probability thresholds for multiple commodities
    keymaps = [
        # TODO: are these the right correspondences given the I/O data for the current project?
        KeyMap('NAICS6', 211111, 'SCTG', (16L, 19L), (.45, .55)),
        # Crude Petroleum and Natural Gas Extraction: Crude petroleum; Coal and petroleum products, n.e.c.            # nopep8
        KeyMap('NAICS6', 324110, 'SCTG', (17L, 18L, 19L), (.25, .25, .50)),
        # Petroleum Refineries: Gasoline and aviation turbine fuel; Fuel oils; Coal and petroleum products, n.e.c.    # nopep8
    ]

    for keymap in keymaps:
        flag_rows = (foreign_producers[keymap.key_col] == keymap.key_value)
        if np.isscalar(keymap.target_values):
            foreign_producers.loc[flag_rows, keymap.target] = keymap.target_values
        else:
            assert len(keymap.target_values) == len(keymap.probs)
            temp_fp = foreign_producers.loc[flag_rows].copy()
            prod_val = temp_fp['prod_val'].copy()
            return_df = pd.DataFrame(columns=temp_fp.columns)
            for target_value, prob in zip(keymap.target_values, keymap.probs):
                temp_fp.loc[:, keymap.target] = target_value
                temp_fp.loc[:, 'prod_val'] = prod_val * prob
                return_df = pd.concat([return_df, temp_fp], ignore_index=True)
            foreign_producers = pd.concat([foreign_producers.loc[~flag_rows], return_df],
                                          ignore_index=True)

    for keymap in keymaps:
        flag_rows = (foreign_consumers[keymap.key_col] == keymap.key_value)
        if np.isscalar(keymap.target_values):
            foreign_consumers.loc[flag_rows, keymap.target] = keymap.target_values
        else:
            assert len(keymap.target_values) == len(keymap.probs)
            temp_fp = foreign_consumers.loc[flag_rows].copy()
            prod_val = temp_fp['prod_val'].copy()
            return_df = pd.DataFrame(columns=temp_fp.columns)
            for target_value, prob in zip(keymap.target_values, keymap.probs):
                temp_fp.loc[:, keymap.target] = target_value
                temp_fp.loc[:, 'prod_val'] = prod_val * prob
                return_df = pd.concat([return_df, temp_fp], ignore_index=True)
            foreign_consumers = pd.concat([foreign_consumers.loc[~flag_rows], return_df],
                                          ignore_index=True)

    assert foreign_producers.index.is_unique
    assert foreign_producers.index.is_unique

    foreign_producers['producer'] = True
    foreign_consumers['producer'] = False

    ests_foreign = foreign_producers.append(foreign_consumers, ignore_index=True)
    ests_foreign.rename(columns={'NAICS6': 'NAICS2007', 'NAICSio': 'NAICS6_make'},
                        inplace=True)
    ests_foreign = ests_foreign[~ests_foreign.NAICS6_make.isnull()].copy()
    # - Derive 2, 3, and 4 digit NAICS codes
    ests_foreign['n4'] = (ests_foreign.NAICS2007 / 100).astype(int)
    ests_foreign['n3'] = (ests_foreign.n4 / 10).astype(int)
    ests_foreign['n2'] = (ests_foreign.n3 / 10).astype(int)
    assert ~ests_foreign.n2.isnull().any()

    # - code esizecat
    ests_foreign['esizecat'] = est.esizecat.max()
    ests_foreign['emp_range'] = reindex(employment_categories.set_index('id').emp_range,
                                        ests_foreign.esizecat)
    ests_foreign['low_emp'] = reindex(employment_categories.set_index('id').low_threshold,
                                      ests_foreign.esizecat)

    # - Add 10 level industry classification
    ests_foreign['industry10'] = reindex(naics_industry.industry, ests_foreign.n3).astype(str)

    if ests_foreign.industry10.isnull().any():
        unmatched_naics = ests_foreign[ests_foreign.industry10.isnull()].n3.unique()
        logger.error("%s unmatched n3 naics codes in naics_industry" % len(unmatched_naics))
        print "\nnon matching n3 naics codes\n", unmatched_naics
        ests_foreign.industry10.fillna('', inplace=True)

    # - Add 5 level industry classification
    ests_foreign['industry5'] = reindex(industry_10_5.industry5, ests_foreign.industry10)

    if ests_foreign.industry5.isnull().any():
        unmatched_industry_10 = ests_foreign[ests_foreign.industry5.isnull()].n3.unique()
        logger.error("%s unmatched industry_10 codes in industry_10_5" % len(unmatched_industry_10))
        print "\nnon matching industry_10 codes\n", unmatched_industry_10
        ests_foreign.industry10.fillna('', inplace=True)

    group_by_keys = ['NAICS6_make', 'TAZ', 'FAF4', 'SCTG', 'producer', 'industry10',
                     'industry5', 'esizecat', 'low_emp', 'emp_range']
    ests_foreign = ests_foreign.groupby(group_by_keys,
                                        as_index=False).agg({'prod_val': sum})

    # Reindex foreign ests
    max_bus_id = est.index.max()
    logger.info("assigning foreign est indexes starting above MAX_BUS_ID %s" % (max_bus_id,))
    ests_foreign.index = ests_foreign.index + max_bus_id + 1L
    ests_foreign.index.name = est.index.name

    est = est.append(ests_foreign) #pd.concat([est, ests_foreign])
    est.loc[est.FAF4 > 800, 'county_FIPS'] = 0
    est.loc[est.FAF4 > 800, 'state_FIPS'] = 0

    assert est.index.is_unique
    return est


def est_sim_taz_allocation(est, naics_empcat):
    # most of the R code serves no purpose when there is only one level of TAZ

    # - Assign the model employment category to each domestic est
    domestic_index = (est.FAF4 < 800)
    est.loc[domestic_index, 'model_emp_cat'] = reindex(naics_empcat.model_emp_cat,
                                                       est.loc[domestic_index, 'n2'])

    assert ~est.loc[domestic_index].model_emp_cat.isnull().any()

    assert est.index.is_unique

    return est


def scale_cbp_to_se(employment, est, use_bucketround=False):
    employment['adjustment'] = employment.emp_SE / employment.emp_CBP
    employment.adjustment.replace(np.inf, np.nan, inplace=True)

    # add adjustment factor for each est based on its TAZ and model_emp_cat
    on_cols = ['TAZ', 'model_emp_cat']
    index_name = est.index.name or 'index'
    est = pd.merge(
        left=est.reset_index(),
        right=employment[on_cols + ['adjustment']],
        on=on_cols).set_index(index_name)

    # - Scale employees in ests table and bucket round
    # FIXME any reason not to replace bucket round with target round
    est.adjustment = est.adjustment.fillna(1.0)  # adjustment factor of 1.0 has no effect
    est.emp = est.emp * est.adjustment
    if use_bucketround:
        est.emp = bucket_round(est.emp.values)
    else:
        est.emp = round_preserve_threshold(est.emp.values)

    del est['adjustment']
    del employment['adjustment']
    return est


def est_emp_generator(
        low_emp,
        emp_range,
        interpolate_model,
        est_length=1
):
    """
    Generates employee number based on values provided randomly
    :param est_length:
    :param low_emp:
    :param emp_range:
    :param interpolate_model:
    :return: ests_emp
    """
    max_emp = low_emp + emp_range
    emp_n = [i for i in xrange(low_emp, max_emp)]
    inter_val = interpolate_model(emp_n)
    prob_emp = inter_val / np.sum(inter_val)
    prng = pipeline.get_rn_generator().get_global_rng()
    est_emp = prng.choice(emp_n,
                          size=est_length,
                          replace=True,
                          p=prob_emp)
    return est_emp


def est_sim_scale_employees(
        est,
        employment_categories,
        naics_empcat,
        socio_economics_taz,        
        taz_fips,
        taz_faf4):
    """
     Scale ests to Employment Forecasts
    """

    # Separate out the foreign ests
    est_foreign = est[est.FAF4 > 800]
    est = est[est.FAF4 < 800].copy()

    # Assign employment based on the proportion of ests obtained by
    # interpolating number of ests between low and high value of
    # employment range

    # number of establishments by employment size category
    est_size = est.groupby(['esizecat', 'emp_range', 'low_emp']).size().to_frame('est_n'). \
        reset_index()
    est_size['est_emp_range'] = est_size['est_n'] / est_size['emp_range']
    # Hyman interpolator to interpolate number of establishments between the low and
    # high value of the employment range
    hyman_interpol = interpolate.PchipInterpolator(est_size['low_emp'],
                                                   est_size['est_emp_range'])

    est_emp = est.groupby(['esizecat', 'low_emp', 'emp_range'],
                          group_keys=False) \
        .apply(lambda x: {'emp': est_emp_generator(x.low_emp.unique(),
                                                   x.emp_range.unique(),
                                                   hyman_interpol,
                                                   x.shape[0]),
                          'bus_id': x.index.tolist()})
    est_emp = pd.concat([pd.DataFrame.from_dict(x) for x in est_emp.to_frame('est_emp').
                        reset_index().est_emp.tolist()],
                        axis=0)
    # ests_emp.loc[ests_emp.emp > 5000, 'emp'] = 5000
    est_emp.set_index('bus_id', inplace=True)
    est.loc[est_emp.index, 'emp'] = est_emp.emp.values

    # employment categories that
    model_emp_cats = naics_empcat.model_emp_cat.unique()

    # we expect all lehd_naics_empcat categories to appear as columns in socio_economics_taz
    assert set(model_emp_cats).issubset(set(socio_economics_taz.columns.values))

    # just want those columns plus TAZ so we can melt
    socio_economics_taz = socio_economics_taz[model_emp_cats]

    # columns that est_sim_scale_employees_taz (allegedly) expects in ests
    region_est_cols = \
        ['TAZ', 'county_FIPS', 'state_FIPS', 'FAF4', 'NAICS2012', 'n4', 'n3', 'n2',
         'NAICS6_make', 'industry10', 'ind'
                                      'ustry5', 'model_emp_cat', 'esizecat',
         'emp']

    est = est[region_est_cols].copy()
    MAX_BUS_ID = est.index.max()
    logger.info("assigning new est indexes starting above MAX_BUS_ID %s" % (MAX_BUS_ID,))

    # - Compare employment between socio-economic input (SE) and synthesized ests (CBP)

    # Summarize employment of synthesized ests by TAZ and Model_EmpCat
    employment = est.groupby(['TAZ', 'model_emp_cat'])['emp'].sum()
    employment = employment.to_frame(name='emp_CBP').reset_index()
    # Melt socio_economics_taz employment data by TAZ and employment category
    employment_SE = socio_economics_taz.reset_index(). \
        melt(id_vars='TAZ', var_name='model_emp_cat', value_name='emp_SE')
    # inner join
    employment = employment.merge(right=employment_SE, how='outer', on=['TAZ', 'model_emp_cat'])

    # FIXME should we leave this nan to distinguish zero counts from missing data?
    employment.emp_SE.fillna(0, inplace=True)
    employment.emp_CBP.fillna(0, inplace=True)

    # drop rows where both employment sources say there should be no employment in empcat
    employment = employment[~((employment.emp_SE == 0) & (employment.emp_CBP == 0))]

    # Re-allocation of TAZ to establishments for better scaling
    # 1. First relocate ests to TAZ where CBP has 0 employment and SE has more than 0
    #    employment.
    # 2. Final relocate ests from TAZ where the CBP number of establishments is more than
    #    total SE employment.

    employment = pd.merge(employment,
                          est.groupby(['TAZ', 'model_emp_cat']).size().
                          to_frame('n_est').reset_index(),
                          on=['TAZ', 'model_emp_cat'],
                          how='outer').fillna(0)
    employment['avg_emp'] = reindex(est.groupby('model_emp_cat')['emp'].median(),
                                    employment.model_emp_cat)

    # FIXME Steps
    # Identify TAZ to be assigned
    # Identify TAZ that has the capacity to distribute
    # Identify how many to move to TAZ that has zero CBP employment
    # Loop over model employment category until TAZ identified in step 1 has establishments
    # Select establishments randomly based on probability determined by available
    # employment (over SE requirement) and distance.
    # Recalculate TAZ level statistics based on relocated establishments and continue.

    # # Identify TAZ and model_emp_cat that needs to be reallocated somewhere else
    # employment['to_assign'] = employment['emp_CBP'] < 1
    # employment['to_distribute'] = (~(employment['to_assign']) & ((employment['n_est']) > 1))
    # employment['to_move'] = (employment['emp_SE'] / employment['avg_emp']).apply(np.ceil)
    # employment.loc[employment['emp_CBP'] > 0, 'to_move'] = 0.0

    # Employment Category Absent from CBP
    emp_cats_not_in_ests = ['gov']

    # rem_assign = employment[~employment.model_emp_cat.isin(emp_cats_not_in_ests)].to_assign.sum()
    # numa_dist = numa_dist[numa_dist.oTAZ != numa_dist.dTAZ]
    # est['orig_TAZ'] = est['TAZ']

    # t0 = print_elapsed_time()

    # prng = pipeline.get_rn_generator().get_global_rng()
    # ITER_MAX = 3
    # ONLY_ONE = False
    # while rem_assign > 0:
    #     for model_emp_cat in employment.model_emp_cat[employment.to_assign].unique():
    #         if model_emp_cat in emp_cats_not_in_ests:
    #             continue
    #         print 'Adjusting employment category: %s\n' % model_emp_cat
    #         # Get a subset of data to work with
    #         est_sub = est[est.model_emp_cat == model_emp_cat].copy()
    #         emp_taz = employment[employment.model_emp_cat == model_emp_cat]
    #         est_sub['to_move'] = reindex(emp_taz.set_index('TAZ')['to_move'], est_sub.TAZ)
    #         emp_assign = emp_taz[emp_taz.to_assign]
    #         emp_assign['dist_threshold'] = 25
    #         emp_assign['SE_adjust_factor'] = 1
    #         emp_distribute = emp_taz[emp_taz.to_distribute]
    #         emp_avg_adj_factor = 1.0
    #         status = cp_model.INFEASIBLE
    #         current_iter = 1
    #         if (emp_assign.shape[0] > 0) & (emp_distribute.shape[0] > 0):
    #             numa_dist_sub = numa_dist[(numa_dist.dTAZ.isin(emp_assign.TAZ))]
    #             numa_dist_sub['dist_threshold'] = reindex(emp_assign.set_index('TAZ')['dist_threshold'], numa_dist_sub.dTAZ)  
    #             numa_dist_sub['added'] = False
    #             add_TAZ_pair = numa_dist_sub[(numa_dist_sub.distance < numa_dist_sub.dist_threshold) & (~numa_dist_sub.added)][['oTAZ', 'dTAZ', 'distance']]         
    #             est_sub2 = pd.merge(add_TAZ_pair,
    #                                 est_sub[['TAZ', 'emp']].reset_index(),
    #                                 how='inner',
    #                                 left_on='oTAZ',
    #                                 right_on='TAZ').set_index('bus_id')
    #             numa_dist_sub.loc[(numa_dist_sub.distance < numa_dist_sub.dist_threshold) & (~numa_dist_sub.added), 'added'] = True
    #             while status <> cp_model.OPTIMAL:
    #                 if current_iter > 1:
    #                     add_TAZ_pair = numa_dist_sub[(numa_dist_sub.distance < numa_dist_sub.dist_threshold) & (~numa_dist_sub.added)][['oTAZ', 'dTAZ', 'distance']]
    #                     est_sub3 = pd.merge(add_TAZ_pair,
    #                                         est_sub[['TAZ', 'emp']].reset_index(),
    #                                         how='inner',
    #                                         left_on='oTAZ',
    #                                         right_on='TAZ').set_index('bus_id')
    #                     numa_dist_sub.loc[(numa_dist_sub.distance < numa_dist_sub.dist_threshold) & (~numa_dist_sub.added), 'added'] = True
    #                     est_sub2 = est_sub2.drop('relocate', axis=1).append(est_sub3)
    #                 emp_avail = est_sub2.groupby('dTAZ')['emp'].agg('sum')
    #                 emp_assign['emp_avail'] = reindex(emp_avail, emp_assign.TAZ)
    #                 # check_size = (emp_assign.emp_avail.isna()).any() | ((emp_assign.emp_SE * emp_assign.SE_adjust_factor)>= emp_assign.emp_avail).any()
    #                 check_size = (emp_assign.emp_avail.isna()).any() | ((emp_assign.emp_avail/emp_assign.emp_SE) <= emp_assign.avg_emp).any()
    #                 while check_size:
    #                     emp_assign.loc[emp_assign.emp_avail.isna(),'dist_threshold'] += 25
    #                     # emp_assign.loc[emp_assign.emp_avail <= emp_assign.emp_SE,'dist_threshold'] += 25
    #                     emp_assign.loc[(emp_assign.emp_avail/emp_assign.emp_SE) <= (emp_assign.avg_emp*emp_avg_adj_factor),'dist_threshold'] += 25
    #                     # emp_assign.loc[emp_assign.dist_threshold > 500, 'SE_adjust_factor'] /= 1.125
    #                     numa_dist_sub['dist_threshold'] = reindex(emp_assign.set_index('TAZ')['dist_threshold'], numa_dist_sub.dTAZ)
    #                     add_TAZ_pair = numa_dist_sub[(numa_dist_sub.distance < numa_dist_sub.dist_threshold) & (~numa_dist_sub.added)][['oTAZ', 'dTAZ', 'distance']]
    #                     est_sub3 = pd.merge(add_TAZ_pair,
    #                                     est_sub[['TAZ', 'emp']].reset_index(),
    #                                     how='inner',
    #                                     left_on='oTAZ',
    #                                     right_on='TAZ').set_index('bus_id')
    #                     numa_dist_sub.loc[(numa_dist_sub.distance < numa_dist_sub.dist_threshold) & (~numa_dist_sub.added), 'added'] = True
    #                     est_sub2 = est_sub2.drop('relocate', axis=1, errors='ignore').append(est_sub3)
    #                     emp_avail = est_sub2.groupby('dTAZ')['emp'].agg('sum')
    #                     emp_assign['emp_avail'] = reindex(emp_avail, emp_assign.TAZ)
    #                     # check_size = (emp_assign.emp_avail.isna()).any() | ((emp_assign.emp_SE * emp_assign.SE_adjust_factor)>= emp_assign.emp_avail).any()
    #                     check_size = (emp_assign.emp_avail.isna()).any() | ((emp_assign.emp_avail/emp_assign.emp_SE) <= emp_assign.avg_emp).any()
    #                 print 'Distance threshold used: %.2f' % emp_assign.dist_threshold.mean()
    #                 model = cp_model.CpModel()
    #                 relocate_const = est_sub2['dTAZ'].reset_index().apply(lambda df: model.NewBoolVar('x[%i,%i]' % (df.bus_id, df.dTAZ)), axis=1)
    #                 relocate_const.index = est_sub2.index
    #                 est_sub2['relocate'] = relocate_const
    #                 se_emp_dtaz_const = est_sub2.groupby('dTAZ')[['relocate','emp']].apply(lambda df: sum(list(df.relocate * df.emp))).to_frame('se_emp_dtaz_const')
    #                 # se_emp_dtaz_const = est_sub2.groupby('dTAZ')[['relocate','emp']].apply(lambda df: sum(list(df.relocate))).to_frame('se_emp_dtaz_const')
    #                 # se_emp_dtaz_const['emp_constraint'] = [model.Add(se_emp_dtaz_const.loc[TAZ,'se_emp_dtaz_const'] >= emp_assign.loc[emp_assign.TAZ==TAZ,'to_move'].values[0].astype('int')) for TAZ in se_emp_dtaz_const.index]
    #                 if (current_iter <= ITER_MAX) & (not ONLY_ONE):
    #                     se_emp_dtaz_const['emp_constraint'] = [model.Add(se_emp_dtaz_const.loc[TAZ,'se_emp_dtaz_const'] >= np.ceil(emp_assign.loc[emp_assign.TAZ==TAZ,'emp_SE'].values[0] * emp_assign.loc[emp_assign.TAZ==TAZ,'SE_adjust_factor'].values[0]).astype('int')) for TAZ in se_emp_dtaz_const.index]
    #                 else:
    #                     se_emp_dtaz_const['emp_constraint'] = [model.Add(se_emp_dtaz_const.loc[TAZ,'se_emp_dtaz_const'] >= 1) for TAZ in se_emp_dtaz_const.index]
    #                 se_emp_otaz_const = est_sub2.groupby('oTAZ')[['relocate','emp']].apply(lambda df: sum(list(df.relocate))).to_frame('se_emp_otaz_const')
    #                 se_emp_otaz_const['emp_constraint'] = [model.Add(se_emp_otaz_const.loc[TAZ,'se_emp_otaz_const'] <= emp_taz.loc[emp_taz.TAZ==TAZ,'n_est'].values[0].astype('int')-1) for TAZ in se_emp_otaz_const.index]
    #                 bus_const = est_sub2.reset_index().groupby('bus_id')[['relocate']].apply(lambda df: model.Add(sum(list(df.relocate.values)) <= 1)).to_frame('variables')
    #                 obj = sum([x*c for x, c in zip(est_sub2.distance.astype('int').values,est_sub2.relocate.values)])
    #                 model.Minimize(obj)
    #                 solver = cp_model.CpSolver()
    #                 solver.parameters.log_search_progress = True
    #                 solver.parameters.num_search_workers = 24
    #                 solution_printer = cp_model.ObjectiveSolutionPrinter()
    #                 status = solver.SolveWithSolutionCallback(model, solution_printer)                    
    #                 # emp_assign['dist_threshold'] +=25 
    #                 # numa_dist_sub['dist_threshold'] +=25
    #                 emp_avg_adj_factor *= 2.0
    #                 emp_assign.loc[(emp_assign.emp_avail/emp_assign.emp_SE) <= (emp_assign.avg_emp*emp_avg_adj_factor),'dist_threshold'] += 25
    #                 numa_dist_sub['dist_threshold'] = reindex(emp_assign.set_index('TAZ')['dist_threshold'], numa_dist_sub.dTAZ)
    #                 current_iter += 1
    #                 # emp_assign.loc[emp_assign.dist_threshold > 500, 'SE_adjust_factor'] /= 1.125
    #             est_sub2['relocated'] = est_sub2.apply(lambda df: solver.Value(df.relocate), axis=1)
    #             est_moved = est_sub2[est_sub2.relocated==1]
    #             est.loc[est_moved.index, 'TAZ'] = est_moved['dTAZ'].astype(int).values                
    #     employment = est.groupby(['TAZ', 'model_emp_cat'])['emp'].sum()
    #     employment = employment.to_frame(name='emp_CBP').reset_index()
    #     # Melt socio_economics_taz employment data by TAZ and employment category
    #     employment_SE = socio_economics_taz.reset_index(). \
    #         melt(id_vars='TAZ', var_name='model_emp_cat', value_name='emp_SE')
    #     # inner join
    #     employment = employment.merge(right=employment_SE, how='outer', on=['TAZ', 'model_emp_cat'])

    #     # FIXME should we leave this nan to distinguish zero counts from missing data?
    #     employment.emp_SE.fillna(0, inplace=True)
    #     employment.emp_CBP.fillna(0, inplace=True)

    #     # Re-allocation of TAZ to establishments for better scaling
    #     employment = pd.merge(employment,
    #                           est.groupby(['TAZ', 'model_emp_cat']).size().
    #                           to_frame('n_est').reset_index(),
    #                           on=['TAZ', 'model_emp_cat'],
    #                           how='outer').fillna(0)

    #     # Identify TAZ and model_emp_cat that needs to be reallocated somewhere else
    #     employment = employment[~((employment.emp_SE == 0) & (employment.emp_CBP == 0))].copy()
    #     employment['to_assign'] = employment['emp_CBP'] < 1
    #     employment['to_distribute'] = (~(employment['to_assign']) & ((employment['n_est']) > 1))
    #     employment['avg_emp'] = reindex(est.groupby('model_emp_cat')['emp'].median(),
    #                                     employment.model_emp_cat)
    #     employment['to_move'] = (employment['emp_SE'] / employment['avg_emp']).apply(np.ceil)
    #     employment.loc[employment['emp_CBP'] > 0, 'to_move'] = 0.0

    #     # drop rows where both employment sources say there should be no employment in empcat
    #     rem_assign = employment[~employment.model_emp_cat.isin(emp_cats_not_in_ests)]. \
    #         to_assign.sum()
    #     print 'Establishments remaining to be moved :%d\n' % rem_assign

    # logger.info("%d ests relocated." % (est[est.orig_TAZ != est.TAZ].shape[0]))

    # t0 = print_elapsed_time("est_sim_scale_employees relocating to zero CBP \nzero+ SE NUMA",
    #                         t0,
    #                         debug=True)

    # # FIXME Steps
    # # Identify TAZ that has the capacity to distribute
    # # Identify TAZ to be assigned
    # # Identify how many to move from the TAZ
    # # Loop over model employment category until TAZ identified in step 1 has establishments
    # # less than or equal to employment.
    # # Select establishments randomly based on probability determined by expected number
    # # of employee required to match SE data and distance.
    # # Recalculate TAZ level statistics based on relocated establishments and continue.

    # # Identify TAZ and model_emp_cat that needs to be reallocated somewhere else
    # emp_to_check = 'emp_SE'
    # employment['to_distribute'] = employment['n_est'] > employment[emp_to_check]
    # employment['to_assign'] = ~employment['to_distribute']
    # employment['avg_emp'] = reindex(est.groupby('model_emp_cat')['emp'].median(),
    #                                 employment.model_emp_cat)
    # employment['to_move'] = employment['n_est'] - employment[emp_to_check]
    # employment.loc[employment.to_move < 0.0, 'to_move'] = 0.0

    # rem_distribute = employment.to_distribute.sum()
    # while rem_distribute > 0:
    #     for model_emp_cat in employment[employment.to_distribute].model_emp_cat.unique():
    #         print 'Adjusting employment category: %s\n' % model_emp_cat
    #         est_sub = est[est.model_emp_cat == model_emp_cat].copy()
    #         emp_taz = employment[employment.model_emp_cat == model_emp_cat]
    #         emp_assign = emp_taz[emp_taz.to_assign]
    #         emp_assign['emp_reqd'] = (emp_assign.emp_SE - emp_assign.emp_CBP)
    #         emp_distribute = emp_taz[emp_taz.to_distribute]
    #         emp_distribute['dist_threshold'] = 25
    #         # emp_distribute['SE_adjust_factor'] = 1
    #         # emp_avg_adj_factor = 1.0
    #         status = cp_model.INFEASIBLE
    #         current_iter = 1
    #         if emp_distribute.shape[0] > 0:
    #             emp_reqd = (emp_assign[emp_assign.emp_reqd > 0].emp_reqd.sum())
    #             emp_distr = (emp_distribute.emp_CBP-emp_distribute.emp_SE).sum()
    #             include_high_ind = emp_reqd < emp_distr
    #             numa_dist_sub = numa_dist[(numa_dist.dTAZ.isin(emp_assign.TAZ))]
    #             numa_dist_sub['dist_threshold'] = reindex(emp_distribute.set_index('TAZ')['dist_threshold'], numa_dist_sub.oTAZ)  
    #             numa_dist_sub['added'] = False
    #             add_TAZ_pair = numa_dist_sub[(numa_dist_sub.distance < numa_dist_sub.dist_threshold) & (~numa_dist_sub.added)][['oTAZ', 'dTAZ', 'distance']]         
    #             est_sub2 = pd.merge(add_TAZ_pair,
    #                                 est_sub[['TAZ', 'emp']].reset_index(),
    #                                 how='inner',
    #                                 left_on='oTAZ',
    #                                 right_on='TAZ').set_index('bus_id')
    #             numa_dist_sub.loc[(numa_dist_sub.distance < numa_dist_sub.dist_threshold) & (~numa_dist_sub.added), 'added'] = True
    #             while status <> cp_model.OPTIMAL:
    #                 if current_iter > 1:
    #                     if (emp_assign.emp_reqd > 0).any() & (not include_high_ind):
    #                         add_TAZ_pair = numa_dist_sub[(numa_dist_sub.distance < numa_dist_sub.dist_threshold) & (~numa_dist_sub.added) & (numa_dist_sub.dTAZ.isin(emp_assign[emp_assign.emp_reqd > 0].TAZ))][['oTAZ', 'dTAZ', 'distance']]
    #                     else:
    #                         add_TAZ_pair = numa_dist_sub[(numa_dist_sub.distance < numa_dist_sub.dist_threshold) & (~numa_dist_sub.added)][['oTAZ', 'dTAZ', 'distance']]
    #                     est_sub3 = pd.merge(add_TAZ_pair,
    #                                         est_sub[['TAZ', 'emp']].reset_index(),
    #                                         how='inner',
    #                                         left_on='oTAZ',
    #                                         right_on='TAZ').set_index('bus_id')
    #                     numa_dist_sub.loc[(numa_dist_sub.distance < numa_dist_sub.dist_threshold) & (~numa_dist_sub.added), 'added'] = True
    #                     est_sub2 = est_sub2.drop('relocate', axis=1).append(est_sub3)
    #                 # emp_move = est_sub2.groupby('dTAZ')['emp'].agg('sum')
    #                 # emp_assign['emp_move'] = reindex(emp_move, emp_assign.TAZ)
    #                 emp_move = est_sub[est_sub.TAZ.isin(est_sub2.oTAZ.unique())].groupby('TAZ')['emp'].agg('sum')
    #                 emp_distribute['emp_move'] = reindex(emp_move, emp_distribute.TAZ)
    #                 emp_distribute['emp_move'] = emp_distribute.emp_move - emp_distribute.emp_SE
    #                 # check_size = (emp_distribute.emp_move.isna()).any() | ((emp_assign.emp_SE * emp_assign.SE_adjust_factor)>= emp_assign.emp_avail).any()
    #                 # check_size = (emp_distribute.emp_move.isna()).any() | ((emp_distribute.emp_CBP - emp_distribute.emp_move) >= emp_distribute.emp_SE).any()
    #                 check_size = (emp_distribute.emp_move.isna()).any() | (emp_assign[emp_assign.emp_reqd > 0].emp_reqd.sum() < emp_distribute.emp_move.sum()).any()
    #                 while check_size:
    #                     emp_distribute.loc[emp_distribute.emp_move.isna(),'dist_threshold'] += 25
    #                     # emp_assign.loc[emp_assign.emp_avail <= emp_assign.emp_SE,'dist_threshold'] += 25
    #                     # emp_distribute.loc[(emp_distribute.emp_CBP - emp_distribute.emp_move) >= emp_distribute.emp_SE,'dist_threshold'] += 25
    #                     # emp_assign.loc[emp_assign.dist_threshold > 500, 'SE_adjust_factor'] /= 1.125
    #                     if (emp_assign[emp_assign.emp_reqd > 0].emp_reqd.sum() < emp_distribute.emp_move.fillna(0).sum()):
    #                         emp_distribute.loc[~emp_distribute.emp_move.isna(),'dist_threshold'] += 25
    #                     numa_dist_sub['dist_threshold'] = reindex(emp_distribute.set_index('TAZ')['dist_threshold'], numa_dist_sub.oTAZ)
    #                     if (emp_assign.emp_reqd > 0).any() & (not include_high_ind):
    #                         add_TAZ_pair = numa_dist_sub[(numa_dist_sub.distance < numa_dist_sub.dist_threshold) & (~numa_dist_sub.added) & (numa_dist_sub.dTAZ.isin(emp_assign[emp_assign.emp_reqd > 0].TAZ))][['oTAZ', 'dTAZ', 'distance']]
    #                     else:
    #                         add_TAZ_pair = numa_dist_sub[(numa_dist_sub.distance < numa_dist_sub.dist_threshold) & (~numa_dist_sub.added)][['oTAZ', 'dTAZ', 'distance']]
    #                     est_sub3 = pd.merge(add_TAZ_pair,
    #                                         est_sub[['TAZ', 'emp']].reset_index(),
    #                                         how='inner',
    #                                         left_on='oTAZ',
    #                                         right_on='TAZ').set_index('bus_id')
    #                     numa_dist_sub.loc[(numa_dist_sub.distance < numa_dist_sub.dist_threshold) & (~numa_dist_sub.added), 'added'] = True
    #                     est_sub2 = est_sub2.drop('relocate', axis=1, errors='ignore').append(est_sub3)
    #                     emp_move = est_sub[est_sub.TAZ.isin(est_sub2.oTAZ)].groupby('TAZ')['emp'].agg('sum')
    #                     emp_distribute['emp_move'] = reindex(emp_move, emp_distribute.TAZ)
    #                     emp_distribute['emp_move'] = emp_distribute.emp_move - emp_distribute.emp_SE
    #                     # check_size = (emp_assign.emp_avail.isna()).any() | ((emp_assign.emp_SE * emp_assign.SE_adjust_factor)>= emp_assign.emp_avail).any()
    #                     # check_size = (emp_distribute.emp_move.isna()).any() | ((emp_distribute.emp_CBP - emp_distribute.emp_move) >= emp_distribute.emp_SE).any()
    #                     check_size = (emp_distribute.emp_move.isna()).any() | (emp_assign[emp_assign.emp_reqd > 0].emp_reqd.sum() < emp_distribute.emp_move.fillna(0).sum()).any()
    #                 print 'Distance threshold used: %.2f' % emp_distribute.dist_threshold.mean()
    #                 model = cp_model.CpModel()
    #                 relocate_const = est_sub2['dTAZ'].reset_index().apply(lambda df: model.NewBoolVar('x[%i,%i]' % (df.bus_id, df.dTAZ)), axis=1)
    #                 relocate_const.index = est_sub2.index
    #                 est_sub2['relocate'] = relocate_const
    #                 se_emp_otaz_const = est_sub2.groupby('oTAZ')[['relocate','emp']].apply(lambda df: sum(list(df.relocate * df.emp))).to_frame('se_emp_otaz_const')
    #                 # se_emp_otaz_const = est_sub2.groupby('dTAZ')[['relocate','emp']].apply(lambda df: sum(list(df.relocate))).to_frame('se_emp_dtaz_const')
    #                 # se_emp_otaz_const['emp_constraint'] = [model.Add(se_emp_dtaz_const.loc[TAZ,'se_emp_dtaz_const'] >= emp_distribute.loc[emp_assign.TAZ==TAZ,'to_move'].values[0].astype('int')) for TAZ in se_emp_dtaz_const.index]
    #                 se_emp_otaz_const['emp_constraint'] = [model.Add(se_emp_otaz_const.loc[TAZ,'se_emp_otaz_const'] <= emp_distribute.loc[emp_distribute.TAZ==TAZ,'emp_SE'].values[0].astype('int')) for TAZ in se_emp_dtaz_const.index]
    #                 se_emp_dtaz_const = est_sub2.groupby('dTAZ')[['relocate','emp']].apply(lambda df: sum(list(df.relocate * df.emp))).to_frame('se_emp_dtaz_const')
    #                 se_emp_dtaz_const['emp_constraint'] = [model.Add(se_emp_dtaz_const.loc[TAZ,'se_emp_dtaz_const'] <= emp_assign.loc[emp_assign.TAZ==TAZ,'emp_reqd'].values[0].astype('int')) for TAZ in se_emp_dtaz_const.index]
    #                 bus_const = est_sub2.reset_index().groupby('bus_id')[['relocate']].apply(lambda df: model.Add(sum(list(df.relocate.values)) <= 1)).to_frame('variables')
    #                 obj = sum([x*(-c) for c, x in zip((est_sub2.emp/est_sub2.distance).astype('int').values,est_sub2.relocate.values)])
    #                 model.Maximize(obj)
    #                 solver = cp_model.CpSolver()
    #                 solver.parameters.log_search_progress = True
    #                 solver.parameters.num_search_workers = 24
    #                 solution_printer = cp_model.ObjectiveSolutionPrinter()
    #                 status = solver.SolveWithSolutionCallback(model, solution_printer)                    
    #                 emp_distribute['dist_threshold'] +=25 
    #                 numa_dist_sub['dist_threshold'] +=25
    #                 current_iter += 1
    #             est_sub2['relocated'] = est_sub2.apply(lambda df: solver.Value(df.relocate), axis=1)
    #             est_moved = est_sub2[est_sub2.relocated==1]
    #             est.loc[est_moved.index, 'TAZ'] = est_moved['dTAZ'].astype(int).values
    #     employment = est.groupby(['TAZ', 'model_emp_cat'])['emp'].sum()
    #     employment = employment.to_frame(name='emp_CBP').reset_index()
    #     employment_SE = socio_economics_taz.reset_index(). \
    #         melt(id_vars='TAZ', var_name='model_emp_cat', value_name='emp_SE')
    #     # inner join
    #     employment = employment.merge(right=employment_SE, how='outer', on=['TAZ', 'model_emp_cat'])

    #     # FIXME should we leave this nan to distinguish zero counts from missing data?
    #     employment.emp_SE.fillna(0, inplace=True)
    #     employment.emp_CBP.fillna(0, inplace=True)

    #     # Re-allocation of TAZ to establishments for better scaling
    #     employment = pd.merge(employment,
    #                           est.groupby(['TAZ', 'model_emp_cat']).size().
    #                           to_frame('n_est').reset_index(),
    #                           on=['TAZ', 'model_emp_cat'],
    #                           how='outer').fillna(0)

    #     # Identify TAZ and model_emp_cat that needs to be reallocated somewhere else
    #     employment['to_distribute'] = employment['n_est'] > employment[emp_to_check]
    #     employment['to_assign'] = ~employment['to_distribute']
    #     employment['avg_emp'] = reindex(est.groupby('model_emp_cat')['emp'].median(),
    #                                     employment.model_emp_cat)
    #     employment['to_move'] = employment['n_est'] - employment[emp_to_check]
    #     employment.loc[employment.to_move < 0.0, 'to_move'] = 0.0

    #     # drop rows where both employment sources say there should be no employment in empcat
    #     employment = employment[~((employment.emp_SE == 0) & (employment.emp_CBP == 0))]
    #     rem_distribute = employment.to_distribute.sum()
    #     print 'Establishments remaining to be moved :%d\n' % rem_distribute

    # logger.info("%d ests relocated." % (est[est.orig_TAZ != est.TAZ].shape[0]))
    # t0 = print_elapsed_time("est_sim_scale_employees relocating establishments for better scaling",
    #                         t0,
    #                         debug=True)

    # ests.to_csv('ests_moved2.csv')
    # Three cases to deal with:
    # 1. Employment change where there is some CBP employment and some SE employment
    # 2. Employment in SE data but none from CBP
    # 3. Employment from CBP but none in SE data
    #
    # Note: if difference is 0, no action required --> Adjustment factor will be 1:

    # - Case 1 and 3: scale the number of employees without creating any ests

    est = scale_cbp_to_se(employment, est)

    # - drop ests with no employees
    # FIXME all international (TAZ=0) ests have 0 employees?
    if (est.emp == 0).any():
        n0 = est.shape[0]
        est = est[est.emp > 0]
        n1 = est.shape[0]
        logger.info('Dropped %s out of %s no-emp ests' % (n0 - n1, n0))

    # - Case 2: create new n new est where n is employees_needed / avg_number_employees_per_firm

    # - Add est to empty TAZ-Employment category combinations to deal with Case 2
    # For each combination:
    # 1. Select a number of est max of EMPDIFF/average emp from all est in that Model_EmpCat
    # 2. Add them to the est table
    # 3. recalc the EMPADJ to refine the employment to match exactly the SE data employment

    est_needed = employment[employment.emp_CBP < 1].copy()

    # - Calculate average employment by Model_EmpCat

    empcat_avg_emp = est.groupby('model_emp_cat')['emp'].mean()
    est_needed['avg_emp'] = reindex(empcat_avg_emp, est_needed.model_emp_cat)

    # Employment Category Absent from CBP
    emp_cats_absent_from_cbp = est_needed.model_emp_cat[est_needed.avg_emp.isnull()].unique()

    DEFAULT_AVG_EMP = 10  # FIXME magic constant
    est_needed.avg_emp.fillna(DEFAULT_AVG_EMP, inplace=True)

    # - number of est to be sampled is employees_needed / avg_number_employees_per_firm
    assert (est_needed.emp_CBP == 0).all()
    est_needed['n'] = (est_needed.emp_SE / est_needed.avg_emp).clip(lower=1).round().astype(
        int)

    prng = pipeline.get_rn_generator().get_global_rng()
    new_est = []
    for emp_cat, emp_cat_est_needed in est_needed.groupby('model_emp_cat'):

        # if emp_cat not in est.model_emp_cat.values:
        if emp_cat in emp_cats_absent_from_cbp:
            logger.warn('Skipping model_emp_cat %s not found in cbp' % emp_cat)
            continue

        new_est_ids = prng.choice(
            a=est[est.model_emp_cat == emp_cat].index.values,
            size=emp_cat_est_needed.n.sum(),
            replace=True)

        df = pd.DataFrame({
            'old_bus_id': new_est_ids,
            'new_TAZ': np.repeat(emp_cat_est_needed.TAZ, emp_cat_est_needed.n)
        })

        new_est.append(df)

    new_est = pd.concat(new_est)

    # Look up the firm attributes for these new est (from the ones they were created from)
    new_est = new_est.merge(right=est, left_on='old_bus_id', right_index=True)

    del new_est['old_bus_id']

    del new_est['TAZ']
    new_est.rename(columns={'new_TAZ': 'TAZ'}, inplace=True)

    print new_est.columns.values

    # - Update the County and State FIPS and FAF zones
    new_est.state_FIPS = reindex(taz_fips.state_FIPS, new_est.TAZ)
    new_est.county_FIPS = reindex(taz_fips.county_FIPS, new_est.TAZ)
    new_est.FAF4 = reindex(taz_faf4.FAF4, new_est.TAZ)

    # - Give the new est new, unique business IDs
    new_est.reset_index(drop=True, inplace=True)
    new_est.index = new_est.index + est.index.max() + 1
    new_est.index.name = est.index.name

    # - scale/bucket round to ensure that employee counts of the new est matche the SE data
    # Summarize employment of new est by TAZ and Model_EmpCat
    employment_new = new_est.groupby(['TAZ', 'model_emp_cat'])['emp'].sum()
    employment_new = employment_new.to_frame(name='emp_CBP').reset_index()
    # Melt socio_economics_taz employment data by TAZ and employment category
    employment_SE = socio_economics_taz.reset_index().\
        melt(id_vars='TAZ', var_name='model_emp_cat', value_name='emp_SE')
    # left join since we only care about new est
    employment_new = \
        employment_new.merge(right=employment_SE, how='left', on=['TAZ', 'model_emp_cat'])

    assert not employment_new.emp_SE.isnull().any()
    assert not employment_new.emp_CBP.isnull().any()
    assert not (employment_new.emp_CBP == 0).any()

    new_est = scale_cbp_to_se(employment_new, new_est)

    for emp_cat in est_needed.model_emp_cat.unique():
        n_needed = est_needed[est_needed.model_emp_cat == emp_cat].n.sum()
        n_created = (new_est.model_emp_cat == emp_cat).sum()

        if n_needed != n_created:
            logger.warn("model_emp_cat %s needed %s, created %s" % (emp_cat, n_needed, n_created))

    # Combine the original est and the new est
    est = pd.concat([est, new_est])

    assert not (est.emp < 1).any()

    # Recode employee counts into categories
    est['esizecat'] = \
        pd.cut(x=est.emp,
               bins=np.append(employment_categories.low_threshold.values, np.inf),
               labels=employment_categories.id,
               include_lowest=True).astype(int)
    assert not est.esizecat.isnull().any()

    assert est.index.is_unique  # index (bus_id) should be unique

    assert not (est.emp < 1).any()

    # - error check: ests employment should match emp_SE for all taz and emp_cats
    t0 = print_elapsed_time()
    summary = pd.merge(
        left=est.groupby(['TAZ', 'model_emp_cat'])['emp'].sum().to_frame('emp_ests'),
        right=employment.set_index(['TAZ', 'model_emp_cat']),
        left_index=True, right_index=True, how='outer'
    ).reset_index()
    # ignore emp_cats not present in ests
    summary = summary[~summary.model_emp_cat.isin(emp_cats_not_in_ests)]
    summary.emp_ests.fillna(0, inplace=True)
    assert np.abs(summary.emp_ests.sum() - summary.emp_SE.sum()) <= 5
    t0 = print_elapsed_time("error check est_sim_scale_employees results", t0, debug=True)

    # Reset the indices (bus_id) of foreign ests
    MAX_BUS_ID = est.index.max()
    logger.info("assigning foreign est indexes starting above MAX_BUS_ID %s" % (MAX_BUS_ID,))
    est_foreign.reset_index(drop=True, inplace=True)
    est_foreign.index = est_foreign.index + MAX_BUS_ID + 1
    est_foreign.index.name = est.index.name

    # Combine the new ests with foreign ests
    est = pd.concat([est, est_foreign]).sort_index()
    assert est.index.is_unique  # index (bus_id) should be unique

    # Recode employee counts into categories
    est.loc[est.FAF4 > 800, 'emp'] = \
        est_emp_generator(employment_categories.loc[employment_categories.index.max(),
                                                    'low_threshold'],
                          employment_categories.loc[employment_categories.index.max(),
                                                    'emp_range'],
                          hyman_interpol,
                          (est.FAF4 > 800).sum()).astype(int)
    est['esizecat'] = \
        pd.cut(x=est.emp,
               bins=np.append(employment_categories.low_threshold.values, np.inf),
               labels=employment_categories.id,
               include_lowest=True).astype(int)
    assert not est.esizecat.isnull().any()

    # firms['emp'] = est['emp']
    # firms.emp.fillna(0, inplace=True)
    # firms = firms.assign(emp=lambda df: df.groupby('firm_id')['emp'].transform('sum'))
    # firms['fsizecat'] = pd.cut(x=firms.emp,
    #                            bins=np.append(firm_employment_categories.low_threshold.values,
    #                                           np.inf),
    #                            labels=firm_employment_categories.id,
    #                            include_lowest=True).astype(int)
    return est #, firms


def est_sim_assign_SCTG(
        est,
        NAICS2007io_to_SCTG):
    """
    Simulate Production Commodities, choose a single commodity for ests making more than one
    """

    # Look up all the SCTGs (Standard Classification of Transported Goods) each est can produce
    # Note: not every est produces a transportable SCTG commodity
    # Some ests can potentially make more than one SCTG, simulate exactly one

    t0 = print_elapsed_time()

    # Separate out the foreign ests
    est_foreign = est[est.FAF4 > 800]
    est = est[est.FAF4 < 800].copy()

    # Merge in the single-SCTG naics
    est['SCTG'] = reindex(
        NAICS2007io_to_SCTG[NAICS2007io_to_SCTG.proportion == 1].set_index('NAICSio').SCTG,
        est.NAICS6_make
    )

    t0 = print_elapsed_time("Merge in the single-SCTG naics", t0, debug=True)

    # random select SCTG for multi-SCTG naics based on proportions (probabilities)
    multi_NAICS2007io_to_SCTG = NAICS2007io_to_SCTG[NAICS2007io_to_SCTG.proportion < 1]
    multi_sctg_naics_codes = multi_NAICS2007io_to_SCTG.NAICSio.unique()
    multi_sctg_ests = est[est.NAICS6_make.isin(multi_sctg_naics_codes)]

    # for each distinct multi-SCTG naics
    prng = pipeline.get_rn_generator().get_global_rng()
    for naics, naics_est in multi_sctg_ests.groupby('NAICS6_make'):
        # slice the NAICS2007io_to_SCTG rows for this naics
        naics_sctgs = multi_NAICS2007io_to_SCTG[multi_NAICS2007io_to_SCTG.NAICSio == naics]

        # choose a random SCTG code for each business with this naics code
        sctgs = prng.choice(naics_sctgs.SCTG.values,
                            size=len(naics_est),
                            p=naics_sctgs.proportion.values,
                            replace=True)

        est.loc[naics_est.index, 'SCTG'] = sctgs

    t0 = print_elapsed_time("choose SCTG for multi-SCTG naics", t0, debug=True)

    KeyMap = collections.namedtuple('KeyMap',
                                    ['key_col', 'key_value', 'target', 'target_values', 'probs'])

    def assign_keymap(keymap, df):
        flag_rows = (df[keymap.key_col] == keymap.key_value)
        if np.isscalar(keymap.target_values):
            df.loc[flag_rows, keymap.target] = keymap.target_values
        else:
            assert len(keymap.target_values) == len(keymap.probs)
            df.loc[flag_rows, keymap.target] = \
                prng.choice(keymap.target_values, size=flag_rows.sum(), p=keymap.probs)

    # Identify ests who make 2+ commodities (especially wholesalers) and simulate a specific
    # commodity for them based on probability thresholds for multiple commodities
    keymaps = [
        # TODO: are these the right correspodences given the I/O data for the current project?
        KeyMap('NAICS2012', 211111, 'SCTG', (16L, 19L), (.45, .55)),
        # Crude Petroleum and Natural Gas Extraction: Crude petroleum; Coal and petroleum products, n.e.c.            # nopep8
        KeyMap('NAICS2012', 324110, 'SCTG', (17L, 18L, 19L), (.25, .25, .50)),
        # Petroleum Refineries: Gasoline and aviation turbine fuel; Fuel oils; Coal and petroleum products, n.e.c.    # nopep8
        # nopep8
        KeyMap('n4', 4233, 'SCTG', (10L, 11L, 12L, 25L, 26L), (0.10, 0.10, 0.60, 0.10, 0.10)),
        # Lumber and Other Construction Materials Merchant Wholesalers                            # nopep8
        KeyMap('n4', 4235, 'SCTG', (13L, 14L, 31L, 32L), (0.25, 0.25, 0.25, 0.25)),
        # Metal and Mineral (except Petroleum) Merchant Wholesalers                               # nopep8
        KeyMap('n4', 4247, 'SCTG', (16L, 17L, 18L, 19L), (0.25, 0.25, 0.25, 0.25)),
        # Petroleum and Petroleum Products Merchant Wholesalers                                   # nopep8
        KeyMap('n4', 4246, 'SCTG', (20L, 21L, 22L, 23L), (0.25, 0.25, 0.25, 0.25)),
        # Chemical and Allied Products Merchant Wholesalers                                       # nopep8
        KeyMap('n4', 4245, 'SCTG', (1L, 2L, 3L, 4L), (0.25, 0.25, 0.25, 0.25)),
        # Farm Product Raw Material Merchant Wholesalers                                          # nopep8
        KeyMap('n4', 4244, 'SCTG', (5L, 6L, 7L, 9L), (0.25, 0.25, 0.25, 0.25)),
        # Grocery and Related Product Wholesalers                                                 # nopep8
        KeyMap('n4', 4241, 'SCTG', (27L, 28L, 29L), (0.33, 0.33, 0.34)),
        # Paper and Paper Product Merchant Wholesalers                                            # nopep8
        KeyMap('n4', 4237, 'SCTG', (15L, 33L), (0.50, 0.50)),
        # Hardware, and Plumbing and Heating Equipment and Supplies Merchant Wholesalers          # nopep8
        KeyMap('n4', 4251, 'SCTG', (35L, 38L), (0.50, 0.50)),
        # Wholesale Electronic Markets and Agents and Brokers                                     # nopep8
        KeyMap('n4', 4236, 'SCTG', (35L, 38L), (0.50, 0.50)),
        # Electrical and Electronic Goods Merchant Wholesalers                                    # nopep8
        KeyMap('n4', 4231, 'SCTG', (36L, 37L), (0.50, 0.50)),
        # Motor Vehicle and Motor Vehicle Parts and Supplies Merchant Wholesalers                 # nopep8

        KeyMap('n4', 4248, 'SCTG', 8L, 1),
        # Beer, Wine, and Distilled Alcoholic Beverage Merchant Wholesalers         # nopep8
        KeyMap('n4', 4242, 'SCTG', 21L, 1),
        # Drugs and Druggists Sundries Merchant Wholesalers                         # nopep8
        KeyMap('n4', 4234, 'SCTG', 24L, 1),
        # Professional and Commercial Equipment and Supplies Merchant Wholesalers   # nopep8
        KeyMap('n4', 4243, 'SCTG', 30L, 1),
        # Apparel, Piece Goods, and Notions Merchant Wholesalers                    # nopep8
        KeyMap('n4', 4238, 'SCTG', 34L, 1),
        # Machinery, Equipment, and Supplies Merchant Wholesalers                   # nopep8
        KeyMap('n4', 4232, 'SCTG', 39L, 1),
        # Furniture and Home Furnishing Merchant Wholesalers                        # nopep8
        KeyMap('n4', 4239, 'SCTG', 40L, 1),
        # Miscellaneous Durable Goods Merchant Wholesalers                          # nopep8
        KeyMap('n4', 4249, 'SCTG', 40L, 1),
        # Miscellaneous Nondurable Goods Merchant Wholesalers                       # nopep8
    ]

    for keymap in keymaps:
        assign_keymap(keymap, df=est)

    # all n2=43 ests get NAICS6_Make padded n4 (e.g. 4248 gets NAICS6_Make '424800')
    est.NAICS6_make = est.NAICS6_make.mask(est.n2 == 42, (est.n4 * 100).astype(str))

    # Combine the new ests with foreign ests
    est = pd.concat([est, est_foreign]).sort_index()

    assert est.index.is_unique
    return est


def est_sim_types(est):
    """
    Identify special ests: warehouses, producers, and makers subsample
    """

    assert est.index.is_unique

    # Separate out the foreign ests
    est_foreign = est[est.FAF4 > 800]
    est = est[est.FAF4 < 800].copy()

    # Warehouses
    # NAICS 481 air, 482 rail, 483 water, 493 warehouse and storage
    est['warehouse'] = \
        est.TAZ.isin(base_variables.BASE_TAZ1_HALO_STATES) & \
        (est.n3.isin(base_variables.NAICS3_WAREHOUSE))

    # Producers and Makers
    # Create a flag to make sure at least one est is used
    # as a maker for each zone and commodity combination
    producers = est[~est.SCTG.isnull()].copy()

    prng = pipeline.get_rn_generator().get_global_rng()
    g = ['TAZ', 'FAF4', 'NAICS6_make', 'SCTG']
    keeper_ids = \
        producers[g].groupby(g).agg(lambda x: prng.choice(x.index, size=1)[0]).values
    producers['must_keep'] = False
    producers.loc[keeper_ids, 'must_keep'] = True

    # Create a sample of "makers" from the full producers table based on the following rules:
    # 1. Keep all individual businesses inside the region plus halo states
    # 2. Keep all large businesses throughout the US
    # 3. Keep those identified as "MustKeep" above

    makers = producers[
        producers.TAZ.isin(base_variables.BASE_TAZ1_HALO_STATES) |
        (producers.esizecat >= 6) |
        producers.must_keep]

    # producers['identity'] = True
    # ests['producer'] = producers.identity

    # Add maker and producer flags back to list of ests
    est['producer'] = False
    est.loc[producers.index, 'producer'] = True

    est['maker'] = False
    est.loc[makers.index, 'maker'] = True

    # Combine the new ests with foreign ests
    est = pd.concat([est, est_foreign]).sort_index()

    return est[['state_FIPS', 'county_FIPS', 'FAF4', 'TAZ', 'SCTG', 'NAICS2012',
                'NAICS6_make', 'industry10', 'industry5', 'model_emp_cat', 'esizecat', 'emp',
                'warehouse', 'producer', 'maker', 'prod_val']].copy()


def est_sim_producers(est, io_values, unitcost):
    """
    Create Producers

    Creates a dataframe of producer ests in which each producer ests appears once
    for every commodity that uses produced commodity as an input

    producers
    seller_id                    int64
    TAZ                          int64
    county_FIPS                  int64
    state_FIPS                   int64
    FAF4                       float64
    n4                           int64
    n3                           int64
    n2                           int64
    NAICS                          str
    size                         int64
    SCTG                       float64
    NAICS6_use                  object
    non_transport_unit_cost    float64
    output_capacity_tons       float64
    output_commodity               str
    """

    t0 = print_elapsed_time()

    # producers.NAICS6_make - the commodity the producer produces
    # io_values.NAICS6_make = input commodity required to make NAICS6_use
    # io_values.NAICS6_use = output commodity uses io_values.NAICS6_make as an input
    producers = est[est.producer]
    domestic_producer_idx = producers.FAF4 < 800

    # - get the max_bus_id
    MAX_BUS_ID = est[est.FAF4 < 800].index.max()

    # we only want i/o pairs that use domestic producers commodity as an input
    # FIXME how should we handle foreign production
    # FIXME there are i/o codes in foreign producers that do not exist in
    #  domestic producers
    io_values = io_values[(io_values.NAICS6_make.isin(producers[domestic_producer_idx].NAICS6_make.
                                                      unique()))]

    # - For each output, select only the most important input commodities

    # FIXME sort and goupby need to be in synch - but which is it????
    # - setkey(InputOutputValues, NAICS6_Make, ProVal)
    io_values = io_values.sort_values(by=['NAICS6_use', 'pro_val'])

    # - total number of domestic employees manufacturing NAICS6_make commodity
    producer_emp_counts_by_naics = \
        producers[(~producers.state_FIPS.isnull()) & domestic_producer_idx][['NAICS6_make',
                                                                             'emp']]. \
        groupby('NAICS6_make')['emp'].sum()

    # FIXME why are we summing emp again?
    # InputOutputValues <- InputOutputValues[,.(ProVal=sum(ProVal), Emp = sum(Emp)), .(NAICS6_Make)]
    io_values = io_values[['NAICS6_make', 'pro_val']].groupby('NAICS6_make',
                                                              group_keys=False,
                                                              as_index=False).sum()

    # - annotate input_output_values with total emp count for producers of NAICS6_input
    io_values['emp'] = \
        reindex(producer_emp_counts_by_naics, io_values.NAICS6_make)

    io_values.set_index('NAICS6_make', inplace=True)

    # InputOutputValues[, ValEmp := ProVal/Emp]
    io_values['val_emp'] = io_values.pro_val / io_values.emp

    t0 = print_elapsed_time("io_values", t0, debug=True)

    producers = pd.merge(left=producers.reset_index(), right=io_values[['val_emp']],
                         left_on='NAICS6_make', right_index=True, how='inner')

    # compute domestic producer commodity value
    producers['prod_val'] = np.where(producers.prod_val.isnull(), producers.val_emp * producers.emp,
                                     producers.prod_val)

    # check there are no null prod_val
    assert ~producers.prod_val.isnull().any()

    # compute producer capacity
    producers['unit_cost'] = reindex(unitcost.unit_cost, producers.SCTG)
    producers['prod_cap'] = producers.prod_val * 1E6 / producers.unit_cost
    producers.loc[producers.FAF4 > 800, 'bus_id'] = np.arange(0, (producers.FAF4 > 800).sum()) + \
                                                    MAX_BUS_ID + 1

    col_map = {'bus_id': 'seller_id',
               'NAICS6_make': 'NAICS',
               'unit_cost': 'non_transport_unit_cost',
               'prod_cap': 'output_capacity_tons',
               'emp': 'size'}
    producers.rename(columns=col_map, inplace=True)

    producer_cols = \
        ['seller_id', 'TAZ', 'county_FIPS', 'state_FIPS', 'FAF4',
         'NAICS', 'size', 'SCTG', 'non_transport_unit_cost', 'output_capacity_tons']
    producers = producers[producer_cols].copy()

    # FIXME is this supposed to duplicate or rename?
    producers['output_commodity'] = producers['NAICS']
    # Return the maximum seller_id
    MAX_BUS_ID = producers.seller_id.max()

    # FIXME convert producers to dictionary of data-frame for ease of storage
    producers = dict(tuple(producers.groupby(['NAICS', 'SCTG'])))

    return producers, MAX_BUS_ID


def est_sim_consumers(est, io_values, NAICS2007io_to_SCTG, unitcost, est_pref_weights, MAX_BUS_ID):
    """
    Create Consumers

    produces a dataframe of consumer ests in which each consumer est

    buyer_id                        int64
    input_commodity                   str
    SCTG                            int64
    NAICS                          object
    TAZ                             int64
    FAF4                          float64
    esizecat                          str
    size                            int64
    con_val                       float64
    non_transport_unit_cost       float64
    purchase_amount_tons          float64
    pref_weight_1_unit_cost       float64
    pref_weight_2_ship_time       float64
    single_source_max_fraction    float64
    """

    t0 = print_elapsed_time()

    # io_values.NAICS6_make = input commodity required to make NAICS6_use
    # io_values.NAICS6_use = output commodity uses io_values.NAICS6_make as an input
    # producers.NAICS6_make - the commodity the producer produces

    # Separate foreign ests
    est_foreign = est[est.FAF4 > 800]
    est = est[est.FAF4 < 800]

    # Identify consumer ests
    cols = ['TAZ', 'county_FIPS', 'state_FIPS', 'FAF4', 'NAICS6_make', 'esizecat', 'emp']
    consumers = est[cols].copy()
    consumers.reset_index(inplace=True)  # bus_id index as explicit column

    t0 = print_elapsed_time("est_sim_consumers cols", t0, debug=True)

    # Create a flag to make sure at least one est is sampled
    # for each zone and PRODUCED commodity combination
    prng = pipeline.get_rn_generator().get_global_rng()
    g = ['TAZ', 'FAF4', 'NAICS6_make', 'esizecat']
    keeper_ids = \
        consumers[g].groupby(g).agg(lambda x: prng.choice(x.index, size=1)[0]).values
    t0 = print_elapsed_time("keeper_ids", t0, debug=True)

    consumers['must_keep'] = False
    consumers.loc[keeper_ids, 'must_keep'] = True

    t0 = print_elapsed_time("est_sim_consumers must_keep", t0, debug=True)

    # Create a sample of consumer ests based on the following rules to form
    # the basis of the producer-consumer pairs to be simulated:
    # 1. Keep all individual businesses in the region and halo states.
    # 2. Also keep all large businesses throughout the U.S
    # 3. Keep those identified as "MustKeep" above
    # 4. Randomly sample an additional small percentage

    temp_rand = prng.rand(consumers.shape[0])

    consumers = consumers[
        consumers.TAZ.isin(base_variables.BASE_TAZ1_HALO_STATES) |
        (consumers.esizecat >= 5) |
        consumers.must_keep |
        (temp_rand > 0.95)]

    # Remove extra fields
    del consumers['must_keep']

    t0 = print_elapsed_time("est_sim_consumers slice", t0, debug=True)

    # For each consuming est generate a list of input commodities that need to be purchased
    # Filter to just transported commodities
    # FIXME filters so io_values only contains producer NAICS6_make
    # FIXME but nothing guarantees that the io_values NAICS6_use have any domestic producers
    producer_naics = est.NAICS6_make[est.producer].unique()
    producer_naics = np.unique(
        np.append(producer_naics, est_foreign.NAICS6_make[est_foreign.producer].unique()))
    io_values = io_values[io_values.NAICS6_make.isin(producer_naics)]

    # - Calculate cumulative pct value of the consumption inputs
    io_values = io_values.sort_values(by=['NAICS6_use', 'pro_val'])
    io_values['cum_pro_val'] = \
        io_values.groupby('NAICS6_use')['pro_val'].transform('cumsum')
    io_values['cum_pct_pro_val'] = \
        io_values['cum_pro_val'] / io_values.groupby('NAICS6_use')['cum_pro_val'].transform('last')

    # Select suppliers including the first above the threshold value
    # FIXME filter tp select io_values with highest pro_val for NAICS6_use
    io_values = io_values[io_values.cum_pct_pro_val > (1 - base_variables.BASE_PROVALTHRESHOLD)]

    # - Calcuate value per employee required (US domestic employment) for each NAICS6_make code
    # FIXME - use FAF4 filters to filter domestic ests
    domestic_emp_counts_by_naics = \
        est[(~est.state_FIPS.isnull())][['NAICS6_make', 'emp']]. \
        groupby('NAICS6_make')['emp'].sum()

    # - drop io_values rows where there are no domestic producers of the output commodity
    io_values = io_values[io_values.NAICS6_use.isin(domestic_emp_counts_by_naics.index)]

    # - annotate input_output_values with total emp count for producers of NAICS_output
    io_values['emp'] = reindex(domestic_emp_counts_by_naics, io_values.NAICS6_use)
    io_values['val_emp'] = io_values.pro_val / io_values.emp

    t0 = print_elapsed_time("est_sim_consumers io_values", t0, debug=True)

    # there are ests with no inputs (e.g. wholesalers)
    # but we might want to keep an eye on them, as they will get dropped in merge
    # output_NAICS = io_values.NAICS6_use.unique()
    # ests_NAICS6_make = ests.NAICS6_make.unique()
    # naics_without_inputs = list(set(ests_NAICS6_make) - set(output_NAICS))
    # logger.info("%s NAICS_make without input commodities %s" %
    #             (len(naics_without_inputs), naics_without_inputs))
    # t0 = print_elapsed_time("naics_without_inputs", t0, debug=True)

    # - inner join sample_ests with the top inputs required to produce est's product
    # pairs is a list of ests with one row for every major commodity the est consumes
    # pairs.NAICS6_use is the commodity the est PRODUCES
    # pairs.NAICS6_make is a commodity that the est CONSUMES
    consumers = pd.merge(left=consumers.rename(columns={'NAICS6_make': 'NAICS6_use'}),
                         right=io_values[['NAICS6_use', 'NAICS6_make', 'val_emp']],
                         left_on='NAICS6_use', right_on='NAICS6_use', how='inner')

    assert not consumers.val_emp.isnull().any()

    t0 = print_elapsed_time("est_sim_consumers pairs", t0, debug=True)

    # Look up all the SCTGs (Standard Classification of Transported Goods) each est can produce
    # Note: not every est produces a transportable SCTG commodity
    # Some ests can potentially make more than one SCTG, simulate exactly one
    #

    is_multi = (NAICS2007io_to_SCTG.proportion < 1)
    multi_NAICS2007io_to_SCTG = NAICS2007io_to_SCTG[is_multi]
    consumers_NAICS6_make = consumers.NAICS6_make.to_frame(name='NAICS6_make')
    consumer_is_multi = consumers.NAICS6_make.isin(multi_NAICS2007io_to_SCTG.NAICSio.unique())
    t0 = print_elapsed_time("est_sim_consumers pair_is_multi flag", t0, debug=True)

    # - single-SCTG naics
    singletons = pd.merge(
        consumers_NAICS6_make[~consumer_is_multi],
        NAICS2007io_to_SCTG[~is_multi][['NAICSio', 'SCTG']].set_index('NAICSio'),
        left_on="NAICS6_make",
        right_index=True,
        how="left").SCTG
    t0 = print_elapsed_time("est_sim_consumers single-SCTG naics", t0, debug=True)

    # - random select SCTG for multi-SCTG naics based on proportions (probabilities)

    # for each distinct multi-SCTG naics
    multi_sctg_pairs = consumers_NAICS6_make[consumer_is_multi]
    sctgs = []
    bus_ids = []
    for naics, naics_ests in multi_sctg_pairs.groupby('NAICS6_make'):
        # slice the NAICS2007io_to_SCTG rows for this naics
        naics_sctgs = multi_NAICS2007io_to_SCTG[multi_NAICS2007io_to_SCTG.NAICSio == naics]

        # choose a random SCTG code for each business with this naics code
        sctgs.append(prng.choice(naics_sctgs.SCTG.values,
                                 size=len(naics_ests),
                                 p=naics_sctgs.proportion.values,
                                 replace=True))

        bus_ids.append(naics_ests.index.values)

    t0 = print_elapsed_time("est_sim_consumers choose SCTG for multi-SCTG naics", t0, debug=True)

    # singletons
    sctgs.append(singletons.values)
    bus_ids.append(singletons.index.values)
    sctgs = list(itertools.chain.from_iterable(sctgs))
    bus_ids = list(itertools.chain.from_iterable(bus_ids))
    t0 = print_elapsed_time("est_sim_consumers itertools.chain", t0, debug=True)

    # sctgs = pd.Series(sctgs, index=bus_ids)
    # t0 = print_elapsed_time("est_sim_consumers SCTG series", t0, debug=True)

    consumers['SCTG'] = pd.Series(sctgs, index=bus_ids)
    t0 = print_elapsed_time("est_sim_consumers pairs['SCTG']", t0, debug=True)

    assert sum(consumers['SCTG'].isnull()) == 0

    # FIXME - move this tweaks to a data file

    # Account for multiple SCTGs produced by certain naics codes (i.e. 211111 and 324110)
    i = (consumers.NAICS6_make == "211000") & (consumers.SCTG == 16)
    consumers.loc[i[i].index, 'SCTG'] = \
        prng.choice([16, 19], p=[0.45, 0.55], size=i.sum(), replace=True)

    i = (consumers.NAICS6_make == "324110")
    consumers.loc[i[i].index, 'SCTG'] = \
        prng.choice([17, 18, 19], p=[0.25, 0.5, 0.25], size=i.sum(), replace=True)

    # Assume a small chance that a consumer works with a wholesaler instead of a direct shipper for
    # a given shipment/commodity. Mutate some suppliers to wholesaler NAICS.

    pairs_NAICS6_use2 = consumers['NAICS6_use'].transform(lambda naics6: naics6.str[:2])
    temp_rand = prng.rand(consumers.shape[0])

    # Pairs[NAICS6_Use2 != "42" & temprand < 0.30 & SCTG < 41,     NAICS6_Make := sctg_whl[SCTG]]
    sctg_whl = np.array([
        0,  # padding
        424500, 424500, 424500, 424500, 424400, 424400, 424400, 424800, 424400, 423300,
        423300, 423300, 423500, 423500, 423700, 424700, 424700, 424700, 424700, 424600,
        424200, 424600, 424600, 423400, 423300, 423300, 424100, 424100, 424100, 424300,
        423500, 423500, 423700, 423800, 425100, 423100, 423100, 425100, 423200, 423900])
    i = (pairs_NAICS6_use2 != '42') & (consumers.SCTG < 41) & (temp_rand < 0.3)
    consumers.loc[i[i].index, 'NAICS6_make'] = sctg_whl[consumers.loc[i[i].index, 'SCTG']]. \
        astype(str)

    # Pairs[NAICS6_Use2 != "42" & temprand < 0.15 & SCTG %in% c(35, 38), NAICS6_Make := "423600"]
    i = (pairs_NAICS6_use2 != '42') & (consumers.SCTG < 41) & (temp_rand < 0.15)
    consumers.loc[i[i].index, 'NAICS6_make'] = '423600'

    # Pairs[NAICS6_Use2 != "42" & temprand < 0.15 & SCTG == 40,          NAICS6_Make := "424900"]
    i = (pairs_NAICS6_use2 != '42') & (consumers.SCTG == 40) & (temp_rand < 0.15)
    consumers.loc[i[i].index, 'NAICS6_make'] = '424900'

    t0 = print_elapsed_time("est_sim_consumers tweaks", t0, debug=True)

    # - conflate duplicates
    # Accounting for multiple SCTGs from one NAICS and for wholesalers means that certain
    # users now have multiple identical inputs on a NAICS6_Make - SCTG basis
    # aggregate (summing over ValEmp) so that NAICS6_MAke - SCTG is unique for each user

    by_cols = ['bus_id', 'NAICS6_make', 'SCTG', 'NAICS6_use', 'TAZ', 'FAF4', 'esizecat']
    consumers = consumers.groupby(by_cols)['val_emp'].sum().to_frame(name='val_emp').reset_index()

    t0 = print_elapsed_time("est_sim_consumers group and sum", t0, debug=True)

    consumers = consumers.copy()

    consumers['emp'] = reindex(est.emp, consumers.bus_id)
    consumers['con_val'] = consumers.emp * 1E6 * consumers.val_emp
    consumers['unit_cost'] = reindex(unitcost.unit_cost, consumers.SCTG)
    consumers['purchase_amount_tons'] = consumers.con_val / consumers.unit_cost

    del consumers['val_emp']
    del est

    # - Foreign consumption
    io_values['pct_pro_val'] = io_values.groupby('NAICS6_make')['pro_val']. \
        transform(lambda value: value / value.sum())
    consumers_foreign = pd.merge(left=est_foreign[~est_foreign.producer.astype(bool)].
                                 reset_index(),
                                 right=io_values[['NAICS6_use', 'NAICS6_make', 'pct_pro_val']],
                                 on='NAICS6_make',
                                 how='inner')
    consumers_foreign['bus_id'] = np.arange(0L, consumers_foreign.shape[0]) + MAX_BUS_ID + 1
    consumers_foreign.pct_pro_val.fillna(0, inplace=True)
    consumers_foreign['con_val'] = consumers_foreign.prod_val * 1E6 * consumers_foreign.pct_pro_val
    consumers_foreign['unit_cost'] = reindex(unitcost.unit_cost, consumers_foreign.SCTG)
    consumers_foreign['purchase_amount_tons'] = consumers_foreign.con_val / (consumers_foreign.
                                                                             unit_cost)
    consumers_foreign = consumers_foreign[consumers.columns].copy()
    del est_foreign

    # consumers = pd.concat([consumers, consumers_foreign], ignore_index=True)

    consumers.NAICS6_make = consumers.NAICS6_make.astype('category')
    consumers.NAICS6_use = consumers.NAICS6_use.astype('category')

    consumers['cost_weight'] = reindex(est_pref_weights.cost_weight, consumers.SCTG)
    consumers['time_weight'] = reindex(est_pref_weights.time_weight, consumers.SCTG)
    consumers['single_source_max_fraction'] = reindex(est_pref_weights.single_source_max_fraction,
                                                      consumers.SCTG)

    consumers.NAICS6_make = consumers.NAICS6_make.astype('str')
    consumers.NAICS6_use = consumers.NAICS6_use.astype('str')
    consumers.SCTG = consumers.SCTG.astype(int)
    consumers.FAF4 = consumers.FAF4.astype(int)

    consumers_foreign.NAICS6_make = consumers_foreign.NAICS6_make.astype('category')
    consumers_foreign.NAICS6_use = consumers_foreign.NAICS6_use.astype('category')

    consumers_foreign['cost_weight'] = reindex(est_pref_weights.cost_weight, consumers_foreign.SCTG)
    consumers_foreign['time_weight'] = reindex(est_pref_weights.time_weight, consumers_foreign.SCTG)
    consumers_foreign['single_source_max_fraction'] = \
        reindex(est_pref_weights.single_source_max_fraction,
                consumers_foreign.SCTG)

    consumers_foreign.NAICS6_make = consumers_foreign.NAICS6_make.astype('str')
    consumers_foreign.NAICS6_use = consumers_foreign.NAICS6_use.astype('str')
    consumers_foreign.SCTG = consumers_foreign.SCTG.astype(int)
    consumers_foreign.FAF4 = consumers_foreign.FAF4.astype(int)

    col_map = {'bus_id': 'buyer_id',
               'NAICS6_make': 'input_commodity',
               'NAICS6_use': 'NAICS',
               'emp': 'size',
               'unit_cost': 'non_transport_unit_cost',
               'cost_weight': 'pref_weight_1_unit_cost',
               'time_weight': 'pref_weight_2_ship_time'
               }
    consumers.rename(columns=col_map, inplace=True)
    consumers_foreign.rename(columns=col_map, inplace=True)

    # FIXME is this supposed to duplicate or rename?
    # consumers['input_commodity'] = consumers['NAICS']

    # FIXME store consumers as a dictionary of data-frame for ease of storage
    consumers = dict(tuple(consumers.groupby(['input_commodity', 'SCTG'])))
    consumers_foreign = dict(tuple(consumers_foreign.groupby(['input_commodity', 'SCTG'])))
    consumers = {naics_sctg: df.append(consumers_foreign.get(naics_sctg), ignore_index=True) for
                 naics_sctg, df in consumers.iteritems()}

    return consumers


def calc_sample_groups(nconsumers, nproducers):
    SUPPLIERS_PER_BUYER = 20
    COMBINATION_THRESHOLD = 3500000
    CONS_PROD_RATIO_LIMIT = 1000000

    n_groups = 1
    nconst = nconsumers
    nprodt = nproducers

    split_prod = (nconsumers / nproducers) < CONS_PROD_RATIO_LIMIT

    if split_prod:
        while nconst * SUPPLIERS_PER_BUYER > COMBINATION_THRESHOLD:
            n_groups += 1
            nconst = math.ceil(nconsumers / n_groups)
            nprodt = math.ceil(nproducers / n_groups)
    else:
        while (nconst * SUPPLIERS_PER_BUYER > COMBINATION_THRESHOLD or
               nconst / nprodt > CONS_PROD_RATIO_LIMIT):
            n_groups += 1
            nconst = math.ceil(nconsumers / n_groups)

    return nprodt, nconst, split_prod, n_groups


def est_sim_naics_set(producers, consumers):
    """
    output summaries

    """

    # Convert producers and consumers to data-frame
    producers = pd.concat(producers, axis=0)
    producers.reset_index(inplace=True, drop=True)
    consumers = pd.concat(consumers, axis=0)
    consumers.reset_index(inplace=True, drop=True)

    # - matching consumers and suppliers -- by NAICS codes
    producers_summary_naics = \
        producers[['output_commodity', 'SCTG', 'size', 'output_capacity_tons']]. \
        groupby(['output_commodity', 'SCTG']). \
        agg({'size': ['count', 'sum'], 'output_capacity_tons': 'sum'})
    producers_summary_naics.columns = producers_summary_naics.columns.map('_'.join)
    producers_summary_naics.reset_index(inplace=True)  # explicit output_commodity column
    producers_summary_naics.rename(columns={'size_count': 'producers',
                                            'size_sum': 'employment',
                                            'output_capacity_tons_sum': 'output_capacity',
                                            'output_commodity': 'NAICS'},
                                   inplace=True)

    consumers_summary_naics = \
        consumers[['input_commodity', 'SCTG', 'size', 'purchase_amount_tons']]. \
        groupby(['input_commodity', 'SCTG']). \
        agg({'size': ['count', 'sum'], 'purchase_amount_tons': 'sum'})
    consumers_summary_naics.columns = consumers_summary_naics.columns.map('_'.join)
    consumers_summary_naics.reset_index(inplace=True)  # explicit input_commodity column
    consumers_summary_naics.rename(columns={'size_count': 'consumers',
                                            'size_sum': 'employment',
                                            'purchase_amount_tons_sum': 'input_requirements',
                                            'input_commodity': 'NAICS'},
                                   inplace=True)

    match_summary_naics = pd.merge(
        left=producers_summary_naics[['NAICS', 'SCTG', 'producers', 'output_capacity']],
        right=consumers_summary_naics[['NAICS', 'SCTG', 'consumers', 'input_requirements']],
        left_on=['NAICS', 'SCTG'],
        right_on=['NAICS', 'SCTG'],
        how='outer'
    )

    match_summary_naics = match_summary_naics[["NAICS", "SCTG", "producers", "consumers",
                                               "output_capacity", "input_requirements"]]

    match_summary_naics['ratio_output'] = \
        match_summary_naics.output_capacity / match_summary_naics.input_requirements
    match_summary_naics['possible_matches'] = \
        match_summary_naics.producers * match_summary_naics.consumers

    # - Get number of (none NA) matches by NAICS, and check for imbalance in producers and consumers

    naics_set = match_summary_naics[~match_summary_naics.possible_matches.isnull()]
    naics_set = naics_set[["NAICS", "SCTG", "producers", "consumers", "possible_matches"]]

    naics_set['con_prod_ratio'] = naics_set.consumers / naics_set.producers

    # calculate group sizes for each commodity so that all groups are less than threshold
    # Either cut both consumers and producers or just consumers (with all producers in each group)
    naics_set['nprodt'], naics_set['nconst'], naics_set['split_prod'], naics_set['n_groups'] = \
        zip(*naics_set.apply(
            lambda x: calc_sample_groups(x['consumers'], x['producers']), axis='columns'))

    # FIXME not sure it is worth building these until we know how they will be used
    #   #key the tables for faster subsetting on naics codes
    #   setkey(consumers,InputCommodity)
    #   setkey(producers,OutputCommodity)
    #
    #   for (naics in naics_set$NAICS) {
    #     # Construct data.tables for just the current commodity
    #     consc <- consumers[naics,]
    #     prodc <- producers[naics,]
    #     #write the tables to an R data file
    #     save(consc,prodc, file = file.path(SCENARIO_OUTPUT_PATH,paste0(naics, ".Rdata")))
    #   }
    naics_set.NAICS = naics_set.NAICS.astype('str')
    match_summary_naics.NAICS = match_summary_naics.NAICS.apply(lambda x: x.encode('ascii'))

    return match_summary_naics, naics_set


def est_sim_summary(output_dir, producers, consumers, ests, est_pref_weights):
    """
    summarize CBP, Producers and Consumers
    """

    # Convert producers and consumers to data-frame
    producers = pd.concat(producers, axis=0)
    producers.reset_index(inplace=True, drop=True)
    consumers = pd.concat(consumers, axis=0)
    consumers.reset_index(inplace=True, drop=True)

    est_sum = collections.OrderedDict()

    est_sum['ests'] = ests.shape[0]
    est_sum['ests.producer'] = ests.producer.sum()
    est_sum['ests.maker'] = ests.producer.sum()
    est_sum['employment'] = ests.emp.sum()

    # - ests_emp_by_sctg
    est_emp_by_sctg = ests[['SCTG', 'emp']].groupby('SCTG').emp. \
        agg(['sum', 'count']). \
        rename(columns={'sum': 'employment', 'count': 'establishments'}). \
        reset_index()
    est_emp_by_sctg['SCTG_name'] = \
        reindex(est_pref_weights.SCTG_description, est_emp_by_sctg.SCTG)
    est_emp_by_sctg.to_csv(os.path.join(output_dir, "ests_emp_by_sctg.csv"), index=False)

    # - producers

    # FIXME - 'producers' should be unique or cartesian?
    est_sum['producers_rows'] = producers.shape[0]
    est_sum['producers_unique'] = producers.seller_id.nunique()
    est_sum['producers_emp'] = producers.size.sum()
    est_sum['producers_cap'] = producers.output_capacity_tons.sum()

    # - producers_emp_by_sctg
    producers_emp_by_sctg = \
        producers[['SCTG', 'size', 'output_capacity_tons']]. \
        groupby('SCTG'). \
        agg({'size': ['count', 'sum'], 'output_capacity_tons': 'sum'})
    producers_emp_by_sctg.columns = producers_emp_by_sctg.columns.map('_'.join)
    producers_emp_by_sctg.rename(columns={'size_count': 'producers',
                                          'size_sum': 'employment',
                                          'output_capacity_tons_sum': 'output_capacity'},
                                 inplace=True)
    producers_emp_by_sctg.reset_index(inplace=True)  # explicit SCTG column
    producers_emp_by_sctg['SCTG_name'] = \
        reindex(est_pref_weights.SCTG_description, producers_emp_by_sctg.SCTG)
    producers_emp_by_sctg.to_csv(os.path.join(output_dir, "producers_emp_by_sctg.csv"),
                                 index=False)

    # - consumers

    est_sum['consumers_unique'] = consumers.buyer_id.nunique()
    est_sum['consumption_pairs'] = consumers.shape[0]
    est_sum['threshold'] = base_variables.BASE_PROVALTHRESHOLD
    est_sum['consumer_inputs'] = consumers.purchase_amount_tons.sum()

    # - consumers_by_sctg
    consumers_by_sctg = consumers[['SCTG', 'purchase_amount_tons']]. \
        groupby('SCTG').purchase_amount_tons.agg(['sum', 'count']). \
        rename(columns={'sum': 'input_requirements', 'count': 'consumers'}). \
        reset_index()

    consumers_by_sctg['SCTG_name'] = \
        reindex(est_pref_weights.SCTG_description, consumers_by_sctg.SCTG)
    consumers_by_sctg.to_csv(os.path.join(output_dir, "consumers_by_sctg.csv"), index=False)

    # - matching consumers and suppliers -- by SCTG category

    match_summary = pd.merge(
        left=producers_emp_by_sctg[['SCTG', 'SCTG_name', 'producers', 'output_capacity']],
        right=consumers_by_sctg[['SCTG', 'consumers', 'input_requirements']],
        left_on='SCTG',
        right_on='SCTG'
    ).rename(columns={'SCTG': 'commodity'})
    # reorder fields
    match_summary = match_summary[["commodity", "SCTG_name", "producers", "consumers",
                                   "output_capacity", "input_requirements"]]
    match_summary['ratio_output'] = match_summary.output_capacity / match_summary.input_requirements
    match_summary['possible_matches'] = match_summary.producers * match_summary.consumers
    match_summary.to_csv(os.path.join(output_dir, "matches.csv"), index=False)

    # - write ests_sum
    summary = pd.DataFrame({'key': est_sum.keys(), 'value': est_sum.values()})
    summary.to_csv(os.path.join(output_dir, "est_sim_summary.csv"), index=False)


def regress(df, step_name, df_name):
    file_path = 'regression_data/results/%s/outputs/x_%s.csv' % (step_name, df_name)
    df.to_csv(file_path, index=True, chunksize=100000L)


def est_firm_extract(est_firms):
    est_cols = ['pnaics', 'NAICS2012', 'county_FIPS', 'state_FIPS', 'FAF4', 'TAZ', 'n2', 'n4',
                'esizecat', 'emp']
    firm_cols = ['firm_id', 'naics_firm', 'county_FIPS_firm', 'state_FIPS_firm', 'FAF4_firm',
                 'TAZ_firm', 'n2_firm', 'n4_firm', 'fsizecat']
    est = est_firms.loc[:, est_cols]
    firms = est_firms.loc[:, firm_cols]
    return est, firms


@inject.step()
def est_synthesis(
        output_dir,
        NAICS2012_to_NAICS2007,
        NAICS2007_to_NAICS2007io,
        NAICS2012_to_NAICS2007io,
        employment_categories,
        firm_employment_categories,
        naics_industry,
        industry_10_5,
        naics_empcat,
        socio_economics_taz,
        NAICS2007io_to_SCTG,
        input_output_values,
        taz_fips,
        taz_faf4,
        unit_cost,
        # numa_dist,
        est_pref_weights,
        foreign_prod_values,
        foreign_cons_values
):
    REGRESS = False
    TRACE_TAZ = None

    t0 = print_elapsed_time()

    NAICS2012_to_NAICS2007 = NAICS2012_to_NAICS2007.to_frame()
    NAICS2007_to_NAICS2007io = NAICS2007_to_NAICS2007io.to_frame()
    NAICS2012_to_NAICS2007io = NAICS2012_to_NAICS2007io.to_frame()
    employment_categories = employment_categories.to_frame()
    firm_employment_categories = firm_employment_categories.to_frame()
    naics_industry = naics_industry.to_frame()
    industry_10_5 = industry_10_5.to_frame()
    naics_empcat = naics_empcat.to_frame()
    socio_economics_taz = socio_economics_taz.to_frame()
    NAICS2007io_to_SCTG = NAICS2007io_to_SCTG.to_frame()
    input_output_values = input_output_values.to_frame()
    unit_cost = unit_cost.to_frame()
    # numa_dist = numa_dist.to_frame()
    est_pref_weights = est_pref_weights.to_frame()
    foreign_prod_values = foreign_prod_values.to_frame()
    foreign_cons_values = foreign_cons_values.to_frame()    
    taz_fips = taz_fips.to_frame()
    taz_faf4 = taz_faf4.to_frame()
    # ests_est = ests_est.to_frame()

    t0 = print_elapsed_time("load dataframes", t0, debug=True)

    # - est_sim_load_ests
    t0 = print_elapsed_time()
    # est_firms = est_sim_load_establishments(NAICS2012_to_NAICS2007)
    est = est_sim_load_establishments(NAICS2012_to_NAICS2007)
    t0 = print_elapsed_time("est_sim_load_ests", t0, debug=True)

    # if REGRESS:
    #     # use uncorrected naics for regression
    #     logger.warn("using uncorrected ests.naics codes for regression")
    #     ests.NAICS2012 = ests.pnaics

    logger.info("%s ests" % (est.shape[0],))

    # # - separate firms and establishments
    # t0 = print_elapsed_time()
    # est, firms = est_firm_extract(est_firms)
    # t0 = print_elapsed_time("est_firm_extract", t0, debug=True)

    # - est_sim_enumerate
    t0 = print_elapsed_time()
    est = est_sim_enumerate(est, NAICS2012_to_NAICS2007io, employment_categories,
                            naics_industry, industry_10_5)
    t0 = print_elapsed_time("est_sim_enumerate", t0, debug=True)

    logger.info("%s null NAICS_make in ests" % (est.NAICS6_make.isnull().sum(),))

    if REGRESS:
        regress(df=est, step_name='est_sim_enumerate', df_name='Ests')

    # - est_sim_enumerate_foreign
    t0 = print_elapsed_time()
    est = est_sim_enumerate_foreign(est, NAICS2012_to_NAICS2007io, NAICS2007io_to_SCTG,
                                    employment_categories, naics_industry, industry_10_5,
                                    foreign_prod_values, foreign_cons_values)
    t0 = print_elapsed_time("est_sim_enumerate_foreign", t0, debug=True)
    logger.info("%s null NAICS_make in ests" % (est.NAICS6_make.isnull().sum(),))

    if REGRESS:
        regress(df=est, step_name='est_sim_enumerate_foreign', df_name='Ests')

    # - est_sim_taz_allocation
    t0 = print_elapsed_time()
    est = est_sim_taz_allocation(est, naics_empcat)
    t0 = print_elapsed_time("est_sim_taz_allocation", t0, debug=True)

    if REGRESS:
        regress(df=est, step_name='est_sim_tazallocation', df_name='Ests')
    if TRACE_TAZ is not None:
        print "\nest_sim_tazallocation TRACE_TAZ %s ests\n" % \
              TRACE_TAZ, est[est.TAZ == TRACE_TAZ][['TAZ', 'model_emp_cat', 'emp']].sort_index()

    # - est_sim_scale_employees
    t0 = print_elapsed_time()
    # est, firms = est_sim_scale_employees(est, firms, employment_categories,
    #                                      firm_employment_categories, naics_empcat,
    #                                      socio_economics_taz, taz_fips, taz_faf4, numa_dist)
    est = est_sim_scale_employees(est, 
                                  employment_categories,
                                  naics_empcat,
                                  socio_economics_taz, 
                                  taz_fips, 
                                  taz_faf4)
    t0 = print_elapsed_time("est_sim_scale_employees", t0, debug=True)

    if REGRESS:
        regress(df=est, step_name='est_sim_scale_employees', df_name='Ests')
    if TRACE_TAZ is not None:
        print "\nest_sim_scale_employees TRACE_TAZ %s ests\n" % \
              TRACE_TAZ, est[est.TAZ == TRACE_TAZ][['TAZ', 'NAICS2007', 'NAICS6_make', 'emp']]

    # - est_sim_assign_SCTG
    t0 = print_elapsed_time()
    est = est_sim_assign_SCTG(est, NAICS2007io_to_SCTG)
    t0 = print_elapsed_time("est_sim_assign_SCTG", t0, debug=True)

    logger.debug("%s ests with null SCTG" % est.SCTG.isnull().sum())
    logger.debug("%s ests with null NAICS6_make" % est.NAICS6_make.isnull().sum())

    if REGRESS:
        regress(df=est, step_name='est_sim_sctg', df_name='Ests')
    if TRACE_TAZ is not None:
        print "\nest_sim_assign_SCTG TRACE_TAZ %s ests\n" % \
              TRACE_TAZ, est[est.TAZ == TRACE_TAZ][['TAZ', 'NAICS2007', 'NAICS6_make', 'SCTG']]

    # - est_sim_types
    t0 = print_elapsed_time()
    est = est_sim_types(est)
    t0 = print_elapsed_time("est_sim_types", t0, debug=True)

    logger.info('%s ests, %s producers, %s makers' %
                (est.shape[0], est.producer.sum(), est.maker.sum()))

    if REGRESS:
        regress(df=est, step_name='est_sim_types', df_name='Ests')
    if TRACE_TAZ is not None:
        print "\nest_sim_types TRACE_TAZ %s ests\n" % \
              TRACE_TAZ, est[est.TAZ == TRACE_TAZ][['TAZ', 'SCTG', 'producer', 'maker']]

    # - est_sim_producers
    t0 = print_elapsed_time()
    producers, MAX_BUS_ID = est_sim_producers(est, input_output_values, unit_cost)
    t0 = print_elapsed_time("est_sim_producers", t0, debug=True)

    logger.info('%s producers' % (np.sum([df.shape[0] for df in producers.itervalues()]),))

    if REGRESS:
        regress(df=producers, step_name='est_sim_producers', df_name='producers')

    # - est_sim_consumers
    t0 = print_elapsed_time()
    consumers = est_sim_consumers(est, input_output_values, NAICS2007io_to_SCTG, unit_cost,
                                  est_pref_weights, MAX_BUS_ID)
    t0 = print_elapsed_time("est_sim_consumers", t0, debug=True)

    logger.info('%s consumers' % (np.sum([df.shape[0] for df in consumers.itervalues()]),))

    if REGRESS:
        regress(df=consumers, step_name='est_sim_consumers', df_name='consumers')
    if TRACE_TAZ is not None:
        print "\nest_sim_consumers TRACE_TAZ %s consumers\n" % \
              TRACE_TAZ, consumers[consumers.TAZ == TRACE_TAZ].sort_values('bus_id')

    # - est_sim_naics_set
    t0 = print_elapsed_time()
    matches_naics, naics_set = est_sim_naics_set(
        producers,
        consumers)
    t0 = print_elapsed_time("est_sim_naics_set", t0, debug=True)

    if REGRESS:
        regress(df=naics_set, step_name='est_sim_naics_set', df_name='naics_set')

    # - est_sim_summary
    t0 = print_elapsed_time()
    est_sim_summary(output_dir, producers, consumers, est, est_pref_weights)
    t0 = print_elapsed_time("est_sim_summary", t0, debug=True)

    inject.add_table('establishments', est.reset_index())
    # inject.add_table('firms', firms.reset_index())
    for naics_sctg, df in producers.iteritems():
        table_name = 'producers_' + '_'.join(map(str, naics_sctg)).partition('.')[0]
        inject.add_table(table_name, df)
    for naics_sctg, df in consumers.iteritems():
        table_name = 'consumers_' + '_'.join(map(str, naics_sctg)).partition('.')[0]
        inject.add_table(table_name, df)
    inject.add_table('matches_naics', matches_naics)
    inject.add_table('naics_set', naics_set)
