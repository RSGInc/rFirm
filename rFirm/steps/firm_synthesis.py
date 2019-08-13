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

from rFirm.util import read_table
from rFirm.util import round_preserve_threshold

import rFirm.base_variables as base_variables

logger = logging.getLogger(__name__)


def firm_sim_load_firms(NAICS2012_to_NAICS2007):
    """
    For now, just read FirmsandEstablishments.cvs file created by dev/NFM_Firmsynthesis.R
    """

    data_dir = setting('data_dir', inject.get_injectable('data_dir'))

    table_list_name = 'firm_synthesis_tables'
    table_list = setting(table_list_name)
    assert table_list is not None

    firms = read_table('firms_establishments', table_list['firms_establishments'], data_dir)

    # FIXME for regression
    firms = firms.sort_index()

    # The firms table has 2012 naics code
    firms['NAICS2012'] = firms.naics

    assert firms.NAICS2012.isin(NAICS2012_to_NAICS2007.NAICS2012).all()

    # make sure nobody is using this unawares
    firms.rename(columns={'naics': 'pnaics'}, inplace=True)

    assert firms.index.is_unique

    return firms


def firm_sim_enumerate(
        firms,
        NAICS2012_to_NAICS2007io,
        employment_categories,
        naics_industry,
        industry_10_5):
    """

    enumerate firms

    """

    # - Look up NAICS2007io classifications
    t0 = print_elapsed_time()

    # Only use domestic firms
    firms = firms[firms.FAF4 < 800].copy()

    # Merge in the single-NAICSio naics
    firms['NAICS6_make'] = reindex(
        NAICS2012_to_NAICS2007io[NAICS2012_to_NAICS2007io.proportion == 1].
        set_index('NAICS').NAICSio, firms.NAICS2012
    )

    t0 = print_elapsed_time("Merge in the single-NAICSio naics", t0, debug=True)

    # random select NAICSio 2007 code for multi 2012 naics code
    # based on proportions (probabilities)
    multi_NAICS2012_to_NAICS2007io = \
        NAICS2012_to_NAICS2007io[NAICS2012_to_NAICS2007io.proportion < 1]
    multi_naics2007io_naics2012_codes = multi_NAICS2012_to_NAICS2007io.NAICS.unique()
    multi_naics2007io_firms = firms[firms.NAICS2012.isin(multi_naics2007io_naics2012_codes)]

    prng = pipeline.get_rn_generator().get_global_rng()
    for naics, naics_firms in multi_naics2007io_firms.groupby('NAICS2012'):
        # slice the NAICS2012_to_NAICS2007io rows for this naics 2012 code
        naics_naicsio = \
            multi_NAICS2012_to_NAICS2007io[multi_NAICS2012_to_NAICS2007io.NAICS == naics]

        # choose a random NAICS2007io code for each business with this naics 2012 code
        naicsio = prng.choice(naics_naicsio.NAICSio.values,
                              size=len(naics_firms),
                              p=naics_naicsio.proportion.values,
                              replace=True)

        firms.loc[naics_firms.index, 'NAICS6_make'] = naicsio

    t0 = print_elapsed_time("choose 2007 NAICSio for multi-2012 naics", t0, debug=True)

    if firms.NAICS6_make.isnull().any():
        logger.error("%s null NAICS6_make codes in firms" % firms.NAICS6_make.isnull().sum())

    # - Derive 2, 3, and 4 digit NAICS codes
    firms['n4'] = (firms.NAICS2012 / 100).astype(int)
    firms['n3'] = (firms.n4 / 10).astype(int)
    firms['n2'] = (firms.n3 / 10).astype(int)
    assert ~firms.n2.isnull().any()

    # - recode esizecat map from str employment_categories.esizecat to int employment_categories.id
    firms['esizecat'] = reindex(employment_categories.set_index('esizecat').id, firms.esizecat)
    firms['emp_range'] = reindex(employment_categories.set_index('id').emp_range, firms.esizecat)
    firms['low_emp'] = reindex(employment_categories.set_index('id').low_threshold, firms.esizecat)

    # - Add 10 level industry classification
    firms['industry10'] = reindex(naics_industry.industry, firms.n3).astype(str)

    if firms.industry10.isnull().any():
        unmatched_naics = firms[firms.industry10.isnull()].n3.unique()
        logger.error("%s unmatched n3 naics codes in naics_industry" % len(unmatched_naics))
        print "\nnon matching n3 naics codes\n", unmatched_naics
        firms.industry10.fillna('', inplace=True)

    # - Add 5 level industry classification
    firms['industry5'] = reindex(industry_10_5.industry5, firms.industry10)

    if firms.industry5.isnull().any():
        unmatched_industry_10 = firms[firms.industry5.isnull()].n3.unique()
        logger.error("%s unmatched industry_10 codes in industry_10_5" % len(unmatched_industry_10))
        print "\nnon matching industry_10 codes\n", unmatched_industry_10
        firms.industry10.fillna('', inplace=True)

    assert firms.index.is_unique

    return firms


def firm_sim_enumerate_foreign(firms,
                               NAICS2007io_to_SCTG,
                               employment_categories,
                               naics_industry,
                               industry_10_5,
                               foreign_prod_values, foreign_cons_values):
    # Both for_prod and for_cons include foreign public production/consumption value.
    # Reallocate within each country to the remaining privately owned industries in
    # proportion to their prod/cons value
    # In the future, research commodity shares for public production and consumption and
    # allocate in a more detailed way

    # Foreign production
    no_pub_codes = [91000, 92000, 98000, 99000]
    foreign_prod_sum = foreign_prod_values.groupby(['FAF4', 'CBPZONE']).agg({'pro_val': sum})
    # Private transactions do not appear to exist in the foreign trade data
    foreign_prod_sum_private = foreign_prod_values[~foreign_prod_values.NAICS6.isin(no_pub_codes)].\
        groupby(['FAF4', 'CBPZONE']).agg({'pro_val': sum})
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
                                          loc[foreign_prod_values.CBPZONE.values,
                                              'prod_scale'].values *
                                          foreign_prod_faf.loc[foreign_prod_values.FAF4.values,
                                                               'prod_scale'].values).astype(int)

    # Foreign consumption
    foreign_cons_sum = foreign_cons_values.groupby(['FAF4', 'CBPZONE']).agg({'con_val': sum})
    # Private transactions do not appear to exist in the foreign trade data
    foreign_cons_sum_private = \
        foreign_cons_values[~foreign_cons_values.NAICS6.isin(no_pub_codes)]. \
        groupby(['FAF4', 'CBPZONE']).agg({'con_val': sum})
    foreign_cons_sum = pd.merge(foreign_cons_sum, foreign_cons_sum_private,
                                how='outer', left_index=True, right_index=True,
                                suffixes=['', '_private'])
    foreign_cons_sum.con_val_private.fillna(0, inplace=True)
    foreign_cons_sum = \
        foreign_cons_sum.assign(con_scale=lambda df: np.where(df.con_val_private > 0,
                                                              df.con_val / df.con_val_private,
                                                              0))

    # Account for countries with no non-public production by reallocating within FAF ZONE
    # so FAF ZONE production is conserved
    foreign_cons_faf = foreign_cons_sum.reset_index().groupby(['FAF4']). \
        apply(lambda dfg: dfg.con_val.sum() / dfg.con_val_private.sum()).to_frame('con_scale')

    # Do the scaling for foreign production
    foreign_cons_values = foreign_cons_values[~foreign_cons_values.NAICS6.isin(no_pub_codes)].copy()
    foreign_cons_values['con_val_new'] = (foreign_cons_values.con_val.values *
                                          foreign_cons_sum.reset_index('FAF4', drop=True).
                                          loc[foreign_cons_values.CBPZONE.values,
                                              'con_scale'].values *
                                          foreign_cons_faf.loc[foreign_cons_values.FAF4.values,
                                                               'con_scale'].values).astype(int)

    # Enumerate foreign producers and consumers
    # Create one agent per country per commodity
    # Remove any records where the country and FAF zone are not known
    foreign_producers = foreign_prod_values[~foreign_prod_values.FAF4.isnull()][
        ['NAICS6', 'NAICSio', 'pro_val_new', 'CBPZONE', 'FAF4']
    ].rename(columns={'pro_val_new': 'prod_val'}).assign(prod_val=lambda df: df.prod_val / 1e6)
    foreign_consumers = foreign_cons_values[~foreign_cons_values.FAF4.isnull()][
        ['NAICS6', 'NAICSio', 'con_val_new', 'CBPZONE', 'FAF4']
    ].rename(columns={'con_val_new': 'prod_val'}).assign(prod_val=lambda df: df.prod_val / 1e6)

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
    for naics, naics_firms in multi_sctg_producers.groupby('NAICSio'):
        # slice the NAICS2007io_to_SCTG rows for this naics
        naics_sctgs = multi_NAICS2007io_to_SCTG[multi_NAICS2007io_to_SCTG.NAICSio == naics]

        # choose a random SCTG code for each business with this naics code
        sctgs = prng.choice(naics_sctgs.SCTG.values,
                            size=len(naics_firms),
                            p=naics_sctgs.proportion.values,
                            replace=True)

        foreign_producers.loc[naics_firms.index, 'SCTG'] = sctgs

    for naics, naics_firms in multi_sctg_consumers.groupby('NAICSio'):
        # slice the NAICS2007io_to_SCTG rows for this naics
        naics_sctgs = multi_NAICS2007io_to_SCTG[multi_NAICS2007io_to_SCTG.NAICSio == naics]

        # choose a random SCTG code for each business with this naics code
        sctgs = prng.choice(naics_sctgs.SCTG.values,
                            size=len(naics_firms),
                            p=naics_sctgs.proportion.values,
                            replace=True)

        foreign_consumers.loc[naics_firms.index, 'SCTG'] = sctgs

    foreign_producers['n4'] = (foreign_producers.NAICS6 / 100).astype(int)
    foreign_consumers['n4'] = (foreign_consumers.NAICS6 / 100).astype(int)

    KeyMap = collections.namedtuple('KeyMap',
                                    ['key_col', 'key_value', 'target', 'target_values', 'probs'])
    # Identify firms who make 2+ commodities (especially wholesalers) and simulate a specific
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
            prod_val = temp_fp['prod_val']
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
            prod_val = temp_fp['prod_val']
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

    firms_foreign = pd.concat([foreign_producers, foreign_consumers], ignore_index=True)
    firms_foreign.rename(columns={'NAICS6': 'NAICS2007', 'NAICSio': 'NAICS6_make'},
                         inplace=True)
    # - Derive 2, 3, and 4 digit NAICS codes
    firms_foreign['n4'] = (firms_foreign.NAICS2007 / 100).astype(int)
    firms_foreign['n3'] = (firms_foreign.n4 / 10).astype(int)
    firms_foreign['n2'] = (firms_foreign.n3 / 10).astype(int)
    assert ~firms_foreign.n2.isnull().any()

    # - code esizecat
    firms_foreign['esizecat'] = firms.esizecat.max()
    firms_foreign['emp_range'] = reindex(employment_categories.set_index('id').emp_range,
                                         firms_foreign.esizecat)
    firms_foreign['low_emp'] = reindex(employment_categories.set_index('id').low_threshold,
                                       firms_foreign.esizecat)

    # - Add 10 level industry classification
    firms_foreign['industry10'] = reindex(naics_industry.industry, firms_foreign.n3).astype(str)

    if firms_foreign.industry10.isnull().any():
        unmatched_naics = firms_foreign[firms_foreign.industry10.isnull()].n3.unique()
        logger.error("%s unmatched n3 naics codes in naics_industry" % len(unmatched_naics))
        print "\nnon matching n3 naics codes\n", unmatched_naics
        firms_foreign.industry10.fillna('', inplace=True)

    # - Add 5 level industry classification
    firms_foreign['industry5'] = reindex(industry_10_5.industry5, firms_foreign.industry10)

    if firms_foreign.industry5.isnull().any():
        unmatched_industry_10 = firms_foreign[firms_foreign.industry5.isnull()].n3.unique()
        logger.error("%s unmatched industry_10 codes in industry_10_5" % len(unmatched_industry_10))
        print "\nnon matching industry_10 codes\n", unmatched_industry_10
        firms_foreign.industry10.fillna('', inplace=True)

    # Reindex foreign firms
    MAX_BUS_ID = firms.index.max()
    logger.info("assigning foreign firm indexes starting above MAX_BUS_ID %s" % (MAX_BUS_ID,))
    firms_foreign.index = firms_foreign.index + MAX_BUS_ID + 1L
    firms_foreign.index.name = firms.index.name

    firms = pd.concat([firms, firms_foreign])
    firms.loc[firms.FAF4 > 800, 'TAZ'] = 0
    firms.loc[firms.FAF4 > 800, 'county_FIPS'] = 0
    firms.loc[firms.FAF4 > 800, 'state_FIPS'] = 0

    assert firms.index.is_unique
    return firms


def firm_sim_taz_allocation(firms, naics_empcat):
    # most of the R code serves no purpose when there is only one level of TAZ

    # - Assign the model employment category to each firm
    firms['model_emp_cat'] = reindex(naics_empcat.model_emp_cat, firms.n2)

    assert ~firms.model_emp_cat.isnull().any()

    assert firms.index.is_unique

    return firms


def scale_cbp_to_se(employment, firms):
    employment['adjustment'] = employment.emp_SE / employment.emp_CBP
    employment.adjustment.replace(np.inf, np.nan, inplace=True)

    # add adjustment factor for each firm based on its TAZ and model_emp_cat
    on_cols = ['TAZ', 'model_emp_cat']
    index_name = firms.index.name or 'index'
    firms = pd.merge(
        left=firms.reset_index(),
        right=employment[on_cols + ['adjustment']],
        on=on_cols).set_index(index_name)

    # - Scale employees in firms table and bucket round
    # FIXME any reason not to replace bucket round with target round
    firms.adjustment = firms.adjustment.fillna(1.0)  # adjustment factor of 1.0 has no effect
    firms.emp = firms.emp * firms.adjustment
    firms.emp = round_preserve_threshold(firms.emp.values)

    del firms['adjustment']
    del employment['adjustment']
    return firms


def firm_emp_generator(
        low_emp,
        emp_range,
        interpolate_model,
        firm_length=1
):
    """
    Generates employee number based on values provided randomly
    :param firm_length:
    :param low_emp:
    :param emp_range:
    :param interpolate_model:
    :return: firms_emp
    """
    max_emp = low_emp + emp_range
    emp_n = [i for i in xrange(low_emp, max_emp)]
    inter_val = interpolate_model(emp_n)
    prob_emp = inter_val / np.sum(inter_val)
    prng = pipeline.get_rn_generator().get_global_rng()
    firms_emp = prng.choice(emp_n,
                            size=firm_length,
                            replace=True,
                            p=prob_emp)
    return firms_emp


def firm_sim_scale_employees(
        firms,
        employment_categories,
        naics_empcat,
        socio_economics_taz,
        numa_dist):
    """
     Scale Firms to Employment Forecasts
    """

    # Separate out the foreign firms
    firms_foreign = firms[firms.FAF4 > 800]
    firms = firms[firms.FAF4 < 800].copy()

    # Assign employment based on the proportion of firms obtained by
    # interpolating number of firms between low and high value of
    # employment range

    # number of establishments by employment size category
    firms_size = firms.groupby(['esizecat', 'emp_range', 'low_emp']).size().to_frame('est_n').\
        reset_index()
    firms_size['est_emp_range'] = firms_size['est_n'] / firms_size['emp_range']
    # Hyman interpolator to interpolate number of establishments between the low and
    # high value of the employment range
    hyman_interpol = interpolate.PchipInterpolator(firms_size['low_emp'],
                                                   firms_size['est_emp_range'])

    firms_emp = firms.groupby(['esizecat', 'low_emp', 'emp_range'],
                              group_keys=False) \
        .apply(lambda x: {'emp': firm_emp_generator(x.low_emp.unique(),
                                                    x.emp_range.unique(),
                                                    hyman_interpol,
                                                    x.shape[0]),
                          'bus_id': x.index.tolist()})
    firms_emp = pd.concat([pd.DataFrame.from_dict(x) for x in firms_emp.to_frame('firm_emp').
                          reset_index().firm_emp.tolist()],
                          axis=0)
    # firms_emp.loc[firms_emp.emp > 5000, 'emp'] = 5000
    firms_emp.set_index('bus_id', inplace=True)
    firms.loc[firms_emp.index, 'emp'] = firms_emp.emp.values

    # employment categories that
    model_emp_cats = naics_empcat.model_emp_cat.unique()

    # we expect all lehd_naics_empcat categories to appear as columns in socio_economics_taz
    assert set(model_emp_cats).issubset(set(socio_economics_taz.columns.values))

    # just want those columns plus TAZ so we can melt
    socio_economics_taz = socio_economics_taz[model_emp_cats]

    # columns that firm_sim_scale_employees_taz (allegedly) expects in firms
    region_firm_cols = \
        ['TAZ', 'county_FIPS', 'state_FIPS', 'FAF4', 'NAICS2012', 'n4', 'n3', 'n2',
         'NAICS6_make', 'industry10', 'ind'
                                      'ustry5', 'model_emp_cat', 'esizecat',
         'emp']

    firms = firms[region_firm_cols].copy()
    MAX_BUS_ID = firms.index.max()
    logger.info("assigning new firm indexes starting above MAX_BUS_ID %s" % (MAX_BUS_ID,))

    # - Compare employment between socio-economic input (SE) and synthetized firms (CBP)

    # Summarize employment of synthesized firms by TAZ and Model_EmpCat
    employment = firms.groupby(['TAZ', 'model_emp_cat'])['emp'].sum()
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
    # 1. First relocate firms to TAZ where CBP has 0 employment and SE has more than 0
    #    employment.
    # 2. Final relocate firms from TAZ where the CBP number of establishments is more than
    #    total SE employment.

    employment = pd.merge(employment,
                          firms.groupby(['TAZ', 'model_emp_cat']).size().
                          to_frame('n_est').reset_index(),
                          on=['TAZ', 'model_emp_cat'],
                          how='outer').fillna(0)
    employment['avg_emp'] = reindex(firms.groupby('model_emp_cat')['emp'].median(),
                                    employment.model_emp_cat)

    # Identify TAZ and model_emp_cat that needs to be reallocated somewhere else
    employment['to_assign'] = employment['emp_CBP'] < 1
    employment['to_distribute'] = (~(employment['to_assign']) & ((employment['n_est']) > 1))
    employment['to_move'] = (employment['emp_SE'] / employment['avg_emp']).apply(np.ceil)
    employment.loc[employment['emp_CBP'] > 0, 'to_move'] = 0.0

    # Employment Category Absent from CBP
    emp_cats_not_in_firms = ['gov']

    rem_assign = employment[~employment.model_emp_cat.isin(emp_cats_not_in_firms)].to_assign.sum()
    numa_dist = numa_dist[numa_dist.oTAZ != numa_dist.dTAZ]
    firms['orig_TAZ'] = firms['TAZ']

    t0 = print_elapsed_time()

    prng = pipeline.get_rn_generator().get_global_rng()
    while rem_assign > 0:
        for model_emp_cat in employment[employment.to_assign].model_emp_cat.unique():
            if model_emp_cat in emp_cats_not_in_firms:
                continue
            # print 'Adjusting employment category: %s\n' % model_emp_cat
            # Get a subset of data to work with
            firms_sub = firms[firms.model_emp_cat == model_emp_cat].copy()
            emp_taz = employment[employment.model_emp_cat == model_emp_cat]
            firms_sub['to_move'] = reindex(emp_taz.set_index('TAZ')['to_move'], firms_sub.TAZ)
            emp_assign = emp_taz[emp_taz.to_assign]
            emp_distribute = emp_taz[emp_taz.to_distribute]
            if (emp_assign.shape[0] > 0) & (emp_distribute.shape[0] > 0):
                # Get possible TAZ candidates for relocation
                emp_assign = pd.merge(numa_dist[numa_dist.dTAZ.isin(emp_distribute.TAZ)],
                                      emp_assign[['TAZ', 'to_move', 'avg_emp']],
                                      how='inner',
                                      left_on='oTAZ',
                                      right_on='TAZ')
                emp_assign['emp_avail'] = reindex(
                    emp_distribute.set_index('TAZ').apply(lambda df: df.emp_SE - df.emp_CBP,
                                                          axis=1),
                    emp_assign.dTAZ).fillna(0).clip_lower(0)
                dist_multiplier = 1000.0
                emp_multiplier = 0.01
                emp_assign['prob'] = (dist_multiplier * emp_assign['distance'].rdiv(1)) +\
                                     (emp_multiplier * (emp_assign['emp_avail'] /
                                                        emp_assign['avg_emp']))
                emp_assign.loc[emp_assign.emp_avail < 1, 'prob'] = 0
                dist_threshold = 25
                check_size = 0L
                while check_size == 0:
                    check_size = emp_assign[(emp_assign.distance <= dist_threshold) &
                                            (emp_assign.prob > 0)].shape[0]
                    if check_size == 0:
                        dist_threshold = dist_threshold + 25
                # print 'Distance threshold used: %d' % dist_threshold
                emp_assign.loc[emp_assign.distance > dist_threshold, 'prob'] = 0
                emp_assign = emp_assign[emp_assign.prob > 0].copy()
                emp_assign = emp_assign.sort_values('prob', ascending=False)
                emp_assign['prob'] = emp_assign.groupby('oTAZ')['prob']. \
                    transform(lambda value: value / value.sum())
                taz_select = emp_assign.groupby(['oTAZ', 'to_move']). \
                    apply(lambda grp: prng.choice(grp.dTAZ, grp.to_move.unique().astype(int),
                                                  replace=True,
                                                  p=grp.prob))
                taz_select = taz_select.apply(pd.Series).stack().to_frame('dTAZ').astype(int)
                taz_select = taz_select.reset_index(level=2, drop=True).groupby('dTAZ'). \
                    apply(lambda df: prng.choice(df.index.values,
                                                 1L,
                                                 replace=False,
                                                 p=(df.index.get_level_values('to_move').values /
                                                    df.index.get_level_values('to_move').
                                                    values.sum()))[0]). \
                    apply(pd.Series). \
                    rename(columns={0: 'oTAZ', 1: 'to_move'}).reset_index(). \
                    astype(int)
                bus_select = pd.merge(taz_select, firms_sub.reset_index()[['bus_id', 'TAZ', 'emp']],
                                      how='inner',
                                      left_on='dTAZ',
                                      right_on='TAZ'
                                      )
                bus_select = bus_select.groupby('oTAZ').apply(
                    lambda df: prng.choice(df.bus_id.values, min(df.to_move.unique().astype(int),
                                                                 df.shape[0] - 1L),
                                           replace=False, p=df.emp / df.emp.sum()))
                bus_select = bus_select.apply(pd.Series).stack().to_frame('bus_id').astype(int)
                bus_select = bus_select.reset_index(level=1, drop=True)
                est_moved = pd.merge(bus_select, taz_select,
                                     how='inner', left_index=True, right_on='oTAZ')
                firms.loc[est_moved.bus_id.values, 'TAZ'] = est_moved['oTAZ'].astype(int).values
        employment = firms.groupby(['TAZ', 'model_emp_cat'])['emp'].sum()
        employment = employment.to_frame(name='emp_CBP').reset_index()
        # Melt socio_economics_taz employment data by TAZ and employment category
        employment_SE = socio_economics_taz.reset_index(). \
            melt(id_vars='TAZ', var_name='model_emp_cat', value_name='emp_SE')
        # inner join
        employment = employment.merge(right=employment_SE, how='outer', on=['TAZ', 'model_emp_cat'])

        # FIXME should we leave this nan to distinguish zero counts from missing data?
        employment.emp_SE.fillna(0, inplace=True)
        employment.emp_CBP.fillna(0, inplace=True)

        # Re-allocation of TAZ to establishments for better scaling
        employment = pd.merge(employment,
                              firms.groupby(['TAZ', 'model_emp_cat']).size().
                              to_frame('n_est').reset_index(),
                              on=['TAZ', 'model_emp_cat'],
                              how='outer').fillna(0)

        # Identify TAZ and model_emp_cat that needs to be reallocated somewhere else
        employment = employment[~((employment.emp_SE == 0) & (employment.emp_CBP == 0))].copy()
        employment['to_assign'] = employment['emp_CBP'] < 1
        employment['to_distribute'] = (~(employment['to_assign']) & ((employment['n_est']) > 1))
        employment['avg_emp'] = reindex(firms.groupby('model_emp_cat')['emp'].median(),
                                        employment.model_emp_cat)
        employment['to_move'] = (employment['emp_SE'] / employment['avg_emp']).apply(np.ceil)
        employment.loc[employment['emp_CBP'] > 0, 'to_move'] = 0.0

        # drop rows where both employment sources say there should be no employment in empcat
        rem_assign = employment[~employment.model_emp_cat.isin(emp_cats_not_in_firms)].\
            to_assign.sum()
        # print 'Establishments remaining to be moved :%d\n' % rem_distribute

    logger.info("%d firms relocated." % (firms[firms.orig_TAZ != firms.TAZ].shape[0]))

    t0 = print_elapsed_time("firm_sim_scale_employees relocating to zero CBP \nzero+ SE NUMA",
                            t0,
                            debug=True)

    # Identify TAZ and model_emp_cat that needs to be reallocated somewhere else
    emp_to_check = 'emp_SE'
    employment['to_distribute'] = employment['n_est'] > employment[emp_to_check]
    employment['to_assign'] = ~employment['to_distribute']
    employment['avg_emp'] = reindex(firms.groupby('model_emp_cat')['emp'].median(),
                                    employment.model_emp_cat)
    employment['to_move'] = employment['n_est'] - employment[emp_to_check]
    employment.loc[employment.to_move < 0.0, 'to_move'] = 0.0

    rem_distribute = employment.to_distribute.sum()
    while rem_distribute > 0:
        for model_emp_cat in employment[employment.to_distribute].model_emp_cat.unique():
            # print 'Adjusting employment category: %s\n' % model_emp_cat
            firms_sub = firms[firms.model_emp_cat == model_emp_cat].copy()
            emp_taz = employment[employment.model_emp_cat == model_emp_cat]
            firms_sub['to_move'] = reindex(emp_taz.set_index('TAZ')['to_move'], firms_sub.TAZ)
            emp_assign = emp_taz[emp_taz.to_assign]
            emp_distribute = emp_taz[emp_taz.to_distribute]
            if emp_distribute.shape[0] > 0:
                emp_distribute = pd.merge(numa_dist, emp_distribute[['TAZ', 'to_move', 'avg_emp']],
                                          how='inner',
                                          left_on='oTAZ',
                                          right_on='TAZ')
                emp_distribute['emp_reqd'] = reindex(
                    emp_assign.set_index('TAZ').apply(lambda df: df.emp_SE - df.emp_CBP,
                                                      axis=1),
                    emp_distribute.dTAZ).fillna(0)
                emp_distribute.loc[emp_distribute.emp_reqd < 0, 'emp_reqd'] = 0
                dist_multiplier = 1000.0
                emp_multiplier = 0.01
                emp_distribute['prob'] = (dist_multiplier * emp_distribute['distance'].rdiv(1)) +\
                                         (emp_multiplier * (emp_distribute['emp_reqd'] /
                                                            emp_distribute['avg_emp']))
                emp_distribute.loc[emp_distribute.emp_reqd == 0, 'prob'] = 0
                dist_threshold = 25
                check_size = 0L
                while check_size == 0:
                    check_size = emp_distribute[(emp_distribute.distance <= dist_threshold) &
                                                (emp_distribute.prob > 0)].shape[0]
                    if check_size == 0:
                        dist_threshold = dist_threshold + 25
                # print 'Distance threshold used: %d' % dist_threshold
                emp_distribute.loc[emp_distribute.distance > dist_threshold, 'prob'] = 0
                emp_distribute = emp_distribute[emp_distribute.prob > 0].copy()
                emp_distribute = emp_distribute.sort_values('prob', ascending=False)
                emp_distribute['prob'] = emp_distribute.groupby('oTAZ')['prob']. \
                    transform(lambda value: value / value.sum())
                taz_select = emp_distribute.groupby('oTAZ').apply(lambda grp:
                                                                  prng.choice(grp.dTAZ,
                                                                              grp.to_move.unique().
                                                                              astype(int),
                                                                              replace=True,
                                                                              p=grp.prob))
                taz_select = taz_select.apply(pd.Series).stack().to_frame('dTAZ').astype(int)
                bus_select = firms_sub[firms_sub.TAZ.isin(taz_select.index.get_level_values('oTAZ').
                                                          unique().tolist())].\
                    groupby('TAZ').\
                    apply(lambda grp: prng.choice(grp.index, grp.to_move.unique().astype(int),
                                                  replace=False,
                                                  p=(grp.emp / grp.emp.sum())))
                bus_select = bus_select.apply(pd.Series).stack().to_frame('bus_id').\
                    astype(int)
                taz_select.index.names = ['TAZ', None]
                est_moved = pd.merge(bus_select, taz_select, how='inner', left_index=True,
                                     right_index=True)
                firms.loc[est_moved.bus_id.values, 'TAZ'] = est_moved['dTAZ'].astype(int).values
        employment = firms.groupby(['TAZ', 'model_emp_cat'])['emp'].sum()
        employment = employment.to_frame(name='emp_CBP').reset_index()
        employment_SE = socio_economics_taz.reset_index(). \
            melt(id_vars='TAZ', var_name='model_emp_cat', value_name='emp_SE')
        # inner join
        employment = employment.merge(right=employment_SE, how='outer', on=['TAZ', 'model_emp_cat'])

        # FIXME should we leave this nan to distinguish zero counts from missing data?
        employment.emp_SE.fillna(0, inplace=True)
        employment.emp_CBP.fillna(0, inplace=True)

        # Re-allocation of TAZ to establishments for better scaling
        employment = pd.merge(employment,
                              firms.groupby(['TAZ', 'model_emp_cat']).size().
                              to_frame('n_est').reset_index(),
                              on=['TAZ', 'model_emp_cat'],
                              how='outer').fillna(0)

        # Identify TAZ and model_emp_cat that needs to be reallocated somewhere else
        employment['to_distribute'] = employment['n_est'] > employment[emp_to_check]
        employment['to_assign'] = ~employment['to_distribute']
        employment['avg_emp'] = reindex(firms.groupby('model_emp_cat')['emp'].median(),
                                        employment.model_emp_cat)
        employment['to_move'] = employment['n_est'] - employment[emp_to_check]
        employment.loc[employment.to_move < 0.0, 'to_move'] = 0.0

        # drop rows where both employment sources say there should be no employment in empcat
        employment = employment[~((employment.emp_SE == 0) & (employment.emp_CBP == 0))]
        rem_distribute = employment.to_distribute.sum()
        # print 'Establishments remaining to be moved :%d\n' % rem_distribute

    logger.info("%d firms relocated." % (firms[firms.orig_TAZ != firms.TAZ].shape[0]))
    t0 = print_elapsed_time("firm_sim_scale_employees relocating establishments for better scaling",
                            t0,
                            debug=True)

    # firms.to_csv('firms_moved2.csv')
    # Three cases to deal with:
    # 1. Employment change where there is some CBP employment and some SE employment
    # 2. Employment in SE data but none from CBP
    # 3. Employment from CBP but none in SE data
    #
    # Note: if difference is 0, no action required --> Adjustment factor will be 1:

    # - Case 1 and 3: scale the number of employees without creating any firms

    firms = scale_cbp_to_se(employment, firms)

    # - drop firms with no employees
    # FIXME all international (TAZ=0) firms have 0 employees?
    if (firms.emp == 0).any():
        n0 = firms.shape[0]
        firms = firms[firms.emp > 0]
        n1 = firms.shape[0]
        logger.info('Dropped %s out of %s no-emp firms' % (n0 - n1, n0))

    # - Case 2: create new n new firms where n is employees_needed / avg_number_employees_per_firm

    # - Add firms to empty TAZ-Employment category combinations to deal with Case 2
    # For each combination:
    # 1. Select a number of firms max of EMPDIFF/average emp from all firms in that Model_EmpCat
    # 2. Add them to the firms table
    # 3. recalc the EMPADJ to refine the employment to match exactly the SE data employment

    # firms_needed = employment[employment.emp_CBP < 1].copy()
    #
    # # - Calculate average employment by Model_EmpCat
    #
    # empcat_med_emp = firms.groupby('model_emp_cat')['emp'].median()
    # firms_needed['avg_emp'] = reindex(empcat_med_emp, firms_needed.model_emp_cat)
    #
    # # Employment Category Absent from CBP
    # emp_cats_not_in_firms = firms_needed.model_emp_cat[firms_needed.avg_emp.isnull()].unique()
    # if emp_cats_not_in_firms:
    #     logger.warn("%s emp_cats_not_in_firms: %s" %
    #                 (len(emp_cats_not_in_firms), emp_cats_not_in_firms))
    #
    # DEFAULT_AVG_EMP = 10  # FIXME magic constant
    # firms_needed.avg_emp.fillna(DEFAULT_AVG_EMP, inplace=True)
    #
    # # - number of firms to be sampled is employees_needed / avg_number_employees_per_firm
    # assert (firms_needed.emp_CBP == 0).all()
    # firms_needed['n'] = \
    #     (firms_needed.emp_SE / firms_needed.avg_emp).clip(lower=1).round().astype(int)
    # prng = pipeline.get_rn_generator().get_global_rng()
    # new_firms = []
    # for emp_cat, emp_cat_firms_needed in firms_needed.groupby('model_emp_cat'):
    #
    #     n = emp_cat_firms_needed.n.sum()
    #
    #     # if emp_cat not in firms.model_emp_cat.values:
    #     if emp_cat in emp_cats_not_in_firms:
    #         logger.warn('Skipping model_emp_cat %s not found in firms' % emp_cat)
    #         continue
    #
    #     new_firm_ids = prng.choice(
    #         a=firms[firms.model_emp_cat == emp_cat].index.values,
    #         size=n,
    #         replace=True)
    #
    #     df = pd.DataFrame({
    #         'old_bus_id': new_firm_ids,
    #         'new_TAZ': np.repeat(emp_cat_firms_needed.TAZ, emp_cat_firms_needed.n)
    #     })
    #
    #     new_firms.append(df)
    #
    # new_firms = pd.concat(new_firms)
    #
    # # Look up the firm attributes for these new firms (from the ones they were created from)
    # new_firms = pd.merge(left=new_firms, right=firms, left_on='old_bus_id', right_index=True)
    #
    # del new_firms['old_bus_id']
    # del new_firms['TAZ']
    # new_firms.rename(columns={'new_TAZ': 'TAZ'}, inplace=True)
    #
    # # - Update the County and State FIPS and FAF zones
    # new_firms.state_FIPS = reindex(taz_fips.state_FIPS, new_firms.TAZ)
    # new_firms.county_FIPS = reindex(taz_fips.county_FIPS, new_firms.TAZ)
    # new_firms.FAF4 = reindex(taz_faf4.FAF4, new_firms.TAZ)
    #
    # # - Give the new firms new, unique business IDs
    # new_firms.reset_index(drop=True, inplace=True)
    # new_firms.index = new_firms.index + MAX_BUS_ID + 1
    # new_firms.index.name = firms.index.name
    #
    # # - scale/bucket round to ensure that employee counts of the new firms matche the SE data
    # # Summarize employment of new firms by TAZ and Model_EmpCat
    # employment_new = new_firms.groupby(['TAZ', 'model_emp_cat'])['emp'].sum()
    # employment_new = employment_new.to_frame(name='emp_CBP').reset_index()
    # # Melt socio_economics_taz employment data by TAZ and employment category
    # employment_SE = socio_economics_taz.reset_index(). \
    #     melt(id_vars='TAZ', var_name='model_emp_cat', value_name='emp_SE')
    # # left join since we only care about new firms
    # employment_new = \
    #     employment_new.merge(right=employment_SE, how='left', on=['TAZ', 'model_emp_cat'])
    #
    # assert not employment_new.emp_SE.isnull().any()
    # assert not employment_new.emp_CBP.isnull().any()
    # assert not (employment_new.emp_CBP == 0).any()
    #
    # new_firms = scale_cbp_to_se(employment_new, new_firms)
    # # New firms created can have zero employment after bucketround
    # if (new_firms.emp < 1).any():
    #     n0 = new_firms.shape[0]
    #     new_firms = new_firms[new_firms.emp > 0]
    #     n1 = new_firms.shape[0]
    #     logger.info('Dropped %s out of %s no-emp new firms' % (n0 - n1, n0))
    #
    # for emp_cat in firms_needed.model_emp_cat.unique():
    #     n_needed = firms_needed[firms_needed.model_emp_cat == emp_cat].n.sum()
    #     n_created = (new_firms.model_emp_cat == emp_cat).sum()
    #     if n_needed != n_created:
    #         logger.warn("model_emp_cat %s needed %s, created %s" % (emp_cat, n_needed, n_created))
    # logger.info("firm_sim_scale_employees %s original firms %s new firms" %
    #             (firms.shape[0], new_firms.shape[0]))
    #
    # # Combine the original firms and the new firms
    # firms = pd.concat([firms, new_firms]).sort_index()
    assert firms.index.is_unique  # index (bus_id) should be unique

    assert not (firms.emp < 1).any()

    # - error check: firms employmeent should match emp_SE for all taz and emp_cats
    t0 = print_elapsed_time()
    summary = pd.merge(
        left=firms.groupby(['TAZ', 'model_emp_cat'])['emp'].sum().to_frame('emp_firms'),
        right=employment.set_index(['TAZ', 'model_emp_cat']),
        left_index=True, right_index=True, how='outer'
    ).reset_index()
    # ignore emp_cats not present in firms
    summary = summary[~summary.model_emp_cat.isin(emp_cats_not_in_firms)]
    summary.emp_firms.fillna(0, inplace=True)
    assert np.abs(summary.emp_firms.sum() - summary.emp_SE.sum()) <= 5
    t0 = print_elapsed_time("error check firm_sim_scale_employees results", t0, debug=True)

    # Reset the indices (bus_id) of foreign firms
    MAX_BUS_ID = firms.index.max()
    logger.info("assigning foreign firm indexes starting above MAX_BUS_ID %s" % (MAX_BUS_ID,))
    firms_foreign.reset_index(drop=True, inplace=True)
    firms_foreign.index = firms_foreign.index + MAX_BUS_ID + 1
    firms_foreign.index.name = firms.index.name

    # Combine the new firms with foreign firms
    firms = pd.concat([firms, firms_foreign], sort=True).sort_index()
    assert firms.index.is_unique  # index (bus_id) should be unique

    # Recode employee counts into categories
    firms.loc[firms.FAF4 > 800, 'emp'] = \
        firm_emp_generator(employment_categories.loc[employment_categories.index.max(),
                                                     'low_threshold'],
                           employment_categories.loc[employment_categories.index.max(),
                                                     'emp_range'],
                           hyman_interpol,
                           (firms.FAF4 > 800).sum()).astype(int)
    firms['esizecat'] = \
        pd.cut(x=firms.emp,
               bins=np.append(employment_categories.low_threshold.values, np.inf),
               labels=employment_categories.id,
               include_lowest=True).astype(int)
    assert not firms.esizecat.isnull().any()

    return firms


def firm_sim_assign_SCTG(
        firms,
        NAICS2007io_to_SCTG):
    """
    Simulate Production Commodities, choose a single commodity for firms making more than one
    """

    # Look up all the SCTGs (Standard Classification of Transported Goods) each firm can produce
    # Note: not every firm produces a transportable SCTG commodity
    # Some firms can potentially make more than one SCTG, simulate exactly one

    t0 = print_elapsed_time()

    # Separate out the foreign firms
    firms_foreign = firms[firms.FAF4 > 800]
    firms = firms[firms.FAF4 < 800].copy()

    # Merge in the single-SCTG naics
    firms['SCTG'] = reindex(
        NAICS2007io_to_SCTG[NAICS2007io_to_SCTG.proportion == 1].set_index('NAICSio').SCTG,
        firms.NAICS6_make
    )

    t0 = print_elapsed_time("Merge in the single-SCTG naics", t0, debug=True)

    # random select SCTG for multi-SCTG naics based on proportions (probabilities)
    multi_NAICS2007io_to_SCTG = NAICS2007io_to_SCTG[NAICS2007io_to_SCTG.proportion < 1]
    multi_sctg_naics_codes = multi_NAICS2007io_to_SCTG.NAICSio.unique()
    multi_sctg_firms = firms[firms.NAICS6_make.isin(multi_sctg_naics_codes)]

    # for each distinct multi-SCTG naics
    prng = pipeline.get_rn_generator().get_global_rng()
    for naics, naics_firms in multi_sctg_firms.groupby('NAICS6_make'):
        # slice the NAICS2007io_to_SCTG rows for this naics
        naics_sctgs = multi_NAICS2007io_to_SCTG[multi_NAICS2007io_to_SCTG.NAICSio == naics]

        # choose a random SCTG code for each business with this naics code
        sctgs = prng.choice(naics_sctgs.SCTG.values,
                            size=len(naics_firms),
                            p=naics_sctgs.proportion.values,
                            replace=True)

        firms.loc[naics_firms.index, 'SCTG'] = sctgs

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

    # Identify firms who make 2+ commodities (especially wholesalers) and simulate a specific
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
        assign_keymap(keymap, df=firms)

    # all n2=43 firms get NAICS6_Make padded n4 (e.g. 4248 gets NAICS6_Make '424800')
    firms.NAICS6_make = firms.NAICS6_make.mask(firms.n2 == 42, (firms.n4 * 100).astype(str))

    # Combine the new firms with foreign firms
    firms = pd.concat([firms, firms_foreign]).sort_index()

    assert firms.index.is_unique
    return firms


def firm_sim_types(firms):
    """
    Identify special firms: warehouses, producers, and makers subsample
    """

    assert firms.index.is_unique

    # Separate out the foreign firms
    firms_foreign = firms[firms.FAF4 > 800]
    firms = firms[firms.FAF4 < 800].copy()

    # Warehouses
    # NAICS 481 air, 482 rail, 483 water, 493 warehouse and storage
    firms['warehouse'] = \
        firms.TAZ.isin(base_variables.BASE_TAZ1_HALO_STATES) & \
        (firms.n3.isin(base_variables.NAICS3_WAREHOUSE))

    # Producers and Makers
    # Create a flag to make sure at least one firm is used
    # as a maker for each zone and commodity combination
    producers = firms[~firms.SCTG.isnull()].copy()

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
    # firms['producer'] = producers.identity

    # Add maker and producer flags back to list of firms
    firms['producer'] = False
    firms.loc[producers.index, 'producer'] = True

    firms['maker'] = False
    firms.loc[makers.index, 'maker'] = True

    # Combine the new firms with foreign firms
    firms = pd.concat([firms, firms_foreign]).sort_index()

    return firms[['state_FIPS', 'county_FIPS', 'FAF4', 'TAZ', 'SCTG', 'NAICS2012', 'NAICS2007',
                  'NAICS6_make', 'industry10', 'industry5', 'model_emp_cat', 'esizecat', 'emp',
                  'warehouse', 'producer', 'maker', 'prod_val']].copy()


def firm_sim_producers(firms, io_values, unitcost):
    """
    Create Producers

    Creates a dataframe of producer firms in which each producer firms appears once
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
    producers = firms[firms.producer]
    domesitc_producer_idx = producers.FAF4 < 800

    # we only want i/o pairs that use domesitc producers commodity as an input
    # FIXME how should we handle foreign production
    # FIXME there are i/o codes in foreign producers that do not exist in
    #  domestic producers
    io_values = io_values[(io_values.NAICS6_make.isin(producers[domesitc_producer_idx].NAICS6_make.
                                                      unique()))]

    # - For each output, select only the most important input commodities

    # FIXME sort and goupby need to be in synch - but which is it????
    # - setkey(InputOutputValues, NAICS6_Make, ProVal)
    io_values = io_values.sort_values(by=['NAICS6_use', 'pro_val'])

    # Calculate cumulative pct value of the consumption inputs grouped by output commodity
    # - InputOutputValues[, CumPctProVal := cumsum(ProVal)/sum(ProVal), by = NAICS6_Use]
    io_values['cum_pro_val'] = io_values.groupby('NAICS6_use')['pro_val'].transform('cumsum')
    io_values['cum_pct_pro_val'] = \
        io_values['cum_pro_val'] / io_values.groupby('NAICS6_use')['cum_pro_val'].transform('last')

    # select NAICS6_make input commodities with highest pro_val for each NAICS6_use output
    io_values = io_values[io_values.cum_pct_pro_val > (1 - base_variables.BASE_PROVALTHRESHOLD)]

    # FIXME - use factors?
    # InputOutputValues[,NAICS6_Make:=factor(NAICS6_Make,levels = levels(Firms$NAICS6_Make))]
    # InputOutputValues[,NAICS6_Use:=factor(NAICS6_Use,levels = levels(Firms$NAICS6_Make))]

    # - total number of domestic employees manufacturing NAICS6_make commodity
    producer_emp_counts_by_naics = \
        producers[(~producers.state_FIPS.isnull()) & domesitc_producer_idx][['NAICS6_make',
                                                                             'emp']].\
        groupby('NAICS6_make')['emp'].sum()

    # - annotate input_output_values with total emp count for producers of NAICS6_input
    io_values['emp'] = \
        reindex(producer_emp_counts_by_naics, io_values.NAICS6_make)

    # FIXME why are we summing emp again?
    # InputOutputValues <- InputOutputValues[,.(ProVal=sum(ProVal), Emp = sum(Emp)), .(NAICS6_Make)]
    io_values = io_values[['NAICS6_make', 'pro_val', 'emp']].groupby('NAICS6_make').sum()

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

    return producers


def firm_sim_consumers(firms, io_values, NAICS2007io_to_SCTG, unitcost, firm_pref_weights):
    """
    Create Consumers

    produces a dataframe of consumer firms in which each consumer firm

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

    # Separate foreign firms
    firms_foreign = firms[firms.FAF4 > 800]
    firms = firms[firms.FAF4 < 800]

    # Identify consumer firms
    cols = ['TAZ', 'county_FIPS', 'state_FIPS', 'FAF4', 'NAICS6_make', 'esizecat', 'emp']
    consumers = firms[cols].copy()
    consumers.reset_index(inplace=True)  # bus_id index as explicit column

    t0 = print_elapsed_time("firm_sim_consumers cols", t0, debug=True)

    # Create a flag to make sure at least one firm is sampled
    # for each zone and PRODUCED commodity combination
    prng = pipeline.get_rn_generator().get_global_rng()
    g = ['TAZ', 'FAF4', 'NAICS6_make', 'esizecat']
    keeper_ids = \
        consumers[g].groupby(g).agg(lambda x: prng.choice(x.index, size=1)[0]).values
    t0 = print_elapsed_time("keeper_ids", t0, debug=True)

    consumers['must_keep'] = False
    consumers.loc[keeper_ids, 'must_keep'] = True

    t0 = print_elapsed_time("firm_sim_consumers must_keep", t0, debug=True)

    # Create a sample of consumer firms based on the following rules to form
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

    t0 = print_elapsed_time("firm_sim_consumers slice", t0, debug=True)

    # For each consuming firm generate a list of input commodities that need to be purchased
    # Filter to just transported commodities
    # FIXME filters so io_values only contains producer NAICS6_make
    # FIXME but nothing guarantees that the io_values NAICS6_use have any domestic producers
    producer_naics = firms.NAICS6_make[firms.producer].unique()
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

    # FIXME - use factors?
    # InputOutputValues[,NAICS6_Make:=factor(NAICS6_Make,levels = levels(Firms$NAICS6_Make))]
    # InputOutputValues[,NAICS6_Use:=factor(NAICS6_Use,levels = levels(Firms$NAICS6_Make))]

    # - Calcuate value per employee required (US domestic employment) for each NAICS6_make code
    # FIXME - use FAF4 filters to filter domestic firms
    domestic_emp_counts_by_naics = \
        firms[(~firms.state_FIPS.isnull())][['NAICS6_make', 'emp']].\
        groupby('NAICS6_make')['emp'].sum()

    # - drop io_values rows where there are no domestic producers of the output commodity
    io_values = io_values[io_values.NAICS6_use.isin(domestic_emp_counts_by_naics.index)]

    # - annotate input_output_values with total emp count for producers of NAICS_output
    io_values['emp'] = reindex(domestic_emp_counts_by_naics, io_values.NAICS6_use)
    io_values['val_emp'] = io_values.pro_val / io_values.emp

    t0 = print_elapsed_time("firm_sim_consumers io_values", t0, debug=True)

    # there are firms with no inputs (e.g. wholesalers)
    # but we might want to keep an eye on them, as they will get dropped in merge
    # output_NAICS = io_values.NAICS6_use.unique()
    # firms_NAICS6_make = firms.NAICS6_make.unique()
    # naics_without_inputs = list(set(firms_NAICS6_make) - set(output_NAICS))
    # logger.info("%s NAICS_make without input commodities %s" %
    #             (len(naics_without_inputs), naics_without_inputs))
    # t0 = print_elapsed_time("naics_without_inputs", t0, debug=True)

    # - inner join sample_firms with the top inputs required to produce firm's product
    # pairs is a list of firms with one row for every major commodity the firm consumes
    # pairs.NAICS6_use is the commodity the firm PRODUCES
    # pairs.NAICS6_make is a commodity that the firm CONSUMES
    consumers = pd.merge(left=consumers.rename(columns={'NAICS6_make': 'NAICS6_use'}),
                         right=io_values[['NAICS6_use', 'NAICS6_make', 'val_emp']],
                         left_on='NAICS6_use', right_on='NAICS6_use', how='inner')

    assert not consumers.val_emp.isnull().any()

    t0 = print_elapsed_time("firm_sim_consumers pairs", t0, debug=True)

    # Look up all the SCTGs (Standard Classification of Transported Goods) each firm can produce
    # Note: not every firm produces a transportable SCTG commodity
    # Some firms can potentially make more than one SCTG, simulate exactly one
    #

    is_multi = (NAICS2007io_to_SCTG.proportion < 1)
    multi_NAICS2007io_to_SCTG = NAICS2007io_to_SCTG[is_multi]
    consumers_NAICS6_make = consumers.NAICS6_make.to_frame(name='NAICS6_make')
    consumer_is_multi = consumers.NAICS6_make.isin(multi_NAICS2007io_to_SCTG.NAICSio.unique())
    t0 = print_elapsed_time("firm_sim_consumers pair_is_multi flag", t0, debug=True)

    # - single-SCTG naics
    singletons = pd.merge(
        consumers_NAICS6_make[~consumer_is_multi],
        NAICS2007io_to_SCTG[~is_multi][['NAICSio', 'SCTG']].set_index('NAICSio'),
        left_on="NAICS6_make",
        right_index=True,
        how="left").SCTG
    t0 = print_elapsed_time("firm_sim_consumers single-SCTG naics", t0, debug=True)

    # - random select SCTG for multi-SCTG naics based on proportions (probabilities)

    # for each distinct multi-SCTG naics
    multi_sctg_pairs = consumers_NAICS6_make[consumer_is_multi]
    sctgs = []
    bus_ids = []
    for naics, naics_firms in multi_sctg_pairs.groupby('NAICS6_make'):
        # slice the NAICS2007io_to_SCTG rows for this naics
        naics_sctgs = multi_NAICS2007io_to_SCTG[multi_NAICS2007io_to_SCTG.NAICSio == naics]

        # choose a random SCTG code for each business with this naics code
        sctgs.append(prng.choice(naics_sctgs.SCTG.values,
                                 size=len(naics_firms),
                                 p=naics_sctgs.proportion.values,
                                 replace=True))

        bus_ids.append(naics_firms.index.values)

    t0 = print_elapsed_time("firm_sim_consumers choose SCTG for multi-SCTG naics", t0, debug=True)

    # singletons
    sctgs.append(singletons.values)
    bus_ids.append(singletons.index.values)
    sctgs = list(itertools.chain.from_iterable(sctgs))
    bus_ids = list(itertools.chain.from_iterable(bus_ids))
    t0 = print_elapsed_time("firm_sim_consumers itertools.chain", t0, debug=True)

    # sctgs = pd.Series(sctgs, index=bus_ids)
    # t0 = print_elapsed_time("firm_sim_consumers SCTG series", t0, debug=True)

    consumers['SCTG'] = pd.Series(sctgs, index=bus_ids)
    t0 = print_elapsed_time("firm_sim_consumers pairs['SCTG']", t0, debug=True)

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
    consumers.loc[i[i].index, 'NAICS6_make'] = sctg_whl[consumers.loc[i[i].index, 'SCTG']].\
        astype(str)

    # Pairs[NAICS6_Use2 != "42" & temprand < 0.15 & SCTG %in% c(35, 38), NAICS6_Make := "423600"]
    i = (pairs_NAICS6_use2 != '42') & (consumers.SCTG < 41) & (temp_rand < 0.15)
    consumers.loc[i[i].index, 'NAICS6_make'] = '423600'

    # Pairs[NAICS6_Use2 != "42" & temprand < 0.15 & SCTG == 40,          NAICS6_Make := "424900"]
    i = (pairs_NAICS6_use2 != '42') & (consumers.SCTG == 40) & (temp_rand < 0.15)
    consumers.loc[i[i].index, 'NAICS6_make'] = '424900'

    t0 = print_elapsed_time("firm_sim_consumers tweaks", t0, debug=True)

    # - conflate duplicates
    # Accounting for multiple SCTGs from one NAICS and for wholesalers means that certain
    # users now have multiple identical inputs on a NAICS6_Make - SCTG basis
    # aggregate (summing over ValEmp) so that NAICS6_MAke - SCTG is unique for each user

    by_cols = ['bus_id', 'NAICS6_make', 'SCTG', 'NAICS6_use', 'TAZ', 'FAF4', 'esizecat']
    consumers = consumers.groupby(by_cols)['val_emp'].sum().to_frame(name='val_emp').reset_index()

    t0 = print_elapsed_time("firm_sim_consumers group and sum", t0, debug=True)

    consumers = consumers.copy()

    consumers['emp'] = reindex(firms.emp, consumers.bus_id)
    consumers['con_val'] = consumers.emp * 1E6 * consumers.val_emp
    consumers['unit_cost'] = reindex(unitcost.unit_cost, consumers.SCTG)
    consumers['purchase_amount_tons'] = consumers.con_val / consumers.unit_cost

    del consumers['val_emp']
    del firms

    # - Foreign consumption
    io_values['pct_pro_val'] = io_values.groupby('NAICS6_make')['pro_val']. \
        transform(lambda value: value / value.sum())
    consumers_foreign = pd.merge(left=firms_foreign[~firms_foreign.producer.astype(bool)].
                                 reset_index(),
                                 right=io_values[['NAICS6_use', 'NAICS6_make', 'pct_pro_val']],
                                 on='NAICS6_make',
                                 how='inner')
    consumers_foreign.pct_pro_val.fillna(0, inplace=True)
    consumers_foreign['con_val'] = consumers_foreign.prod_val * 1E6 * consumers_foreign.pct_pro_val
    consumers_foreign['unit_cost'] = reindex(unitcost.unit_cost, consumers_foreign.SCTG)
    consumers_foreign['purchase_amount_tons'] = consumers_foreign.con_val / (consumers_foreign.
                                                                             unit_cost)
    consumers_foreign = consumers_foreign[consumers.columns].copy()
    del firms_foreign

    consumers = pd.concat([consumers, consumers_foreign], ignore_index=True)

    consumers.NAICS6_make = consumers.NAICS6_make.astype('category')
    consumers.NAICS6_use = consumers.NAICS6_use.astype('category')

    consumers['cost_weight'] = reindex(firm_pref_weights.cost_weight, consumers.SCTG)
    consumers['time_weight'] = reindex(firm_pref_weights.time_weight, consumers.SCTG)
    consumers['single_source_max_fraction'] = reindex(firm_pref_weights.single_source_max_fraction,
                                                      consumers.SCTG)

    consumers.NAICS6_make = consumers.NAICS6_make.astype(str)
    consumers.NAICS6_use = consumers.NAICS6_use.astype(str)

    col_map = {'bus_id': 'buyer_id',
               'NAICS6_make': 'input_commodity',
               'NAICS6_use': 'NAICS',
               'emp': 'size',
               'unit_cost': 'non_transport_unit_cost',
               'cost_weight': 'pref_weight_1_unit_cost',
               'time_weight': 'pref_weight_2_ship_time'
               }
    consumers.rename(columns=col_map, inplace=True)

    # FIXME is this supposed to duplicate or rename?
    # consumers['input_commodity'] = consumers['NAICS']

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
        while (nconst * SUPPLIERS_PER_BUYER > COMBINATION_THRESHOLD):
            n_groups += 1
            nconst = math.ceil(nconsumers / n_groups)
            nprodt = math.ceil(nproducers / n_groups)
    else:
        while (nconst * SUPPLIERS_PER_BUYER > COMBINATION_THRESHOLD or
               nconst / nprodt > CONS_PROD_RATIO_LIMIT):
            n_groups += 1
            nconst = math.ceil(nconsumers / n_groups)

    return (nprodt, nconst, split_prod, n_groups)


def firm_sim_naics_set(producers, consumers):
    """
    output summaries

    """

    # - matching consumers and suppliers -- by NAICS codes
    producers_summary_naics = \
        producers[['output_commodity', 'size', 'output_capacity_tons']].\
        groupby('output_commodity').\
        agg({'size': ['count', 'sum'], 'output_capacity_tons': 'sum'})
    producers_summary_naics.columns = producers_summary_naics.columns.map('_'.join)
    producers_summary_naics.reset_index(inplace=True)  # explicit output_commodity column
    producers_summary_naics.rename(columns={'size_count': 'producers',
                                            'size_sum': 'employment',
                                            'output_capacity_tons_sum': 'output_capacity',
                                            'output_commodity': 'NAICS'},
                                   inplace=True)

    consumers_summary_naics = \
        consumers[['input_commodity', 'size', 'purchase_amount_tons']].\
        groupby('input_commodity').\
        agg({'size': ['count', 'sum'], 'purchase_amount_tons': 'sum'})
    consumers_summary_naics.columns = consumers_summary_naics.columns.map('_'.join)
    consumers_summary_naics.reset_index(inplace=True)  # explicit input_commodity column
    consumers_summary_naics.rename(columns={'size_count': 'consumers',
                                            'size_sum': 'employment',
                                            'purchase_amount_tons_sum': 'input_requirements',
                                            'input_commodity': 'NAICS'},
                                   inplace=True)

    match_summary_naics = pd.merge(
        left=producers_summary_naics[['NAICS', 'producers', 'output_capacity']],
        right=consumers_summary_naics[['NAICS', 'consumers', 'input_requirements']],
        left_on='NAICS',
        right_on='NAICS',
        how='outer'
    )

    match_summary_naics = match_summary_naics[["NAICS", "producers", "consumers",
                                               "output_capacity", "input_requirements"]]

    match_summary_naics['ratio_output'] = \
        match_summary_naics.output_capacity / match_summary_naics.input_requirements
    match_summary_naics['possible_matches'] = \
        match_summary_naics.producers * match_summary_naics.consumers

    # - Get number of (none NA) matches by NAICS, and check for imbalance in producers and consumers

    naics_set = match_summary_naics[~match_summary_naics.possible_matches.isnull()]
    naics_set = naics_set[["NAICS", "producers", "consumers", "possible_matches"]]

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

    return match_summary_naics, naics_set


def firm_sim_summary(output_dir, producers, consumers, firms, firm_pref_weights):
    """
    summarize CBP, Producers and Consumers
    """

    firms_sum = collections.OrderedDict()

    firms_sum['firms'] = firms.shape[0]
    firms_sum['firms.producer'] = firms.producer.sum()
    firms_sum['firms.maker'] = firms.producer.sum()
    firms_sum['employment'] = firms.emp.sum()

    # - firms_emp_by_sctg
    firms_emp_by_sctg = firms[['SCTG', 'emp']].groupby('SCTG').emp. \
        agg(['sum', 'count']). \
        rename(columns={'sum': 'employment', 'count': 'establishments'}). \
        reset_index()
    firms_emp_by_sctg['SCTG_name'] = \
        reindex(firm_pref_weights.SCTG_description, firms_emp_by_sctg.SCTG)
    firms_emp_by_sctg.to_csv(os.path.join(output_dir, "firms_emp_by_sctg.csv"), index=False)

    # - producers

    # FIXME - 'producers' should be unique or cartesian?
    firms_sum['producers_rows'] = producers.shape[0]
    firms_sum['producers_unique'] = producers.seller_id.nunique()
    firms_sum['producers_emp'] = producers.size.sum()
    firms_sum['producers_cap'] = producers.output_capacity_tons.sum()

    # - producers_emp_by_sctg
    producers_emp_by_sctg = \
        producers[['SCTG', 'size', 'output_capacity_tons']].\
        groupby('SCTG').\
        agg({'size': ['count', 'sum'], 'output_capacity_tons': 'sum'})
    producers_emp_by_sctg.columns = producers_emp_by_sctg.columns.map('_'.join)
    producers_emp_by_sctg.rename(columns={'size_count': 'producers',
                                          'size_sum': 'employment',
                                          'output_capacity_tons_sum': 'output_capacity'},
                                 inplace=True)
    producers_emp_by_sctg.reset_index(inplace=True)  # explicit SCTG column
    producers_emp_by_sctg['SCTG_name'] = \
        reindex(firm_pref_weights.SCTG_description, producers_emp_by_sctg.SCTG)
    producers_emp_by_sctg.to_csv(os.path.join(output_dir, "producers_emp_by_sctg.csv"),
                                 index=False)

    # - consumers

    firms_sum['consumers_unique'] = consumers.buyer_id.nunique()
    firms_sum['consumption_pairs'] = consumers.shape[0]
    firms_sum['threshold'] = base_variables.BASE_PROVALTHRESHOLD
    firms_sum['consumer_inputs'] = consumers.purchase_amount_tons.sum()

    # - consumers_by_sctg
    consumers_by_sctg = consumers[['SCTG', 'purchase_amount_tons']]. \
        groupby('SCTG').purchase_amount_tons.agg(['sum', 'count']). \
        rename(columns={'sum': 'input_requirements', 'count': 'consumers'}). \
        reset_index()

    consumers_by_sctg['SCTG_name'] = \
        reindex(firm_pref_weights.SCTG_description, consumers_by_sctg.SCTG)
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

    # - write firms_sum
    summary = pd.DataFrame({'key': firms_sum.keys(), 'value': firms_sum.values()})
    summary.to_csv(os.path.join(output_dir, "firm_sim_summary.csv"), index=False)


def regress(df, step_name, df_name):
    file_path = 'regression_data/results/%s/outputs/x_%s.csv' % (step_name, df_name)
    df.to_csv(file_path, index=True)


@inject.step()
def firm_synthesis(
        output_dir,
        NAICS2012_to_NAICS2007,
        NAICS2007_to_NAICS2007io,
        NAICS2012_to_NAICS2007io,
        employment_categories,
        naics_industry,
        industry_10_5,
        naics_empcat,
        taz_fips,
        taz_faf4,
        socio_economics_taz,
        NAICS2007io_to_SCTG,
        input_output_values,
        unitcost,
        numa_dist,
        firm_pref_weights,
        foreign_prod_values,
        foreign_cons_values,
        firms_est
):
    REGRESS = False
    TRACE_TAZ = None

    t0 = print_elapsed_time()

    NAICS2012_to_NAICS2007 = NAICS2012_to_NAICS2007.to_frame()
    NAICS2007_to_NAICS2007io = NAICS2007_to_NAICS2007io.to_frame()
    NAICS2012_to_NAICS2007io = NAICS2012_to_NAICS2007io.to_frame()
    employment_categories = employment_categories.to_frame()
    naics_industry = naics_industry.to_frame()
    industry_10_5 = industry_10_5.to_frame()
    naics_empcat = naics_empcat.to_frame()
    taz_fips = taz_fips.to_frame()
    taz_faf4 = taz_faf4.to_frame()
    socio_economics_taz = socio_economics_taz.to_frame()
    NAICS2007io_to_SCTG = NAICS2007io_to_SCTG.to_frame()
    input_output_values = input_output_values.to_frame()
    unitcost = unitcost.to_frame()
    numa_dist = numa_dist.to_frame()
    firm_pref_weights = firm_pref_weights.to_frame()
    foreign_prod_values = foreign_prod_values.to_frame()
    foreign_cons_values = foreign_cons_values.to_frame()
    foreign_prod_values = pd.merge(foreign_prod_values, NAICS2007_to_NAICS2007io, how='inner',
                                   left_on='NAICS6', right_index=True)
    foreign_cons_values = pd.merge(foreign_cons_values, NAICS2007_to_NAICS2007io, how='inner',
                                   left_on='NAICS6', right_index=True)
    firms_est = firms_est.to_frame()

    t0 = print_elapsed_time("load dataframes", t0, debug=True)

    # - firm_sim_load_firms
    t0 = print_elapsed_time()
    firms = firm_sim_load_firms(
        NAICS2012_to_NAICS2007)
    t0 = print_elapsed_time("firm_sim_load_firms", t0, debug=True)

    if REGRESS:
        # use uncorrected naics for regression
        logger.warn("using uncorrected firms.naics codes for regression")
        firms.NAICS2012 = firms.pnaics

    logger.info("%s firms" % (firms.shape[0],))

    # - firm_sim_enumerate
    t0 = print_elapsed_time()
    firms = firm_sim_enumerate(
        firms,
        NAICS2012_to_NAICS2007io,
        employment_categories,
        naics_industry,
        industry_10_5)
    t0 = print_elapsed_time("firm_sim_enumerate", t0, debug=True)

    logger.info("%s null NAICS_make in firms" % (firms.NAICS6_make.isnull().sum(),))

    if REGRESS:
        regress(df=firms, step_name='firm_sim_enumerate', df_name='Firms')

    # - firm_sim_enumerate_foreign
    t0 = print_elapsed_time()
    firms = firm_sim_enumerate_foreign(
        firms,
        NAICS2007io_to_SCTG,
        employment_categories,
        naics_industry,
        industry_10_5,
        foreign_prod_values, foreign_cons_values)
    t0 = print_elapsed_time("firm_sim_enumerate_foreign", t0, debug=True)
    logger.info("%s null NAICS_make in firms" % (firms.NAICS6_make.isnull().sum(),))

    if REGRESS:
        regress(df=firms, step_name='firm_sim_enumerate_foreign', df_name='Firms')

    # - firm_sim_taz_allocation
    t0 = print_elapsed_time()
    firms = firm_sim_taz_allocation(
        firms,
        naics_empcat)
    t0 = print_elapsed_time("firm_sim_taz_allocation", t0, debug=True)

    if REGRESS:
        regress(df=firms, step_name='firm_sim_tazallocation', df_name='Firms')
    if TRACE_TAZ is not None:
        print "\nfirm_sim_tazallocation TRACE_TAZ %s firms\n" % \
              TRACE_TAZ, firms[firms.TAZ == TRACE_TAZ][['TAZ', 'model_emp_cat', 'emp']].sort_index()

    # - firm_sim_scale_employees
    t0 = print_elapsed_time()
    firms = firm_sim_scale_employees(
        firms,
        employment_categories,
        naics_empcat,
        socio_economics_taz,
        numa_dist)
    t0 = print_elapsed_time("firm_sim_scale_employees", t0, debug=True)

    if REGRESS:
        regress(df=firms, step_name='firm_sim_scale_employees', df_name='Firms')
    if TRACE_TAZ is not None:
        print "\nfirm_sim_scale_employees TRACE_TAZ %s firms\n" % \
              TRACE_TAZ, firms[firms.TAZ == TRACE_TAZ][['TAZ', 'NAICS2007', 'NAICS6_make', 'emp']]

    # - firm_sim_assign_SCTG
    t0 = print_elapsed_time()
    firms = firm_sim_assign_SCTG(
        firms,
        NAICS2007io_to_SCTG)
    t0 = print_elapsed_time("firm_sim_assign_SCTG", t0, debug=True)

    logger.debug("%s firms with null SCTG" % firms.SCTG.isnull().sum())
    logger.debug("%s firms with null NAICS6_make" % firms.NAICS6_make.isnull().sum())

    if REGRESS:
        regress(df=firms, step_name='firm_sim_sctg', df_name='Firms')
    if TRACE_TAZ is not None:
        print "\nfirm_sim_assign_SCTG TRACE_TAZ %s firms\n" % \
              TRACE_TAZ, firms[firms.TAZ == TRACE_TAZ][['TAZ', 'NAICS2007', 'NAICS6_make', 'SCTG']]

    # - firm_sim_types
    t0 = print_elapsed_time()
    firms = firm_sim_types(
        firms)
    t0 = print_elapsed_time("firm_sim_types", t0, debug=True)

    logger.info('%s firms, %s producers, %s makers' %
                (firms.shape[0], firms.producer.sum(), firms.maker.sum()))

    if REGRESS:
        regress(df=firms, step_name='firm_sim_types', df_name='Firms')
    if TRACE_TAZ is not None:
        print "\nfirm_sim_types TRACE_TAZ %s firms\n" % \
              TRACE_TAZ, firms[firms.TAZ == TRACE_TAZ][['TAZ', 'SCTG', 'producer', 'maker']]

    # - firm_sim_producers
    t0 = print_elapsed_time()
    producers = firm_sim_producers(
        firms,
        input_output_values,
        unitcost
    )
    t0 = print_elapsed_time("firm_sim_producers", t0, debug=True)

    logger.info('%s producers' % (producers.shape[0],))

    if REGRESS:
        regress(df=producers, step_name='firm_sim_producers', df_name='producers')

    # # - firm_sim_iopairs
    # t0 = print_elapsed_time()
    # firm_io_pairs = firm_sim_iopairs(
    #     firms,
    #     input_output_values,
    #     NAICS2007io_to_SCTG)
    # t0 = print_elapsed_time("firm_sim_iopairs", t0, debug=True)
    #
    # logger.info('%s firm_sim_iopairs' % (firm_io_pairs.shape[0],))
    #
    # if REGRESS:
    #     regress(df=firm_io_pairs, step_name='firm_sim_iopairs', df_name='iopairs')
    # if TRACE_TAZ is not None:
    #     print "\nfirm_sim_iopairs TRACE_TAZ %s firm_input_output_pairs\n" % \
    #           TRACE_TAZ, firm_io_pairs[firm_io_pairs.TAZ == TRACE_TAZ].sort_values('bus_id')

    # - firm_sim_consumers
    t0 = print_elapsed_time()
    consumers = firm_sim_consumers(
        firms,
        input_output_values,
        NAICS2007io_to_SCTG,
        unitcost,
        firm_pref_weights)
    t0 = print_elapsed_time("firm_sim_consumers", t0, debug=True)

    logger.info('%s consumers' % (consumers.shape[0],))

    if REGRESS:
        regress(df=consumers, step_name='firm_sim_consumers', df_name='consumers')
    if TRACE_TAZ is not None:
        print "\nfirm_sim_consumers TRACE_TAZ %s consumers\n" % \
              TRACE_TAZ, consumers[consumers.TAZ == TRACE_TAZ].sort_values('bus_id')

    # - firm_sim_naics_set
    t0 = print_elapsed_time()
    matches_naics, naics_set = firm_sim_naics_set(
        producers,
        consumers)
    t0 = print_elapsed_time("firm_sim_naics_set", t0, debug=True)

    if REGRESS:
        regress(df=naics_set, step_name='firm_sim_naics_set', df_name='naics_set')

    # - firm_sim_summary
    t0 = print_elapsed_time()
    firm_sim_summary(
        output_dir,
        producers,
        consumers,
        firms,
        firm_pref_weights)
    t0 = print_elapsed_time("firm_sim_summary", t0, debug=True)

    inject.add_table('firms_establishments', firms)
    inject.add_table('firm_io_pairs', firm_io_pairs)
    inject.add_table('producers', producers)
    inject.add_table('consumers', consumers)
    inject.add_table('matches_naics', matches_naics)
    inject.add_table('naics_set', naics_set)
