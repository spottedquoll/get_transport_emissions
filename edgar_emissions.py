import os
from pandas import DataFrame, read_excel, concat
import gc
from math import isnan

year = 2012
transport_dir = os.environ['TRANSPORT_DIR']

# Emissions
print('Processing EDGAR data')

results_df = DataFrame()
data_dir = transport_dir + '/edgar_data/'
unit_scale = 1e9 / 1e12
emissions_files = ['v432_CO2_excl_short-cycle_org_C_1970_2012', 'v432_CO2_org_short-cycle_C_1970_2012'
                   , 'v432_PM2.5_fossil_1970_2012', 'v432_PM2.5_bio_1970_2012', 'v432_PM10_1970_2012'
                   , 'v432_CH4_1970_2012', 'v432_NOx_1970_2012', 'v432_SO2_1970_2012', 'v432_N2O_1970_2012'
                   , 'v432_BC_1970_2012', 'v432_CO_1970_2012', 'v432_NMVOC_1970_2012']

transport_cats = ['Domestic aviation', 'Road transportation', 'Rail transportation', 'Inland navigation'
                  , 'Other transportation']

international_cats = ['Int. Shipping', 'Int. Aviation']

# Calculate totals for 2012
for file in emissions_files:

    # Read raw data
    print('Reading ' + file)
    df = read_excel(data_dir + file + '.xls', header=7)

    totals = {}
    running_total = 0

    for tt in transport_cats:
        cat_total = df[df['IPCC_description'] == tt][year].sum() * unit_scale
        totals[tt] = cat_total
        running_total = running_total + cat_total

    for it in international_cats:
        cat_total = df[df['Name'] == it][year].sum() * unit_scale
        totals[it] = cat_total
        running_total = running_total + cat_total

    totals['all_other'] = (df[year].sum() * unit_scale) - running_total

    # Append to results container
    totals['filename'] = file
    results_df = results_df.append(totals, ignore_index=True)

results_df.to_csv(transport_dir + '/edgar_transport_summary.csv')

# Calculate trend in CO2
print('Calculating trend in transport emissions')
co2_emissions_files = ['v432_CO2_excl_short-cycle_org_C_1970_2012', 'v432_CO2_org_short-cycle_C_1970_2012']

df_co2 = read_excel(data_dir + co2_emissions_files[0] + '.xls', header=7)
df_short = read_excel(data_dir + co2_emissions_files[1] + '.xls', header=7)

results = []

for trend_year in range(2000, 2013):
    totals = {'year': trend_year}

    series_name = 'co2_sc'
    for it in international_cats:
        cat_total = df_co2[df_co2['Name'] == it][trend_year].sum() * unit_scale
        totals[series_name + '_' + it] = cat_total

    totals[series_name + '_total'] = df_co2[trend_year].sum() * unit_scale

    series_name = 'co2_excl_sc'
    for it in international_cats:
        cat_total = df_short[df_short['Name'] == it][trend_year].sum() * unit_scale
        totals[series_name + '_' + it] = cat_total

    totals[series_name + '_total'] = df_short[trend_year].sum() * unit_scale

    results.append(totals)

DataFrame(results).to_csv(transport_dir + '/edgar_intl_transport_trend.csv')

# FAO data
print('Reading FAO data')
gc.collect()  # FAO data are large so clear memory

data_dir = transport_dir + '/fao_data/'
fao_prod_df = read_excel(data_dir + 'Production_Crops_E_All_Data.xlsx')
fao_trade_df = read_excel(data_dir + 'Trade_Crops_Livestock_E_All_Data.xlsx')

# Unique commodities from the production dataset (trade has more categories, including processed goods)
production_commodities = list(fao_prod_df['Item'].unique())
countries = list(fao_prod_df[fao_prod_df['Area Code'] < 5000]['Area'].unique())

year_column = 'Y' + str(year)
assert(year_column in list(fao_prod_df.columns))
assert(year_column in list(fao_trade_df.columns))

print('Calculating FAO import shares')
results = []
for c in countries:
    country_container = []

    c_prod_df = fao_prod_df[(fao_prod_df['Area'] == c) & (fao_prod_df['Element'] == 'Production')
                            & (fao_prod_df['Unit'] == 'tonnes')]
    assert(len(c_prod_df) > 0)

    c_trade_df = fao_trade_df[(fao_trade_df['Area'] == c) & (fao_trade_df['Unit'] == 'tonnes')]
    if len(c_trade_df) == 0:
        print('No trade found for ' + c)
    else:
        for p in production_commodities:

            production = c_prod_df[c_prod_df['Item'] == p][year_column]
            if len(production) == 0:
                production = 0
            else:
                assert (len(production) == 1)
                production = production.values[0]
                if isnan(production):
                    production = 0

            exports = c_trade_df[(c_trade_df['Item'] == p) & (c_trade_df['Element'] == 'Export Quantity')][year_column]
            if len(exports) == 0:
                exports = 0
            else:
                assert (len(exports) == 1)
                exports = exports.values[0]
                if isnan(exports):
                    exports = 0

            imports = c_trade_df[(c_trade_df['Item'] == p) & (c_trade_df['Element'] == 'Import Quantity')][year_column]
            if len(imports) == 0:
                imports = 0
            else:
                assert (len(imports) == 1)
                imports = imports.values[0]
                if isnan(imports):
                    imports = 0

            if production > 0 or imports > 0 or exports > 0:
                if imports == 0:
                    imp_frac = 0
                elif production == 0 or (production - exports) <= 0:
                    imp_frac = 1
                else:
                    imp_frac = min(imports / (production - exports), 1)

                net = production - imports

                country_container.append({'country': c, 'commodity': p, 'imports': imports, 'exports': exports
                                         , 'production': production, 'imp_frac': imp_frac, 'net_domestic_prod': net})

        results.append(DataFrame(country_container))

# Finish the results
all_results = concat(results, ignore_index=True)
all_results.to_csv(transport_dir + '/imported_fractions.csv')

print('All finished')