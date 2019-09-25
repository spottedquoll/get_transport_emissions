import os
from pandas import DataFrame, read_excel

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

print('All finished')