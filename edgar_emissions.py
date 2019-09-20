import os
from pandas import DataFrame, read_excel

results_df = DataFrame()
year = 2012
transport_dir = os.environ['TRANSPORT_DIR']
data_dir = transport_dir + '/edgar_data/'
unit_scale = 1 * 10e9 * (1/10e6) * (1/10e6)  # Gg to MT
emissions_files = ['v432_CO2_excl_short-cycle_org_C_1970_2012', 'v432_CO2_org_short-cycle_C_1970_2012'
                   ,'v432_PM2.5_fossil_1970_2012', 'v432_PM2.5_bio_1970_2012']

for file in emissions_files:

    # Read raw data
    print('Reading ' + file)
    df = read_excel(data_dir + file + '.xls', header=7)

    # Get overall total and transport category totals
    transport_cats = ['Domestic aviation', 'Road transportation', 'Rail transportation', 'Inland navigation'
                      , 'Other transportation', 'Memo: International navigation', 'Memo: International aviation']

    totals = {}
    running_total = 0

    for tt in transport_cats:
        cat_total = df[df['IPCC_description'] == tt][year].sum() * unit_scale
        totals[tt] = cat_total
        assert (cat_total > 0)
        running_total = running_total + cat_total

    totals['all_other'] = (df[year].sum() * unit_scale) - running_total

    # Append to results container
    totals['series'] = file
    results_df = results_df.append(totals, ignore_index=True)

# export
results_df.to_csv(transport_dir + '/edgar_transport_summary.csv', index_label=False)
