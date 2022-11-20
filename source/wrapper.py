from .logic import *
from .date_utils import today

# @profile
def generate_dre(statement: str, years_back: int = 5):
    date = today()
    years = range(date.year, date.year - years_back, -1)
    # years = [2021]
    zip_files = download_zips(['ITR', 'DFP'], list(years))
    if statement == "DRE":
        itrs = []
        dfps = []
        for year in years:
            itrs.append(get_data(zip_files, 'ITR', year, statement))
            dfps.append(get_data(zip_files, 'DFP', year, statement))

        print(f'{statement} - concatenating')
        itr_df = pd.concat(itrs)
        dfp_df = pd.concat(dfps)

        print(f'{statement} - transforming')
        itr_df = prepare_df(itr_df, year, True)
        dfp_df = prepare_df(dfp_df, year, False)

        print(f'{statement} - appending')
        dre_df = epd.append_dfs(itr_df, dfp_df)
        # dre_before = dre_df.copy(deep=True)

        print(f'{statement} - calculating')
        dre_df = calculate_values(dre_df)

    print(f'{statement} - formatting')
    dre_df = format_for_output(dre_df)

    print(f'{statement} - writing')
    output = epd.save_sheets_xlsx([dre_df], ['DRE'], f'DRE', 'xlsx')
    print(f'exported - {output}')
