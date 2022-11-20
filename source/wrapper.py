from .logic import *
from .date_utils import today

def generate_dre(statement: str, years_back: int = 5):
    date = today()
    years = range(date.year, date.year - years_back, -1)

    if statement == "DRE":
        itrs = []
        dfps = []
        for year in years:
            print(f'{statement} - ITR - {year} - downloading')
            itrs.append(get_data('ITR/', year, statement))
            print(f'{statement} - DFP - {year} - downloading')
            dfps.append(get_data('DFP/', year, statement))

        print(f'{statement} - concatenating')
        itr_df = pd.concat(itrs)
        dfp_df = pd.concat(dfps)

        print(f'{statement} - transforming')
        itr_df = prepare_df(itr_df, year, True)
        dfp_df = prepare_df(dfp_df, year, False)

        print(f'{statement} - unifying')
        dre_df = epd.append_dfs(itr_df, dfp_df)
        # dre_before = dre_df.copy(deep=True)

        print(f'calculating')
        dre_df = calculate_values(dre_df)

    print(f'writing to excel')
    # epd.save_sheets_xlsx([dre_df, dfp_df, itr_df], ['DRE', 'DFP', 'ITR'], f'dre', '')
    epd.save_sheets_xlsx([dre_df], ['DRE'], f'DRE', 'xlsx')