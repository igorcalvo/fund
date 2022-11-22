from .statement_logic import *
from .company_info import *
from .date_utils import today

def generate_statements(statement: str = '', years_back: int = 5, export_raw_data: bool = False, multi_core: bool = True):
    date = today()
    years = list(range(date.year, date.year - years_back, -1))
    if years_back > 2000:
        years = [years_back]

    statements = ['DRE', 'DFC_MI', 'BPA', 'BPP']
    if statement != '':
        statements = [statement]

    zip_files = download_zips(['ITR', 'DFP'], years)

    results = []
    for statement in statements:
        itrs, dfps = [], []
        for year in years:
            itrs.append(get_data(zip_files, 'ITR', year, statement))
            dfps.append(get_data(zip_files, 'DFP', year, statement))

        print(f'{statement} - concatenating')
        itr_df = pd.concat(itrs)
        dfp_df = pd.concat(dfps)

        if export_raw_data:
            print(f'{statement} - exporting')
            df_before = epd.append_dfs(itr_df, dfp_df)
            epd.save_xlsx(df_before, f'xlsx/{statement}_raw')

        print(f'{statement} - transforming')
        itr_df = prepare_df(itr_df, statement, True)
        dfp_df = prepare_df(dfp_df, statement, False)

        print(f'{statement} - appending')
        df = epd.append_dfs(itr_df, dfp_df)

        print(f'{statement} - calculating')
        df = calculate_values(df, statement, multi_core)

        print(f'{statement} - formatting')
        df = format_for_output(df, statement)
        results.append(df)

    print(f'{statement} - writing')
    if len(statements) == 1:
        filename = f'{statement}_{years[0]}' if len(years) == 1 else 'statement'
    else:
        filename = f'statements_{years[0]}' if len(years) == 1 else 'statements'
    output = epd.save_sheets_xlsx(results, statements, filename, 'xlsx')
    print(f'exported - {output}')


def export_company_info(year: int = 0):
    if year == 0:
        year = today().year

    files = get_fca(year)
    comp_df, ticker_df = get_dfs(files, year)
    comp_df_raw = comp_df.copy(deep=True)
    ticker_df_raw = ticker_df.copy(deep=True)

    print('companies - cleaning up')
    comp_df = cleanup_company_df(comp_df)
    ticker_df = cleanup_ticker_df(ticker_df)

    print('companies - merging')
    df = pd.merge(comp_df, ticker_df, how='left', on='CNPJ_Companhia')

    print('companies - exporting')
    output = epd.save_sheets_xlsx([df, ticker_df_raw, comp_df_raw], ['companies', 'ticker', 'info'], 'companies', 'xlsx')

    print(f'exported - {output}')
