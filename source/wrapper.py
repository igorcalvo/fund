from .statement_logic import *
from .company_info import *
from .shares_logic import *
from .date_utils import today

def generate_statements(statement: str = '',
                        years_back: int = 5,
                        export_raw_data: bool = False,
                        multi_core: bool = True,
                        print_duplicates: bool = False):
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
            epd.export_xlsx(df_before, f'xlsx/{statement}_raw')

        print(f'{statement} - transforming')
        itr_df = prepare_df(itr_df, statement, True)
        dfp_df = prepare_df(dfp_df, statement, False)

        print(f'{statement} - appending')
        df = epd.append_dfs(itr_df, dfp_df)

        print(f'{statement} - calculating')
        df = calculate_values(df, statement, multi_core, print_duplicates)

        print(f'{statement} - formatting')
        df = format_for_output(df, statement)
        results.append(df)

    print(f'{statement} - writing')
    if len(statements) == 1:
        filename = f'{statement}_{years[0]}' if len(years) == 1 else 'statement'
    else:
        filename = f'statements_{years[0]}' if len(years) == 1 else 'statements'
    output = epd.export_sheets_xlsx(results, statements, filename, 'xlsx')
    print(f'exported - {output}')


def export_company_info(year: int = 0, export_xlsx: bool = True):
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

    if export_xlsx:
        print('companies - exporting')
        output = epd.export_sheets_xlsx([df, ticker_df_raw, comp_df_raw], ['companies', 'ticker', 'info'], 'companies', 'xlsx')
        print(f'exported - {output}')

    return df

def get_share_values(years_back: int = 5, multi_core: bool = True):
    try:
        print_prefix = 'share_values'

        date = today()
        years = list(range(date.year, date.year - years_back, -1))
        if years_back > 2000:
            years = [years_back]

        # cnpjs = [r'00.080.671/0001-00', r'00.545.378/0001-70', r'00.272.185/0001-93']

        zip_files = download_zips(['ITR', 'DFP'], years)

        print(f'{print_prefix} - appending')
        itrs, dfps = [], []
        for year in years:
            itrs.append(get_data(zip_files, 'ITR', year, '', '', f'{"ITR".lower()}_cia_aberta'))
            dfps.append(get_data(zip_files, 'DFP', year, '', '', f'{"DFP".lower()}_cia_aberta'))

        print(f'{print_prefix} - transforming')
        # df = get_dataframe(itrs, dfps, cnpjs)
        df = get_dataframe(itrs, dfps)
        # epd.print_df(df)
        print(f'{print_prefix} - {len(df)} rows')
        print(f'{print_prefix} - fetching data')
        share_columns = ['LINK_DOC', 'ON', 'PN', 'TOTAL', 'T ON', 'T PN', 'T TOTAL']
        shares_df = get_shares_multi_core(df, share_columns) if multi_core else get_shares_single_core(df, share_columns)

        print(f'{print_prefix} - joining')
        df = epd.join(df, shares_df, 'LINK_DOC', 'left')

        print(f'{print_prefix} - exporting')
        output = epd.export_sheets_xlsx([df], ['shares'], 'shares', 'xlsx')

        print(f'exported - {output}')
    except Exception as e:
        print("get_share_values", e)
