from ez_pandas.ez_pandas import append_dfs, export_xlsx, export_sheets_xlsx, join
from .statement_logic import prepare_df, calculate_values, format_for_output
from .company_info import get_fca, get_dfs, cleanup_company_df, cleanup_ticker_df
from .shares_logic import get_dataframe, get_shares_single_core, get_shares_multi_core
from .cvm import download_zips, get_data
from .date_utils import today
from .google_sheets import read_sheet
from pandas import DataFrame, concat, merge
from time import time
from datetime import timedelta

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
    accounts_dict = read_sheet('13Y4e7zbz6yY9KQIFZsdkegMby3ZYBDBX8UP8g107F0g', 'Sheet2', 'B3:D13')

    results = []
    for statement in statements:
        itrs, dfps = [], []
        for year in years:
            itrs.append(get_data(zip_files, 'ITR', year, statement))
            dfps.append(get_data(zip_files, 'DFP', year, statement))

        print(f'{statement} - concatenating')
        itr_df = concat(itrs)
        dfp_df = concat(dfps)

        if export_raw_data:
            print(f'{statement} - exporting')
            df_before = append_dfs(itr_df, dfp_df)
            export_xlsx(df_before, f'xlsx/{statement}_raw')

        print(f'{statement} - transforming')
        itr_df = prepare_df(itr_df, statement, accounts_dict, True)
        dfp_df = prepare_df(dfp_df, statement, accounts_dict, False)

        print(f'{statement} - appending')
        df = append_dfs(itr_df, dfp_df)

        print(f'{statement} - calculating')
        df = calculate_values(df, statement, multi_core, print_duplicates)

        print(f'{statement} - formatting')
        df = format_for_output(df, statement)
        results.append(df)

    print(f'writing')
    if len(statements) == 1:
        filename = f'{statement}_{years[0]}' if len(years) == 1 else 'statement'
    else:
        filename = f'statements_{years[0]}' if len(years) == 1 else 'statements'
    output = export_sheets_xlsx(results, statements, filename, 'xlsx')
    print(f'exported - {output}')


def get_company_info(year: int = 0, export_xlsx: bool = True):
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
    df = merge(comp_df, ticker_df, how='left', on='CNPJ_Companhia')

    if export_xlsx:
        print('companies - exporting')
        output = export_sheets_xlsx([df, ticker_df_raw, comp_df_raw], ['companies', 'ticker', 'info'], 'companies', 'xlsx')
        print(f'exported - {output}')

    return df

def get_share_values(cnpjs: list = [], years_back: int = 5, multi_core: bool = True):
    try:
        print_prefix = 'share_values'

        date = today()
        years = list(range(date.year, date.year - years_back, -1))
        if years_back > 2000:
            years = [years_back]

        zip_files = download_zips(['ITR', 'DFP'], years)

        print(f'{print_prefix} - appending')
        itrs, dfps = [], []
        for year in years:
            itrs.append(get_data(zip_files, 'ITR', year, '', '', f'{"ITR".lower()}_cia_aberta'))
            dfps.append(get_data(zip_files, 'DFP', year, '', '', f'{"DFP".lower()}_cia_aberta'))

        print(f'{print_prefix} - transforming')
        df = get_dataframe(itrs, dfps, cnpjs)
        df = df[:len(df)//8]

        print(f'{print_prefix} - fetching data for {len(df)} rows')
        share_columns = ['LINK_DOC', 'ON', 'PN', 'TOTAL', 'T ON', 'T PN', 'T TOTAL']
        shares_df = get_shares_multi_core(df, share_columns) if multi_core else get_shares_single_core(df, share_columns)

        print(f'{print_prefix} - joining')
        df = join(df, shares_df, 'LINK_DOC', 'left')

        print(f'{print_prefix} - exporting')
        output = export_sheets_xlsx([df], ['shares'], 'shares', 'xlsx')

        print(f'exported - {output}')
    except Exception as e:
        print("get_share_values", e)

def get_elapsed_time_message(start: float) -> str:
    elapsed_seconds = round(time() - start, 0)
    delta = timedelta(seconds=elapsed_seconds)
    string = str(delta)
    values = string.split(':')
    result = f'{"0" + str(values[0]) if values[0] < 10 else values[0]} hours {values[1]} minutes {values[2]} seconds'
    return f'*** DONE *** - run time - {result}'