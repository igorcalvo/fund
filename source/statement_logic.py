from ez_pandas.ez_pandas import where_column_equals, where_column_contains, where_column_regex, get_single_value, sort, print_df
from .date_utils import get_date_month, get_date_year, get_previous_date, date_string
from pandas import DataFrame, concat
from numpy import array_split
from itertools import repeat
from multiprocessing import cpu_count, Pool

statement_columns_mapping = {
    "DRE": "DRE",
    "DFC_MI": "DRE",
    "BPA": "BPA",
    "BPP": "BPA",
}

statement_columns = {
    "cleanup": ['MOEDA', 'VERSAO', 'ESCALA_MOEDA', 'ORDEM_EXERC', 'GRUPO_DFP', 'CD_CVM'],
    "output_DRE": ['CNPJ_CIA', 'DENOM_CIA', 'DT_REFER', 'DT_INI_EXERC', 'DT_FIM_EXERC', 'ST_CONTA_FIXA', 'CD_CONTA', 'DS_CONTA', 'VALOR', 'DELTA'],
    "output_BPA": ['CNPJ_CIA', 'DENOM_CIA', 'DT_REFER', 'DT_FIM_EXERC', 'ST_CONTA_FIXA', 'CD_CONTA', 'DS_CONTA', 'VALOR', 'DELTA'],
    "sort": ['DENOM_CIA', 'DT_REFER', 'CD_CONTA'],
}

def get_step_statement_columns(step: str, statement: str = ''):
    if statement == '':
        return statement_columns[step]
    else:
        mapped_statement = statement_columns_mapping[statement]
        key = f"{step}_{mapped_statement}"
        return statement_columns[key]

def correct_currency(df: DataFrame) -> DataFrame:
    df['VL_CONTA'] = df.apply(
        lambda row: row['VL_CONTA'] if row['ESCALA_MOEDA'] != 'MIL' else 1000 * row['VL_CONTA'], axis=1
    )
    return df

def remove_all_but_last(df: DataFrame) -> DataFrame:
    df = where_column_equals(df, 'ORDEM_EXERC', 'ÚLTIMO')
    return df

def drop_not_date(df: DataFrame) -> DataFrame:
    df = where_column_contains(df, 'DT_INI_EXERC', '-01-01')
    return df

def dot_count(string: str) -> int:
    return string.count('.')

def startswith_in_list(string: str, list: str) -> bool:
    return any([string.startswith(v) for v in list])

def keep_account_levels(df: DataFrame, levels: list, exceptions: list = []) -> DataFrame:
    ex_df = None
    if len(exceptions) > 0:
        ex_df = df.copy(deep=True)
        ex_df['ACCOUNT_EXCEPTION'] = ex_df['CD_CONTA'].apply(startswith_in_list, args=[exceptions])
        ex_df = ex_df.loc[(ex_df['ACCOUNT_EXCEPTION'])]
        ex_df = ex_df.drop(columns=['ACCOUNT_EXCEPTION'], axis="columns")

    base_df = df.copy(deep=True)
    base_df['ACCOUNT_LEVEL'] = base_df['CD_CONTA'].apply(dot_count)
    if len(levels) == 1:
        level_df = base_df.loc[base_df['ACCOUNT_LEVEL'] == levels[0]]
    elif len(levels) == 2:
        level_df = base_df.loc[(base_df['ACCOUNT_LEVEL'] >= levels[0]) & (base_df['ACCOUNT_LEVEL'] <= levels[1])]
    else:
        raise Exception(f"keep_account_levels: don't use more than two levels, first is considered the min and last the max. got: {levels}")
    level_df = level_df.drop(columns=['ACCOUNT_LEVEL'], axis="columns")

    result = concat([level_df, ex_df], ignore_index=True, axis='rows') if ex_df is not None else level_df
    return result

def remove_deeper_account_levels(df: DataFrame, statement: str) -> DataFrame:
    if statement == 'DRE':
        return keep_account_levels(df, [1], ['3.04', '3.06'])
    elif statement == 'DFC_MI':
        return keep_account_levels(df, [1])
    elif statement in ('BPA', 'BPP'):
        return keep_account_levels(df, [0, 2])
    else:
        raise Exception(f'remove_deeper_account_levels: statement "{statement}" is not valid.')

def prepare_df(df: DataFrame, statement: str, accounts_dict: DataFrame, is_itr: bool) -> DataFrame:
    df = correct_currency(df)
    df = remove_all_but_last(df)
    df = df.drop(columns=get_step_statement_columns("cleanup"), axis="columns")
    df = remove_deeper_account_levels(df, statement)
    # accounts_dict
    if is_itr and statement == "DRE":
        df = drop_not_date(df)
    df = df.reset_index(drop=True)
    return df

def quarter_dates(row: DataFrame, year_first: bool = True):
    date_end = row['DT_FIM_EXERC']
    date_ref = row['DT_REFER']

    year = get_date_year(date_end)
    month = int(date_ref.split('-')[1])

    if month <= 3:
        return date_string(31, 3, year, year_first)
    elif month <= 6:
        return date_string(30, 6, year, year_first)
    elif month <= 9:
        return date_string(30, 9, year, year_first)
    else:
        return date_string(31, 12, year, year_first)

def handle_duplicate_values(query_value0: DataFrame, statement: str, print_duplicates: bool) -> float:
    value_x = query_value0['VL_CONTA'].values[0]
    value_y = query_value0['VL_CONTA'].values[1]
    if value_x != value_y:
        if statement == 'DRE':
            sort(query_value0, ['DT_INI_EXERC', 'DT_FIM_EXERC'], [1, 1])
        else:
            sort(query_value0, ['DT_FIM_EXERC'], [1])
        value0 = get_single_value(query_value0, 'VL_CONTA')
        if print_duplicates:
            print(f"\nupdate_account_values - duplicate values - returning: {value0}")
            print_df(query_value0)
        return value0
    else:
        return value_x

def update_account_values(row, df: DataFrame, statement: str, print_duplicates: bool, year_first: bool):
    ref_date = row['DT_REFER']

    if get_date_month(ref_date) == 3:
        return row['VL_CONTA']
    else:
        previous_date = get_previous_date(ref_date, year_first)
        query_value0 = df.loc[(df['CNPJ_CIA'] == row['CNPJ_CIA']) &
                              (df['CD_CONTA'] == row['CD_CONTA']) &
                              (df['DT_REFER'] == previous_date)]

        value0 = get_single_value(query_value0, 'VL_CONTA') if query_value0.shape[0] != 0 else 0

        # If duplicate values
        if query_value0.shape[0] > 1:
            value0 = handle_duplicate_values(query_value0, statement, print_duplicates)

        value = row['VL_CONTA'] - value0
        return value


def process_df(input_df: DataFrame, column: str, function_to_apply, ref_df: DataFrame, statement: str, print_duplicates: bool, year_first: bool) -> DataFrame:
    output_df = input_df.copy(deep=True)
    arg_list = [ref_df, statement, print_duplicates, year_first]
    output_df[column] = output_df.apply(function_to_apply, axis=1, args=arg_list)
    return output_df

def parallel_apply(df: DataFrame, column: str, function_to_apply, ref_df: DataFrame, statement: str, print_duplicates: bool, year_first: bool) -> DataFrame:
    free_cores = 2
    cores = cpu_count() - free_cores

    df_chunks = array_split(df, cores)
    df_chunks_years = [list(set([str(get_date_year(date)) for date in list(sub_df['DT_REFER'].unique())])) for sub_df in df_chunks]
    ref_df_chunks = [where_column_regex(ref_df, 'DT_REFER', '|'.join(y)) for y in df_chunks_years]

    with Pool(cores) as pool:
        full_output_df = concat(pool.starmap(process_df, zip(df_chunks, repeat(column), repeat(function_to_apply), ref_df_chunks, repeat(statement), repeat(print_duplicates), repeat(year_first))), ignore_index=True)
    return full_output_df

def calculate_values(df: DataFrame, statement: str, parallel: bool = True, print_duplicates: bool = False) -> DataFrame:
    df['DT_REFER'] = df.apply(quarter_dates, args=[True], axis=1)
    df = df.sort_values(['DT_REFER'])
    ref_df = df.copy(deep=True)
    if parallel:
        df = parallel_apply(df, 'VL_CONTA2', update_account_values, ref_df, statement_columns_mapping[statement], print_duplicates, True)
    else:
        df['VL_CONTA2'] = df.apply(update_account_values, args=[ref_df, statement_columns_mapping[statement], print_duplicates, True], axis=1)
    return df

def format_for_output(df: DataFrame, statement: str) -> DataFrame:
    df = df.rename({'VL_CONTA2': 'DELTA', 'VL_CONTA': 'VALOR'}, axis='columns')
    df = df[get_step_statement_columns("output", statement)]
    df = df.sort_values(get_step_statement_columns("sort"))
    return df
