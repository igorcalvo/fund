from .date_utils import *
from .cvm import get_data, download_zips
import pandas as pd
import ez_pandas.ez_pandas as epd
import line_profiler
# @profile

statement_mapping = {
    "DRE": "DRE",
    "DFC_MI": "DRE",
    "BPA": "BPA",
    "BPP": "BPA",
}

statement_columns = {
    "prepare": ['MOEDA', 'VERSAO', 'ESCALA_MOEDA', 'ORDEM_EXERC'],
    "cleanup": ['VL_CONTA', 'GRUPO_DFP', 'CD_CVM'],
    "output_DRE": ['CNPJ_CIA', 'DENOM_CIA', 'DT_REFER', 'DT_INI_EXERC', 'DT_FIM_EXERC', 'ST_CONTA_FIXA', 'CD_CONTA', 'DS_CONTA', 'VALOR'],
    "output_BPA": ['CNPJ_CIA', 'DENOM_CIA', 'DT_REFER', 'DT_FIM_EXERC', 'ST_CONTA_FIXA', 'CD_CONTA', 'DS_CONTA', 'VALOR'],
    "sort": ['DENOM_CIA', 'DT_REFER', 'CD_CONTA'],
}

def get_step_statement_columns(step: str, statement: str = ''):
    if statement == '':
        return statement_columns[step]
    else:
        mapped_statement = statement_mapping[statement]
        key = f"{step}_{mapped_statement}"
        return statement_columns[key]

def correct_currency(df: pd.DataFrame) -> pd.DataFrame:
    df['VL_CONTA'] = df.apply(
        lambda row: row['VL_CONTA'] if row['ESCALA_MOEDA'] != 'MIL' else 1000 * row['VL_CONTA'], axis=1
    )
    return df

def remove_all_but_last(df: pd.DataFrame) -> pd.DataFrame:
    df = epd.where_column_equals(df, 'ORDEM_EXERC', 'ÚLTIMO')
    return df

def drop_not_date(df: pd.DataFrame) -> pd.DataFrame:
    df = epd.where_column_contains(df, 'DT_INI_EXERC', '-01-01')
    return df

def prepare_df(df: pd.DataFrame, statement: str, is_itr: bool) -> pd.DataFrame:
    df = correct_currency(df)
    df = remove_all_but_last(df)
    df = df.drop(columns=get_step_statement_columns("prepare"), axis="columns")
    if is_itr and statement == "DRE":
        df = drop_not_date(df)
    return df

# @profile
def update_account_values(row, df: pd.DataFrame, year_first: bool):
    ref_date = row['DT_REFER']

    if get_date_month(ref_date) == 3:
        return row['VL_CONTA']
    else:
        previous_date = get_previous_date(ref_date, year_first)
        query_value0 = df.loc[(df['CNPJ_CIA'] == row['CNPJ_CIA']) &
                              (df['CD_CONTA'] == row['CD_CONTA']) &
                              (df['DT_REFER'] == previous_date)]

        try:
            value0 = epd.get_single_value(query_value0, 'VL_CONTA') if query_value0.shape[0] != 0 else 0

            #region Remove for porformance
            if query_value0.shape[0] > 1:
                valuex = query_value0['VL_CONTA'].values[0]
                valuey = query_value0['VL_CONTA'].values[0]
                if valuex != valuey:
                    print("update_account_values: duplicate value:")
                    epd.print_df(query_value0)
                    pass
            #endregion

            value = row['VL_CONTA'] - value0
            return value
        except Exception as e:
            # try:
            #     if 'with size 0' in str(e):
            #         return row['VL_CONTA']
            #     else:
            #         raise Exception(e)
            # except Exception as e:
            print(f"update_account_values error: {e}")

def calculate_values(df: pd.DataFrame, statement: str, parallel: bool = True) -> pd.DataFrame:
    df['DT_REFER'] = df['DT_REFER'].apply(quarter_dates, args=[True])
    ref_df = df.copy(deep=True)
    if parallel:
        df = epd.parallel_apply(df, 'VL_CONTA2', update_account_values, [ref_df, True])
    else:
        df['VL_CONTA2'] = df.apply(update_account_values, [ref_df, True], axis=1)
    return df

def format_for_output(df: pd.DataFrame, statement: str) -> pd.DataFrame:
    df = df.drop(get_step_statement_columns("cleanup"), axis="columns")
    df = df.rename({'VL_CONTA2': 'VALOR'}, axis='columns')
    df = df[get_step_statement_columns("output", statement)]
    df = df.sort_values(get_step_statement_columns("sort"))
    return df