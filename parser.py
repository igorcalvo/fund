from io import StringIO
import pandas as pd
import numpy as np

def bytes_to_stringio(bytes_data):
    string = str(bytes_data, 'ISO-8859-1')
    data = StringIO(string)
    return data

def dataframe_from_content(content: bytes, separator=";"):
    string_io = bytes_to_stringio(content)
    # print(string_io.read().split('\r\n')[35])
    dataframe = pd.read_csv(string_io, sep=separator)
    return dataframe

def save_xls(df_list: list, sheet_names: list, output_path: str, output_filename: str):
    if len(df_list) != len(sheet_names):
        raise Exception(f"export_to_excel: length of df_list: {len(df_list)} doesn't match the length of sheet_names: {len(sheet_names)}")

    full_path = f"{output_path}/{output_filename}"
    with pd.ExcelWriter(full_path) as writer:
        for index, df in enumerate(df_list):
            df.to_excel(writer, sheet_names[index])

def prepare_df(dataframe, year: float, is_itr: bool):
    df = dataframe.drop('MOEDA', axis=1)
    df = df.drop('VERSAO', axis=1)

    df = df[df['ORDEM_EXERC'] == 'ÚLTIMO']
    df = df.drop('ORDEM_EXERC', axis=1)

    df['VL_CONTA'] = np.where(df['ESCALA_MOEDA'] == 'MIL', df['VL_CONTA'] * 1000, df['VL_CONTA'])
    df = df.drop('ESCALA_MOEDA', axis=1)

    if is_itr:
        df = df[df['DT_INI_EXERC'] == f'{year}-01-01']

    return df

def calculate_values(df):
    df['VL_CONTA2'] = df.apply(lambda row: calculate_quarters(row, df), axis=1)
    return df

# def quarter_dates(month: int, year: int):
def quarter_dates(date):
    month = int(date.split('-')[1])
    year = int(date.split('-')[0])
    if month <= 3:
        return f'{year}-03-31'
    elif month <= 6:
        return f'{year}-06-30'
    elif month <= 9:
        return f'{year}-09-30'
    else:
        return f'{year}-12-31'

def get_previous_date(date):
    year = date.split('-')[0]
    if date == f'{year}-03-31':
        return 0
    elif date == f'{year}-06-30':
        return f'{year}-03-31'
    elif date == f'{year}-09-30':
        return f'{year}-06-30'
    elif date == f'{year}-12-31':
        return f'{year}-09-30'
    else:
        raise Exception(f"get_previous_date: got {date} for {year}")

# def get_dates_list(df, cnpj: str):
#     cnpj_rows = df.loc[df['CNPJ_CIA'] == cnpj]
#     print(cnpj_rows)
#     dates_list = list(cnpj_rows['DT_REFER'])
#     return dates_list
# def fix_quarter_dates(row, dates: list, year: int):
#     value = quarter_months(dates.index(row['DT_REFER']), year)
#     return value
# def calculate_quarters(df):
#     cnpjs = set(df['CNPJ_CIA'])
#     for idx, cnpj in enumerate(cnpjs):
#         data = df.loc[df['CNPJ_CIA'] == cnpj]
#         for account_code in set(data['CD_CONTA']):
#             for ref_date in set(data.loc[data['CD_CONTA'] == account_code]['DT_REFER2']):
#                 value1 = 0 if int(ref_date.split('-')[1]) == 3 else data.loc[(data['CD_CONTA'] == account_code) & (data['DT_REFER2'] == get_previous_date(ref_date))]['VL_CONTA']
#                 value2 = data.loc[(data['CD_CONTA'] == account_code) & (data['DT_REFER2'] == ref_date)]['VL_CONTA']
#                 df.loc[(df['CD_CONTA'] == account_code) & (df['DT_REFER2'] == ref_date) & (df['CNPJ_CIA'] == cnpj)]['VL_CONTA2'] = value2 - value1
#         print(f'{round(100 * float(idx) / len(cnpjs), 4)} %')
#     return df
#
def calculate_quarters(row, df):
    cnpj = row['CNPJ_CIA']
    account_code = row['CD_CONTA']
    ref_date = row['DT_REFER2']

    value1 = 0 if int(ref_date.split('-')[1]) == 3 else df.loc[(df['CD_CONTA'] == account_code) & (df['DT_REFER2'] == get_previous_date(ref_date)) & (df['CNPJ_CIA'] == cnpj)]['VL_CONTA']
    value2 = df.loc[(df['CD_CONTA'] == account_code) & (df['DT_REFER2'] == ref_date) & (df['CNPJ_CIA'] == cnpj)]['VL_CONTA']
    result = value2 - value1

    print(value2, value1, result)
    return result

