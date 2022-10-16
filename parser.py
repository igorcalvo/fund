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
    df['DT_REFER2'] = df.apply(fix_quarter_dates, axis=1)
    # df['VL_CONTA2'] = df.apply(calculate_quarters, axis=1)

def quarter_months(quarter: int, year: int):
    # month = int(date.split('-')[1])
    if quarter == 0:
        return f'{year}-03-31'
    elif quarter == 1:
        return f'{year}-06-30'
    elif quarter == 2:
        return f'{year}-09-30'
    else:
        return f'{year}-12-31'

def get_dates_list(df, cnpj: str):
    cnpj_rows = df.loc[df['CNPJ_CIA'] == cnpj]
    print(cnpj_rows)
    dates_list = list(cnpj_rows['DT_REFER'])
    return dates_list
def fix_quarter_dates(row, dates: list, year: int):
    value = quarter_months(dates.index(row['DT_REFER']), year)
    return value
# def calculate_quarters(row, current_date: str, previous_date: str):