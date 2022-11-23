from io import StringIO, BytesIO
from openpyxl import load_workbook
from pandas import read_csv, read_excel, DataFrame

def bytes_to_stringio(bytes_data):
    string = str(bytes_data, 'ISO-8859-1')
    data = StringIO(string)
    return data

def stringio_to_csv(string_io: StringIO, separator: str, file_name: str):
    df = read_csv(string_io, sep=separator)
    df.to_csv(file_name)
    string_io.seek(0)

def df_from_content(content: bytes, separator=';'):
    string_io = bytes_to_stringio(content)
    dataframe = read_csv(string_io, sep=separator)
    return dataframe

def openpyxl_sheet_to_df(sheet) -> DataFrame:
    df = DataFrame(list(sheet.values))
    df.columns = df.iloc[0, :]
    df = df.iloc[1:, ].reset_index(drop=True)
    return df

def df_from_content_xlsx(content: bytes, sheet: str) -> DataFrame:
    xlsx = BytesIO(content)
    xlsx_workbook = load_workbook(xlsx)
    xlsx_sheet = xlsx_workbook[sheet]
    df = openpyxl_sheet_to_df(xlsx_sheet)
    return df