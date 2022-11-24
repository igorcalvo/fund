from io import StringIO, BytesIO
from openpyxl import load_workbook
from pandas import read_csv, read_excel, DataFrame
from contextlib import closing as cl
from zipfile import ZipFile
from xml.etree import ElementTree as et

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

def list_zip_filenames(zip_file) -> list:
    with cl(zip_file), ZipFile(BytesIO(zip_file.content)) as archive:
        return [f.filename for f in archive.filelist]

def get_file_content(zip_file, filename: str):
    with cl(zip_file), ZipFile(BytesIO(zip_file.content)) as archive:
        file = next((a for a in archive.filelist if a.filename == filename), None)
        content = archive.read(file)
        return content

def list_zip_filenames_bytes(zip_file: bytes) -> list:
    with ZipFile(BytesIO(zip_file)) as archive:
        return [f.filename for f in archive.filelist]

def get_file_content_bytes(zip_file: bytes, filename: str):
    with ZipFile(BytesIO(zip_file)) as archive:
        file = next((a for a in archive.filelist if a.filename == filename), None)
        content = archive.read(file)
        return content

def find_in_xml(xml: str, tag: str):
    tree = et.fromstring(xml)
    all_name_elements = tree.findall(f'.//{tag}')
    result = [element.text for element in all_name_elements]
    return result

def string_to_file(content: str, path_to_file: str):
    with open(path_to_file, 'w') as f:
        f.write(content)
        f.close()