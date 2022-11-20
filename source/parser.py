from io import StringIO
from pandas import read_csv

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
