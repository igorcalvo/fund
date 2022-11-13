from io import StringIO
import pandas as pd

def bytes_to_stringio(bytes_data):
    string = str(bytes_data, 'ISO-8859-1')
    data = StringIO(string)
    return data

def stringio_to_csv(string_io: StringIO, separator: str):
    df = pd.read_csv(string_io, sep=separator)
    df.to_csv('pandas.csv')
    string_io.seek(0)

def stringio_to_list(stringio: StringIO, separator: str):
    # stringio_to_csv(stringio, separator)
    result = [l.split(separator) for l in stringio]
    for l in result:
        l[-1] = l[-1].replace('\r\n', '')
    return result

def list_from_content(content: bytes, separator=";"):
    string_io = bytes_to_stringio(content)
    content_list = stringio_to_list(string_io, separator)
    return content_list

def save_xls(df_list: list, sheet_names: list, output_path: str, output_filename: str):
    if len(df_list) != len(sheet_names):
        raise Exception(f"export_to_excel: length of df_list: {len(df_list)} doesn't match the length of sheet_names: {len(sheet_names)}")

    full_path = output_filename if output_path == '' else f"{output_path}/{output_filename}"
    with pd.ExcelWriter(full_path) as writer:
        for index, df in enumerate(df_list):
            df.to_excel(writer, sheet_names[index])
