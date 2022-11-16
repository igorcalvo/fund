# def first_or_default(data_list: list, lamba_function):
#     obj = next((o for o in data_list if lamba_function), None)
#     return obj

def write_csv(fileName: str, data):
    separator = ';'
    cols = ';'.join([col.replace('\r\n', '') for col in data[0]])
    content = ''
    newline = '\r\n'
    for index, row in enumerate(data[1:]):
        content += f"\n{';'.join(row)}"
    with open(fileName, 'w') as dataFile:
        dataFile.seek(0)
        dataFile.write(f'{cols}')
        dataFile.write(f'{content}')
        dataFile.close()

def save_xls(df_list: list, sheet_names: list, output_path: str, output_filename: str):
    if len(df_list) != len(sheet_names):
        raise Exception(f"export_to_excel: length of df_list: {len(df_list)} doesn't match the length of sheet_names: {len(sheet_names)}")

    full_path = output_filename if output_path == '' else f"{output_path}/{output_filename}"
    with pd.ExcelWriter(full_path) as writer:
        for index, df in enumerate(df_list):
            df.to_excel(writer, sheet_names[index])