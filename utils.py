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
