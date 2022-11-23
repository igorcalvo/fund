from .cvm import *
from .parser import df_from_content_xlsx
from multiprocessing import cpu_count
import ez_pandas.ez_pandas as epd

def do():
    year = 2021
    cnpjs = [r'00.080.671/0001-00', r'00.545.378/0001-70']

    zip_files = download_zips(['ITR', 'DFP'], [year])
    itr = get_data(zip_files, 'ITR', year, '', '', f'{"ITR".lower()}_cia_aberta')
    dfp = get_data(zip_files, 'DFP', year, '', '', f'{"DFP".lower()}_cia_aberta')
    df = epd.append_dfs(itr, dfp)
    df = epd.where_column_regex(df, 'CNPJ_CIA', '|'.join(cnpjs))
    df = df.groupby(["CNPJ_CIA", "DT_REFER"], as_index=False).max()
    epd.sort(df, ['CNPJ_CIA', 'DT_REFER'], [1, 1])
    epd.redefine_index(df)
    epd.print_df(df)

    link = epd.get_value(df, 4, 'LINK_DOC')
    zip_file = download_link(link)
    file_names = list_zip_filenames(zip_file)
    filtered_file_name = [f for f in file_names if f.split('.')[-1] in ['itr', 'dfp']]
    if len(filtered_file_name) == 0:
        filtered_file_name = [f for f in file_names if f.split('.')[-1] == 'xlsx']
        file_name = filtered_file_name[0]
        content = get_file_content(zip_file, file_name)
        df2 = df_from_content_xlsx(content, 'Composicao Capital')
        epd.print_df(df2)
        # value = epd.at(df2, 0, 'Capital Integralizado Acoes Preferenciais')
        value = epd.at(df2, 0, 1)
    else:
        file_names2 = list_zip_filenames(filtered_file_name[0])

    # content = get_file_content(zip_file)


    # epd.parallel_apply_column(df, 'FILE', 'LINK_DOC', download_link, [], cpu_count())
    return None