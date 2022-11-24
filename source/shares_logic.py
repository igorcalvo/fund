from .parser import *
from .cvm import download_zips, get_data, download_link
from multiprocessing import cpu_count
from pandas import DataFrame
import ez_pandas.ez_pandas as epd

def extensions_in_filename_list(extensions: list, file_names: list) -> bool:
    return any([file_name for file_name in file_names if file_name.split('.')[-1] in extensions])

def get_file_name(extension: str, file_names: list):
    file_name_list = [file_name for file_name in file_names if file_name.split('.')[-1] == extension]
    if len(file_name_list) > 1:
        print(f'get_file_name: found {len(file_name_list)} files for ext: "{extension}" in {file_names}. Returning {file_name_list[0]}')
        return file_name_list[0]
    elif len(file_name_list) == 0:
        print(f'get_file_name: no files for ext: "{extension}" in {file_names}')
        return None
    else:
        return file_name_list[0]

def get_filename_containing(file_name: str, file_names: list):
    file_name_list = [filename for filename in file_names if file_name in filename]
    if len(file_name_list) > 1:
        print(f'get_filename_containing: found duplicate files for "{file_name}" in {file_names}. Returning {file_name_list[0]}')
        return file_name_list[0]
    elif len(file_name_list) == 0:
        print(f'get_filename_containing: no files matching the name "{file_name}" were found in {file_names}')
        return None
    else:
        return file_name_list[0]

def get_dataframe(itrs: list, dfps: list, cnpjs: list = []) -> DataFrame:
    itr_df = epd.append_df_list(itrs)
    dfp_df = epd.append_df_list(dfps)
    df = epd.append_dfs(itr_df, dfp_df)
    if len(cnpjs) > 0:
        df = epd.where_column_regex(df, 'CNPJ_CIA', '|'.join(cnpjs))
    df = df.groupby(['CNPJ_CIA', 'DT_REFER'], as_index=False).max()
    epd.drop_columns(df, ['ID_DOC', 'DT_RECEB', 'CD_CVM'])
    epd.sort(df, ['CNPJ_CIA', 'DT_REFER'], [1, 1])
    epd.redefine_index(df)
    return df

def shares_list_to_df(shares_values_list: list, share_names_list: list) -> DataFrame:
    df = DataFrame([shares_values_list])
    df.columns = share_names_list
    return df

def get_weird_shares_df(file_names: list, zip_file, share_columns: list):
    extension = 'itr' if extensions_in_filename_list(['itr'], file_names) else 'dfp'
    weird_file_name = get_file_name(extension, file_names)
    content = get_file_content(zip_file, weird_file_name)
    file_names_from_weird = list_zip_filenames_bytes(content)
    file_name = get_filename_containing('ComposicaoCapital', file_names_from_weird)
    xml = get_file_content_bytes(content, file_name)
    xml_string = xml.decode("UTF-8")
    xml_fields = ['QuantidadeAcaoOrdinariaCapitalIntegralizado',
                  'QuantidadeAcaoPreferencialCapitalIntegralizado',
                  'QuantidadeTotalAcaoCapitalIntegralizado']
    values = [find_in_xml(xml_string, tag)[0] for tag in xml_fields]
    df = shares_list_to_df(values, share_columns)
    epd.print_df(df)
    return df

def get_xlsx_shares_df(file_names: list, zip_file, share_columns: list):
    file_name = get_file_name('xlsx', file_names)
    content = get_file_content(zip_file, file_name)
    df = df_from_content_xlsx(content, 'Composicao Capital')
    #region ToBeRemoved
    precision = epd.get_single_value(df, 'Precisao')
    if precision != 'Unidade' and df.shape[0] > 1:
        print(f'get_xlsx_shares_df: found Precisao "{precision}"')
    #endregion
    epd.drop_columns(df, ['Acoes Ordinarias Tesouraria', 'Acoes Preferenciais Tesouraria', 'Total Tesouraria', 'Precisao'])
    epd.rename_columns(df, {'Acoes Ordinarias Capital Integralizado': share_columns[0],
                            'Acoes Preferenciais Capital Integralizado': share_columns[1],
                            'Total Capital Integralizado': share_columns[2]})
    epd.print_df(df)
    return df

def get_xml_shares_df(file_names: list, zip_file, share_columns: list):
    file_name = get_file_name('xml', file_names)
    xml_content = get_file_content(zip_file, file_name)
    xml_string = xml_content.decode("UTF-8")
    xml_fields = ['Ordinarias',
                  'Preferenciais',
                  'QtdeTotalAcoes']
    values = [find_in_xml(xml_string, tag)[0] for tag in xml_fields]
    df = shares_list_to_df(values, share_columns)
    epd.print_df(df)
    return df

def do():
    years = [2021]
    cnpjs = [r'00.080.671/0001-00', r'00.545.378/0001-70', r'00.272.185/0001-93']

    zip_files = download_zips(['ITR', 'DFP'], years)
    itrs, dfps = [], []
    for year in years:
        itrs.append(get_data(zip_files, 'ITR', year, '', '', f'{"ITR".lower()}_cia_aberta'))
        dfps.append(get_data(zip_files, 'DFP', year, '', '', f'{"DFP".lower()}_cia_aberta'))

    df = get_dataframe(itrs, dfps, cnpjs)
    epd.print_df(df)
    share_columns = ['ON', 'PN', 'TOTAL']
    link = epd.get_value(df, 5, 'LINK_DOC')
    # epd.parallel_apply_column(df, 'FILE', 'LINK_DOC', download_link, [], cpu_count())
    zip_file = download_link(link)
    file_names = list_zip_filenames(zip_file)
    if extensions_in_filename_list(['itr', 'dfp'], file_names):
        df2 = get_weird_shares_df(file_names, zip_file, share_columns)
    elif extensions_in_filename_list(['xlsx'], file_names):
        df2 = get_xlsx_shares_df(file_names, zip_file, share_columns)
    elif extensions_in_filename_list(['xml'], file_names):
        df2 = get_xml_shares_df(file_names, zip_file, share_columns)
    return None