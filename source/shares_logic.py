from ez_pandas.ez_pandas import append_df_list, append_dfs, drop_columns, sort, redefine_index, drop_columns, get_single_value, get_value
from .parser import find_in_xml, get_file_content_bytes, string_to_file, list_zip_filenames_bytes, get_file_content, list_zip_filenames, df_from_content_xlsx
from .cvm import download_zips, get_data, download_link
from pandas import DataFrame, concat
from numpy import array_split
from itertools import repeat
from multiprocessing import cpu_count, Pool

enconding = "ISO-8859-1"

def extensions_in_filename_list(extensions: list, file_names: list) -> bool:
    return any([file_name for file_name in file_names if file_name.split('.')[-1] in extensions])

def get_file_name(extension: str, file_names: list):
    file_name_list = [file_name for file_name in file_names if file_name.split('.')[-1] == extension]
    if len(file_name_list) > 1:
        if extension != 'xml':
            print(f'get_file_name: found {len(file_name_list)} files for ext: "{extension}" in {file_names} - returning "{file_name_list[0]}"')
        return file_name_list[0]
    elif len(file_name_list) == 0:
        print(f'get_file_name: no files for ext: "{extension}" in {file_names}')
        return None
    else:
        return file_name_list[0]

def get_filename_containing(file_name: str, file_names: list):
    file_name_list = [filename for filename in file_names if file_name in filename]
    if len(file_name_list) > 1:
        print(f'get_filename_containing: found duplicate files for "{file_name}" in {file_names} - returning "{file_name_list[0]}"')
        return file_name_list[0]
    elif len(file_name_list) == 0:
        print(f'get_filename_containing: no files matching the name "{file_name}" were found in {file_names}')
        return None
    else:
        return file_name_list[0]

def get_dataframe(itrs: list, dfps: list, cnpjs: list = []) -> DataFrame:
    itr_df = append_df_list(itrs)
    dfp_df = append_df_list(dfps)
    df = append_dfs(itr_df, dfp_df)
    if len(cnpjs) > 0:
        df = where_column_regex(df, 'CNPJ_CIA', '|'.join(cnpjs))
    df = df.groupby(['CNPJ_CIA', 'DT_REFER'], as_index=False).max()
    drop_columns(df, ['ID_DOC', 'DT_RECEB', 'CD_CVM'])
    sort(df, ['CNPJ_CIA', 'DT_REFER'], [1, 1])
    redefine_index(df)
    return df

def shares_list_to_df(shares_values_list: list, share_names_list: list) -> DataFrame:
    df = DataFrame([shares_values_list])
    df.columns = share_names_list
    return df

def get_weird_shares_values(file_names: list, zip_file) -> list:
    extension = 'itr' if extensions_in_filename_list(['itr'], file_names) else 'dfp'
    weird_file_name = get_file_name(extension, file_names)
    content = get_file_content(zip_file, weird_file_name)
    file_names_from_weird = list_zip_filenames_bytes(content)
    file_name = get_filename_containing('ComposicaoCapital', file_names_from_weird)
    xml = get_file_content_bytes(content, file_name)
    xml_string = xml.decode(enconding)
    xml_fields = ['QuantidadeAcaoOrdinariaCapitalIntegralizado',
                  'QuantidadeAcaoPreferencialCapitalIntegralizado',
                  'QuantidadeTotalAcaoCapitalIntegralizado',
                  'QuantidadeAcaoOrdinariaTesouraria',
                  'QuantidadeAcaoPreferencialTesouraria',
                  'QuantidadeTotalAcaoTesouraria']
    values = [int(find_in_xml(xml_string, tag)[0]) for tag in xml_fields]
    return values

def check_precision(df: DataFrame):
    precision = get_single_value(df, 'Precisao')
    if precision != 'Unidade' and df.shape[0] > 1:
        raise Exception(f'get_xlsx_shares_df: found Precisao "{precision}"')

def get_xlsx_shares_values(file_names: list, zip_file) -> list:
    file_name = get_file_name('xlsx', file_names)
    content = get_file_content(zip_file, file_name)
    df = df_from_content_xlsx(content, 'Composicao Capital')
    check_precision(df)

    columns_to_drop = ['Precisao']
    possible_drops = ['Ultimo Exercicio', 'Trimestre Atual']
    for to_drop in possible_drops:
        if to_drop in df.columns:
            columns_to_drop.append(to_drop)
    drop_columns(df, columns_to_drop)

    if len(df.columns) > 6:
        raise Exception(f"get_xlsx_shares_values: got more than 6 columns: {list(df.columns)}")

    values = list(df.values[0])
    values = [int(v) if v is not None else 0 for v in values]
    return values

def get_xml_shares_values(file_names: list, zip_file) -> list:
    file_name = get_file_name('xml', file_names)
    xml_content = get_file_content(zip_file, file_name)
    xml_string = xml_content.decode(enconding)
    xml_fields = ['Ordinarias',
                  'Preferenciais',
                  'QtdeTotalAcoes']
    values = [find_in_xml(xml_string, tag) for tag in xml_fields]
    values = [int(values[row][col] if values[row][col] not in ('', None) else 0) for col in range(2) for row in range(3)]
    return values

def get_shares_single_core(df: DataFrame, share_columns: list) -> DataFrame:
    shares = []
    for index in df.index.tolist():
        index_translation = df.index.tolist()[0]
        link = get_value(df, index - index_translation, 'LINK_DOC')
        zip_file = download_link(link)
        file_names = list_zip_filenames(zip_file)

        if extensions_in_filename_list(['itr', 'dfp'], file_names):
            row = get_weird_shares_values(file_names, zip_file)
        elif extensions_in_filename_list(['xlsx'], file_names):
            row = get_xlsx_shares_values(file_names, zip_file)
        elif extensions_in_filename_list(['xml'], file_names):
            row = get_xml_shares_values(file_names, zip_file)
        row.insert(0, link)
        shares.append(row)
    shares_df = DataFrame(shares, columns=share_columns)
    return shares_df

def get_shares_multi_core(df: DataFrame, share_columns: list) -> DataFrame:
    cores = cpu_count() - 1
    df_chunks = array_split(df, cores)
    with Pool(cores) as pool:
        shares_df = concat(pool.starmap(get_shares_single_core, zip(df_chunks, repeat(share_columns))), ignore_index=True)
    return shares_df
