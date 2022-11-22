# fca_cia_aberta_valor_imobiliario_2022.csv -> tickers & cpf
# fca_cia_aberta_geral_2022.csv -> name & cpf
# dfp_cia_aberta_2022.csv -> LINK_DOC -> unzip ->

from .cvm import download_zips, get_data
from .date_utils import today
import ez_pandas.ez_pandas as epd
import pandas as pd

def get_fca(year: int):
    doc = 'FCA'
    files = download_zips([doc], [year])
    return files

def get_dfs(files: dict, year: int):
    ticker_df = get_data(files, 'FCA', year, 'valor_mobiliario', '')
    comp_df = get_data(files, 'FCA', year, 'geral', '')
    return comp_df, ticker_df

def cleanup_company_df(df: pd.DataFrame) -> pd.DataFrame:
    to_drop = ['Data_Referencia', 'Versao', 'ID_Documento', 'Data_Constituicao', 'Data_Registro_CVM', 'Categoria_Registro_CVM',
               'Data_Categoria_Registro_CVM', 'Situacao_Registro_CVM', 'Data_Situacao_Registro_CVM', 'Pais_Origem', 'Pais_Custodia_Valores_Mobiliarios',
               'Situacao_Emissor', 'Data_Situacao_Emissor', 'Especie_Controle_Acionario', 'Data_Especie_Controle_Acionario',
               'Dia_Encerramento_Exercicio_Social', 'Mes_Encerramento_Exercicio_Social', 'Data_Alteracao_Exercicio_Social', 'Pagina_Web']
    df = df.drop(to_drop, axis="columns")

    reoder_columns = ['Nome_Empresarial', 'CNPJ_Companhia', 'Codigo_CVM', 'Setor_Atividade', 'Descricao_Atividade',
                      'Nome_Empresarial_Anterior', 'Data_Nome_Empresarial']
    df = df[reoder_columns]
    df = df.sort_values(['CNPJ_Companhia'])
    return df

def cleanup_ticker_df(df: pd.DataFrame) -> pd.DataFrame:
    to_drop = ['Data_Referencia', 'Versao', 'ID_Documento', 'Classe_Acao_Preferencial', 'Sigla_Entidade_Administradora',
               'Entidade_Administradora', 'Data_Inicio_Negociacao', 'Data_Fim_Negociacao', 'Segmento', 'Data_Inicio_Listagem', 'Data_Fim_Listagem']
    df = df.drop(to_drop, axis="columns")

    reoder_columns = ['CNPJ_Companhia', 'Codigo_Negociacao', 'Composicao_BDR_Unit', 'Valor_Mobiliario', 'Sigla_Classe_Acao_Preferencial', 'Mercado']
    df = df[reoder_columns]

    df = df.sort_values(['CNPJ_Companhia'])
    return df

def do():
    pass
