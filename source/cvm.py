from .parser import df_from_content, list_zip_filenames, get_file_content
from requests import get
from bs4 import BeautifulSoup as bs
from pandas import DataFrame

base_url = r"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/"

def list_cvm_links(url: str) -> list:
    page = get(url).text
    soup = bs(page, 'html.parser')
    links = soup.find('pre').find_all('a')
    strings = [l.text for l in links]
    return strings

def list_folders(url: str = base_url) -> list:
    strings = list_cvm_links(url)
    return strings[1:]

def list_files(doc: str) -> list:
    path = get_file_path(doc)
    strings = list_folders(path)
    return strings

def get_file_path(doc: str, filename: str = '') -> str:
    return f"{base_url}{doc}/DADOS/{filename}" if len(filename) > 0 else f"{base_url}{doc}/DADOS"

def get_zip_file_name(doc: str, year: int) -> str:
    return f"{doc.lower()}_cia_aberta_{year}.zip"

def download_zip(doc: str, year: int):
    print(f"{doc} - {year} - downloading")
    zip_filename = get_zip_file_name(doc, year)
    path = get_file_path(doc, zip_filename)
    zip_file = get(path)
    return zip_file

def get_file_dict_key(doc: str, year: int) -> str:
    return f'{doc}_{year}'

def download_zips(docs: list, years: list) -> dict:
    to_download = [(doc, year) for doc in docs for year in years]
    files = {}
    for download in to_download:
        files.update({get_file_dict_key(download[0], download[1]): download_zip(download[0], download[1])})
    return files

def get_data(files: dict, doc: str, year: int, statement: str, con: str = 'con', exact_filename: str = '') -> DataFrame:
    zip_file = files[get_file_dict_key(doc, year)]
    file_names = list_zip_filenames(zip_file)
    file_name = next((f for f in file_names if f"{statement}_{con}" in f), None) if exact_filename == '' else f'{exact_filename}_{year}.csv'
    content = get_file_content(zip_file, file_name)
    df = df_from_content(content)
    return df

def download_link(link: str):
    # headers = {"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}
    # headers = {"User-Agent": "Mozilla/5.0 (X11; U; Linux i686 (x86_64); en-GB; rv:1.9.0.1) Gecko/2008070206 Firefox/3.0.1"}
    # return s.get(link, headers=headers, timeout=(3, None), verify=False)
    return get(link)