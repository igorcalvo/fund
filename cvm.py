import requests, io, zipfile
from bs4 import BeautifulSoup as bs
from contextlib import closing as cl

from utils import *
base_url = r"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/"

def list_cvm_links(url: str):
    page = requests.get(url).text
    soup = bs(page, 'html.parser')
    links = soup.find('pre').find_all('a')
    strings = [l.text for l in links]
    return strings

def list_folders(url: str = base_url):
    strings = list_cvm_links(url)
    return strings[1:]

def get_file_path(doc: str, filename: str = ''):
    return f"{base_url}{doc}/DADOS/{filename}" if len(filename) > 0 else f"{base_url}{doc}/DADOS"

def list_files(doc: str):
    path = get_file_path(doc)
    strings = list_folders(path)
    return strings

def list_zip_filenames(doc: str, zip_filename: str):
    path = get_file_path(doc, zip_filename)
    r = requests.get(path)
    with cl(r), zipfile.ZipFile(io.BytesIO(r.content)) as archive:
        return [f.filename for f in archive.filelist]

def get_file_content(doc: str, zip_filename: str, filename: str):
    path = get_file_path(doc, zip_filename)
    r = requests.get(path)
    with cl(r), zipfile.ZipFile(io.BytesIO(r.content)) as archive:
        file = first_or_default(archive.filelist, lambda a: a.filename == filename)
        content = archive.read(file)
        return content
