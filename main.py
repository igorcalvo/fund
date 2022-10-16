from cvm import *

docs = list_folders()
zip_files = list_files(docs[0])
files = list_zip_filenames(docs[0], zip_files[0])
get_file_content(docs[0], zip_files[0], files[0])
