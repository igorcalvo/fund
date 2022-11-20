from cvm import *
from logic import *
# from ez_pandas.ez_pandas import save_sheets_xlsx, append_dfs
import ez_pandas.ez_pandas as epd
import pandas as pd

# docs = list_folders()
# zip_files = list_files(docs[0])
# files = list_zip_filenames(docs[0], zip_files[0])
# content = get_file_content(docs[0], zip_files[0], files[0])
# print(docs)

# TODO NEXT
# DFC_MI (dfc e itr)
# BPA
# BPP
# e DRE

# TODO
# https://qr.ae/pvjhFV
# https://queirozf.com/entries/pandas-dataframes-apply-examples#apply-example
# encapsulate
# parallel
# more years
# more statements
# de-para sql?

# TODO REF
# make parallel https://stackoverflow.com/a/55399775
# Cython https://idlecoding.com/from-python-to-cython/

if __name__ == "__main__":
    year = 2020
    statement = "DRE"
    print(f'downloading for {year} - {statement} - ITR')
    itr_df = get_data('ITR/', year, statement)
    print(f'downloading for {year} - {statement} - DFP')
    dfp_df = get_data('DFP/', year, statement)

    print(f'transforming {year} - {statement} - ITR')
    itr_df = prepare_df(itr_df, year, True)
    print(f'transforming {year} - {statement} - DFP')
    dfp_df = prepare_df(dfp_df, year, False)

    print(f'unifying {year} - {statement}')
    dre_df = epd.append_dfs(itr_df, dfp_df)
    dre_before = dre_df.copy(deep=True)

    dre_df = calculate_values(dre_df)

    print(f'writing to excel')
    epd.save_sheets_xlsx([dre_df, dre_before, dfp_df, itr_df], ['DRE', 'DRE_Before', 'DFP', 'ITR'], f'dre_{year}', '')