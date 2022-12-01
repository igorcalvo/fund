from source.wrapper import *
from time import time
from sys import exc_info
from os import path
from source.google_sheets import do

# TODO
# de-para sql?
# simplify imports
# clean up TODOs

# TODO if better performance needed
# google sheets docs
#   https://developers.google.com/sheets/api/quickstart/python
#   https://developers.google.com/sheets/api/guides/values
# requests
#   https://stackoverflow.com/a/62599037
# cpython .cpy
# parallel -> year by year
#   https://stackoverflow.com/a/55399775
#   https://queirozf.com/entries/pandas-dataframes-apply-examples#apply-function-in-parallel
# Cython
#   https://idlecoding.com/from-python-to-cython/
#   https://qr.ae/pvjhFV

if __name__ == "__main__":
    try:
        start = time()
        statements = ['DRE', 'DFC_MI', 'BPA', 'BPP']
        generate_statements(statement='', years_back=5, export_raw_data=False, multi_core=True, print_duplicates=True)
        # export_company_info(year=0, export_xlsx=True)
        # get_share_values(cnpjs=[], years_back=2021, multi_core=False)
        print(f"done after - {round(time() - start, 1)}s")
    except Exception as e:
        raise(e)
