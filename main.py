from source.wrapper import *
from time import time
from sys import exc_info
from os import path

# TODO
# run without verify false and then with it
# simplify imports
# import/export google sheets
# de-para sql?

# TODO if better performance needed
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
        # generate_statements(statement='', years_back=5, export_raw_data=False, multi_core=True, print_duplicates=True)
        # export_company_info(year=0, export_xlsx=True)
        get_share_values(cnpjs=[], years_back=2021, multi_core=False)
        print(f"done after - {round(time() - start, 1)}s")
    except Exception as e:
        e_type, e_obj, e_tb = exc_info()
        e_filename = path.split(e_tb.tb_frame.f_code.co_filename)[1]
        print(f"\n\n{e_obj} at line {e_tb.tb_lineno} of {e_filename}\n{e}")
