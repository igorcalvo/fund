from source.wrapper import *
from source.shares_logic import *
import time as t

# TODO
# get shares & ticker
#   integrate cpfs & fianl df
#   handle exaeptions
#   parallelize
#   run more
# import/export google sheets
# de-para sql?

# TODO if better performance needed
# cpython .cpy
# parallel -> year by year
#   https://stackoverflow.com/a/55399775
#   https://queirozf.com/entries/pandas-dataframes-apply-examples#apply-function-in-parallel
# Cython
#   https://idlecoding.com/from-python-to-cython/
#   https://qr.ae/pvjhFV

if __name__ == "__main__":
    # try:
    #     start = t.time()
    #     statements = ['DRE', 'DFC_MI', 'BPA', 'BPP']
    #     generate_statements(statement='', years_back=5, export_raw_data=False, multi_core=True, print_duplicates=True)
    #     # export_company_info(year=0, export_xlsx=True)
    #     print(f"done after - {round(t.time() - start, 1)}s")
    # except Exception as e:
    #     print(f"error: {e}")
    do()
