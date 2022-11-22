from source.wrapper import *
from source.company_info import *
import time as t

# TODO
# fix errors
# get shares & ticker
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

# DRE & DFC_MI
# 31.701.408/0001-14  2022  ATHENA SAÚDE BRASIL SA
# 37.702.340/0001-74  2021  MONTE RODOVIAS S.A.

if __name__ == "__main__":
    # try:
    #     start = t.time()
    #     statements = ['DRE', 'DFC_MI', 'BPA', 'BPP']
    #     generate_statements(statement='', years_back=5, export_raw_data=False, multi_core=True)
    #     export_company_info(year=0)
    #     print(f"done after - {round(t.time() - start, 1)}s")
    # except Exception as e:
    #     print(f"error: {e}")
    do()
