from source.wrapper import *
import time as t

# TODO
# get shares & ticker
# de-para sql?

# TODO REF
# make parallel https://stackoverflow.com/a/55399775
# https://queirozf.com/entries/pandas-dataframes-apply-examples#apply-function-in-parallel
# Cython https://idlecoding.com/from-python-to-cython/
# https://qr.ae/pvjhFV

if __name__ == "__main__":
    try:
        start = t.time()
        # statements = ['DRE', 'DFC_MI', 'BPA', 'BPP']
        generate_statements(statement='', years_back=5, multi_core=True)
        print(f"done after - {t.time() - start}s")
    except Exception as e:
        print(f"error: {e}")
