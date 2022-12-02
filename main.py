from source.wrapper import generate_statements, company_info, share_values, elapsed_time_message
from time import time

# TODO
# de-para BP

if __name__ == "__main__":
    try:
        start = time()
        statements = ['DRE', 'DFC_MI', 'BPA', 'BPP']
        generate_statements(statement='', years_back=5, export_raw_data=False, multi_core=True, print_duplicates=False)
        # company_info(year=0, export_xlsx=True)
        # share_values(cnpjs=[], years_back=2021, multi_core=True)
        print(elapsed_time_message(start))
    except Exception as e:
        raise(e)
