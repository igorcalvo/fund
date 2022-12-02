from source.wrapper import get_share_values, get_company_info, generate_statements, get_elapsed_time_message
from time import time

# TODO
# de-para sql?

if __name__ == "__main__":
    try:
        start = time()
        statements = ['DRE', 'DFC_MI', 'BPA', 'BPP']
        generate_statements(statement='', years_back=5, export_raw_data=False, multi_core=True, print_duplicates=False)
        # get_company_info(year=0, export_xlsx=True)
        # get_share_values(cnpjs=[], years_back=2021, multi_core=True)
        print(get_elapsed_time_message(start))
    except Exception as e:
        raise(e)
