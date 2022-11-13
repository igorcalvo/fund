from datetime import datetime
from utils import *

def drop_column_from_str_list(str_list: list, header: str) -> list:
    index = str_list[0].index(header)
    for row in str_list:
        row.pop(index)
    return str_list

def str_list_remove_all_but_last(str_list: list):
    header = 'ORDEM_EXERC'
    index = str_list[0].index(header)
    removed_rows = []
    for idx, row in enumerate(str_list):
        if row[index] != 'ÚLTIMO':
            removed_rows.append(idx)
    removed_rows.sort(reverse=True)
    if 0 in removed_rows:
        removed_rows.remove(0)
    for removed in removed_rows:
        str_list.pop(removed)
    return str_list

def str_list_update_accounts(str_list: list):
    header = 'VL_CONTA'
    scale = 'ESCALA_MOEDA'
    value_index = str_list[0].index(header)
    scale_index = str_list[0].index(scale)
    for idx, row in enumerate(str_list[1:]):
        if row[scale_index] == 'MIL':
            row[value_index] = str(float(row[value_index]) * 1000)
    return str_list

def str_list_drop_not_date(str_list: list, year):
    header = 'DT_INI_EXERC'
    index = str_list[0].index(header)
    removed_rows = []
    for idx, row in enumerate(str_list[1:]):
        if row[index] != f'{year}-01-01':
            removed_rows.append(idx)
    removed_rows.sort(reverse=True)
    if 0 in removed_rows:
        removed_rows.remove(0)
    for removed in removed_rows:
        str_list.pop(removed)
    return str_list


def prepare_str_list(str_list: list, year: float, is_itr: bool):
    str_list = drop_column_from_str_list(str_list, 'MOEDA')
    str_list = drop_column_from_str_list(str_list, 'VERSAO')

    str_list = str_list_remove_all_but_last(str_list)
    str_list = str_list_update_accounts(str_list)
    str_list = drop_column_from_str_list(str_list, 'ESCALA_MOEDA')

    if is_itr:
        str_list = str_list_drop_not_date(str_list, year)

    return str_list

def str_list_update_ref_date(str_list: list):
    header = 'DT_REFER'
    new_header = 'DT_REFER2'
    index = str_list[0].index(header)

    str_list[0].append(new_header)
    for idx, row in enumerate(str_list[1:]):
        new_value = quarter_dates(row[index])
        row.append(new_value)
    return str_list

def str_list_select_column(str_list: list, header: str, remove_duplicates: bool = True):
    index = str_list[0].index(header)
    slice = [row[index] for row in str_list[1:]]

    if not remove_duplicates:
        return slice.copy()

    result = list(set(slice))
    result.sort()
    return result

def date_string(day: str | int, month: str | int, year: str | int, year_first: bool = False):
    day = f'0{day}' if isinstance(day, int) and day < 10 else day
    month = f'0{month}' if isinstance(day, int) and month < 10 else month
    return f'{day}-{month}-{year}' if not year_first else f'{year}-{month}-{day}'

def quarter_dates(date: str, year_first: bool = False):
    year_index = [len(d) for d in date.split('-')].index(4)
    year = int(date.split('-')[year_index])
    month = int(date.split('-')[1])

    if month <= 3:
        return date_string(31, 3, year, year_first)
    elif month <= 6:
        return date_string(30, 6, year, year_first)
    elif month <= 9:
        return date_string(30, 9, year, year_first)
    else:
        return date_string(31, 12, year, year_first)

def get_previous_date(date, year_first: bool = False):
    year_index = [len(d) for d in date.split('-')].index(4)
    year = int(date.split('-')[year_index])

    if date == date_string(30, 6, year, year_first):
        return date_string(31, 3, year, year_first)
    elif date == date_string(30, 9, year, year_first):
        return date_string(30, 6, year, year_first)
    elif date == date_string(31, 12, year, year_first):
        return date_string(30, 9, year, year_first)
    else:
        raise Exception(f"get_previous_date: got {date} for {year}")

def find_indexes_for_value(str_list: list, header: str, value):
    index = str_list[0].index(header)
    indexes = [idx + 1 if row[index] == value else None for (idx, row) in enumerate(str_list[1:])]
    indexes = set(indexes)
    if None in indexes:
        indexes.remove(None)
    indexes = list(indexes)
    indexes.sort()
    return indexes

def find_index(str_list: list, header_value_pairs: list, allow_multiple_values: bool):
    indexes = []
    for (header, value) in header_value_pairs:
        new_indexes = find_indexes_for_value(str_list, header, value)
        if len(indexes) == 0:
            indexes = new_indexes.copy()
        indexes = list(set(indexes) & set(new_indexes))

    if len(indexes) > 1 and not allow_multiple_values:
        # raise Exception(f"find_index: found more than one index for {header_value_pairs}")
        print(f'find_index: duplicate values for: {header_value_pairs}')
        return indexes[-1]
    # SINQIA S.A. 04.065.791/0001-99
    return indexes[0] if not allow_multiple_values else indexes

def str_list_select_column_with_filters(str_list: list, header: str, header_value_pairs: list, remove_duplicates: bool = True):
    indexes = find_index(str_list, header_value_pairs, True)
    index = str_list[0].index(header)
    slice = [row[index] for row in [str_list[idx] for idx in indexes]]

    if not remove_duplicates:
        return slice.copy()

    result = list(set(slice))
    result.sort()
    return result

def str_list_update_value(str_list: list, year_first: bool):
    str_list[0].append('VL_CONTA2')
    for row in str_list[1:]:
        row.append('null')

    vl_conta_index = str_list[0].index('VL_CONTA')
    cnpjs = str_list_select_column(str_list, 'CNPJ_CIA')
    print(f'exporting csv')
    # TODO
    # make parallel https://stackoverflow.com/a/55399775
    for cnpj_idx, cnpj in enumerate(cnpjs):
        print(f'{round(100.0 * cnpj_idx / len(cnpjs), 4)}%')
        accounts = str_list_select_column_with_filters(str_list, 'CD_CONTA', [('CNPJ_CIA', cnpj)])
        for account in accounts:
            ref_dates = str_list_select_column_with_filters(str_list, 'DT_REFER2', [('CNPJ_CIA', cnpj), ('CD_CONTA', account)])
            ref_dates.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d" if year_first else "%d-%m-%Y"))
            for date_idx, ref_date in enumerate(ref_dates):
                if date_idx == 0:
                    value_headers = [('CNPJ_CIA', cnpj), ('CD_CONTA', account), ('DT_REFER2', ref_date)]
                    value_row_index = find_index(str_list, value_headers, False)
                    value = float(str_list[value_row_index][vl_conta_index])
                    str_list[value_row_index][-1] = str(value)
                else:
                    value1_headers = [('CNPJ_CIA', cnpj), ('CD_CONTA', account), ('DT_REFER2', ref_date)]
                    value1_row_index = find_index(str_list, value1_headers, False)
                    value1 = float(str_list[value1_row_index][vl_conta_index])
                    str_list[value1_row_index][-1] = str(value1 - value)
                    value = value1
                # value0_headers = [('CNPJ_CIA', cnpj), ('CD_CONTA', account), ('DT_REFER2', get_previous_date(ref_date))]
                # value0_row_index = find_index(str_list, value0_headers) + 1
                # value0 = 0 if int(ref_date.split('-')[1]) == 3 else float(str_list[value0_row_index][vl_conta_index])
                # value1_headers = value0_headers.copy()
                # value1_headers[2] = ('DT_REFER2', ref_date)
                # value1_row_index = find_index(str_list, value1_headers) + 1
                # value1 = float(str_list[value1_row_index][vl_conta_index])
                # str_list[value1_row_index][-1] = value1 - value0
    return str_list

def calculate_values(str_list):
    print(f'updating dates')
    str_list = str_list_update_ref_date(str_list)
    print(f'updating values')
    str_list = str_list_update_value(str_list, False)
    return str_list