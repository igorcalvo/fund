from datetime import datetime

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

def get_date_month(date: str):
    return int(date.split('-')[1])

def get_date_year(date: str):
    year_index = [len(d) for d in date.split('-')].index(4)
    return int(date.split('-')[year_index])

def get_previous_date(date, year_first: bool = False):
    year = get_date_year(date)

    if date == date_string(30, 6, year, year_first):
        return date_string(31, 3, year, year_first)
    elif date == date_string(30, 9, year, year_first):
        return date_string(30, 6, year, year_first)
    elif date == date_string(31, 12, year, year_first):
        return date_string(30, 9, year, year_first)
    else:
        raise Exception(f"get_previous_date: got {date} for {year}")

def sort_date_list(date_list: list, year_first: bool = False):
    date_list.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d" if year_first else "%d-%m-%Y"))
    return date_list

def today() -> datetime:
    return datetime.now()