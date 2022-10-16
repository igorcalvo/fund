
def first_or_default(data_list: list, lamba_function):
    obj = next((o for o in data_list if lamba_function), None)
    return obj