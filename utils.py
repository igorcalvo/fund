def first_or_default(list: list, lamba_function):
    obj = next((o for o in list if lamba_function), None)
    return obj