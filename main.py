from cvm import *
from logic import *
import pandas as pd

# docs = list_folders()
# zip_files = list_files(docs[0])
# files = list_zip_filenames(docs[0], zip_files[0])
# content = get_file_content(docs[0], zip_files[0], files[0])
# print(docs)
# TODO
# encapsulate
# parallel
# more years
# more statements
year = 2021
statement = "DRE"
print(f'downloading for {year} - {statement} - DFP')
str_list1 = get_data('DFP/', year, statement)
print(f'downloading for {year} - {statement} - ITR')
str_list2 = get_data('ITR/', year, statement)

df1 = pd.DataFrame(str_list2[1:], columns=str_list2[0])
df2 = pd.DataFrame(str_list1[1:], columns=str_list1[0])

print(f'transforming {year} - {statement} - DFP')
str_list1 = prepare_str_list(str_list1, year, False)
print(f'transforming {year} - {statement} - ITR')
str_list2 = prepare_str_list(str_list2, year, True)

print(f'unifying {year} - {statement}')
str_list3 = str_list1 + str_list2[1:]

str_list3 = calculate_values(str_list3)
# write_csv('after.csv', str_list3)



# print(df3.head())
# df3 = calculate_values(df3)
#
print(f'writing to excel')
df3 = pd.DataFrame(str_list3[1:], columns=str_list3[0])
save_xls([df3, df1, df2], ['DRE', 'DFP', 'ITR'], '', 'dre_2021.xlsx')