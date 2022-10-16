from cvm import *
from parser import *

docs = list_folders()
zip_files = list_files(docs[0])
files = list_zip_filenames(docs[0], zip_files[0])
get_file_content(docs[0], zip_files[0], files[0])

print(docs)
year = 2021
statement = "DRE"

df1 = get_data('DFP/', year, statement)
df2 = get_data('ITR/', year, statement)

df1 = prepare_df(df1, year, False)
df2 = prepare_df(df2, year, True)

df3 = pd.concat([df2, df1], ignore_index=True)
# calculate_values(df3)
print(get_dates_list(df3, '64.904.295/0001-03'))
print(get_dates_list(df3, '00.000.000/0001-91'))

# arrumar datas