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
# pd.set_option('mode.chained_assignment', None)
# pd.options.mode.chained_assignment = None
df3['DT_REFER2'] = df3.apply(lambda row: quarter_dates(row['DT_REFER']), axis=1)
print(df3.head())
df3 = calculate_values(df3)

save_xls([df3, df1, df2], ['DRE', 'DFP', 'ITR'], 'C:/Users/Igor/Desktop', 'test.xlsx')