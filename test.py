import pandas as pd

df = pd.read_csv("archivostxt/report_7.txt", sep=',')

print(df['Links'].value_counts())

# non_null_data = df['Opens'].dropna()
# print(non_null_data.value_counts())

# non_null_data = df[df['Fecha click'] != '-']['Fecha open']