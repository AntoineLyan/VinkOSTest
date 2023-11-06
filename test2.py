import pandas as pd

df = pd.read_csv('archivostxt/report_7.txt', sep=',')

# Crea la nueva columna 'dummy_int' con valores de 1
df['dummy_int'] = 1

# Guarda el DataFrame modificado en un nuevo archivo CSV
df.to_csv('archivostxt/report_6.txt', index=False, sep=',')