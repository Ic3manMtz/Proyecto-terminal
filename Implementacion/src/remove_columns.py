import dask.dataframe as dd

# Columnas que sí vamos a conservar
columnas_deseadas = [
    'identifier',
    'timestamp',
    'device_lat',
    'device_lon',
    'device_horizontal_accuracy',
    'record_id',
    'time_zone_name'
    ]

# Cargar solo las columnas necesarias
df = dd.read_csv('Mobility_Data.csv', usecols=columnas_deseadas)

# Guardar el resultado
df.to_csv('Mobility_Data_Slim.csv', index=False, single_file=True, encoding='utf-8-sig')