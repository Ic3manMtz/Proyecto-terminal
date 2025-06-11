import dask.dataframe as dd

# Configuración inicial 
ruta_archivo = "Mobility_Data_Slim.csv" 
columnas_usar = ["record_id"]  # Usamos una columna ligera para contar

# 1. Cargar solo una columna (para minimizar memoria)
ddf = dd.read_csv(
    ruta_archivo,
    usecols=columnas_usar,  # Solo cargar columnas necesarias
    sep=",",               # Ajusta el delimitador si es necesario
    dtype={"id": "str"},   # Tipo de dato para evitar warnings
    blocksize="256MB",     # Tamaño de bloque (ajusta según tu RAM)
)

# 2. Contar registros 
print("Contando registros (paciencia para archivos grandes)...")
total_registros = ddf.shape[0].compute() 

# 3. Mostrar resultados
print(f"✅ Total de registros: {total_registros:,}")