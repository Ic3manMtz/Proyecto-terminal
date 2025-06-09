import dask.dataframe as dd

# 1. Especifica la ruta del archivo CSV
ruta_archivo = "Mobility_Data.csv"  

# 2. Carga el archivo con Dask (no carga todo en RAM)
ddf = dd.read_csv(
    ruta_archivo,
    encoding="utf-8",  
    sep=",",           
    dtype="object",    
)

# 2. Obtener los nombres de las columnas
columnas = ddf.columns.tolist()

# 3. Mostrar nombres de columnas + 2 datos de ejemplo por cada una
print("Columnas y 2 ejemplos por cada una:\n")
for col in columnas:
    # Tomar 2 muestras de la columna actual (sin procesar todo el archivo)
    ejemplos = ddf[col].head(2).values.tolist()  # Convierte a lista
    print(f"- {col}: {ejemplos}")