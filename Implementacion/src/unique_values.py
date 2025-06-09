import dask.dataframe as dd

# Configuración
ruta_archivo = "Mobility_Data.csv"  # Cambia la ruta
columna_objetivo = "identifier_type"  # Columna a analizar

# 1. Cargar SOLO la columna necesaria (optimiza memoria)
ddf = dd.read_csv(
    ruta_archivo,
    usecols=[columna_objetivo],  # Solo cargar esta columna
    sep=",",                    # Ajusta el delimitador si es necesario
    dtype={columna_objetivo: "str"},  # Fuerza tipo string
    blocksize="64MB"            # Bloques pequeños para mayor eficiencia
)

# 2. Obtener valores únicos (operación diferida)
valores_unicos = ddf[columna_objetivo].unique()

# 3. Ejecutar y mostrar resultados
print(f"Calculando valores únicos de '{columna_objetivo}'...")
resultados = valores_unicos.compute()  

print("\nValores únicos encontrados:")
print(resultados)  
print(f"\n✅ Total: {len(resultados)} valores distintos")