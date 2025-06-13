import dask.dataframe as dd
import pandas as pd
from tqdm import tqdm

# Configuración
archivo_csv = "Mobility_Data.csv"  # Cambia la ruta
columna_objetivo = "device_horizontal_accuracy"  # Columna a analizar

archivo_salida = "valores_unicos.txt"
chunksize = 1_000_000  # Procesar 1M de registros a la vez

# Procesamiento por bloques (chunks)
valores_unicos = set()

# Usamos tqdm para monitorear el progreso (opcional)
for chunk in tqdm(pd.read_csv(archivo_csv, usecols=[columna_objetivo], chunksize=chunksize)):
    valores_unicos.update(chunk[columna_objetivo].dropna().astype(str))  # Ignorar NaN y convertir a string

# Guardar en archivo (1 valor por línea)
with open(archivo_salida, "w", encoding="utf-8") as f:
    f.write("\n".join(sorted(valores_unicos)))  # Ordenados alfabéticamente

# Resultados
print(f"\n✅ Se encontraron {len(valores_unicos):,} valores únicos.")
print(f"📄 Guardados en: {archivo_salida}")

# Opcional: Ver primeros 10 valores
print("\n🔍 Ejemplo de valores únicos:")
print("\n".join(sorted(valores_unicos)[:10]))