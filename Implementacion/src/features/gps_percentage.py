import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import numpy as np
import pandas as pd  # Importación faltante

# Solicitar ruta del archivo CSV
file_path = input("Ingresa la ruta del archivo CSV: ")

# Leer el archivo CSV con Dask
try:
    ddf = dd.read_csv(
        file_path,
        assume_missing=True,
        dtype={'device_horizontal_accuracy': float}
    )
except Exception as e:
    print(f"Error al leer el archivo: {e}")
    exit()

# Verificar si la columna existe
columna = 'device_horizontal_accuracy'
if columna not in ddf.columns:
    print(f"Error: La columna '{columna}' no existe en el archivo CSV")
    exit()

# Definir los rangos de precisión
rangos = {
    "GPS puro (satelital)": (1, 20),
    "A-GPS (Asistido por red)": (5, 50),
    "Triangulación por WiFi/redes móviles": (20, 500),
    "Geolocalización por IP": (1000, 5000)
}

# Función para procesamiento por bloques
def procesar_bloque(bloque):
    # Limpieza: eliminar NaNs y valores infinitos
    bloque_limpio = bloque.dropna(subset=[columna])
    bloque_limpio = bloque_limpio[np.isfinite(bloque_limpio[columna])]
    
    # Contar ocurrencias en cada rango
    resultados = {}
    for nombre, (min_val, max_val) in rangos.items():
        mask = (bloque_limpio[columna] >= min_val) & (bloque_limpio[columna] <= max_val)
        resultados[nombre] = mask.sum()
    
    resultados['__total_valido'] = len(bloque_limpio)
    return pd.Series(resultados)  # Ahora pd está definido

# Procesar en paralelo con map_partitions
with ProgressBar():
    resultados = ddf.map_partitions(
        procesar_bloque,
        meta={k: 'int64' for k in list(rangos.keys()) + ['__total_valido']}
    ).compute()

# Calcular totales globales
total_valido = resultados['__total_valido'].sum()
conteos_globales = {k: resultados[k].sum() for k in rangos.keys()}

# Calcular porcentajes
porcentajes = {k: (v / total_valido) * 100 for k, v in conteos_globales.items()}

# Mostrar resultados
print("\nResultados:")
print("----------------------------------------")
for nombre, porcentaje in porcentajes.items():
    print(f"{nombre}: {porcentaje:.2f}%")
print("----------------------------------------")
print(f"Total de registros analizados: {total_valido}")
print(f"Registros descartados (NaNs/infinitos): {len(ddf) - total_valido}")
print("\nNota: Los rangos pueden solaparse, un registro puede contar en múltiples categorías")