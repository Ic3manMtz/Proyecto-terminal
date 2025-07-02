import dask.dataframe as dd
import sys  # Importar módulo para manejar argumentos

print("Exploración inicial de datos con Dask\n")

# Verificar parámetro
if len(sys.argv) < 2:
    print("Error: Debe especificar un archivo CSV")
    sys.exit(1)

# Obtener el nombre del archivo desde el primer argumento
ruta_archivo = sys.argv[1]

# Cargar el archivo con Dask
ddf = dd.read_csv(
    ruta_archivo,
    encoding="utf-8",  
    sep=",",           
    dtype="object",    
)

# Obtener los nombres de las columnas
columnas = ddf.columns.tolist()

# Mostrar nombres de columnas + 2 datos de ejemplo por cada una
print("Columnas y 2 ejemplos por cada una:\n")
for col in columnas:
    ejemplos = ddf[col].head(2).values.tolist()
    print(f"- {col}: {ejemplos}")

input("Presiona Enter para continuar...")