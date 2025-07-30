import dask.dataframe as dd
import sys
import os

def contar_registros(ruta_archivo):

    columnas_usar = ["record_id"]  # Solo una columna ligera para contar

    try:
        print(f"\nCargando archivo {ruta_archivo}...")
        ddf = dd.read_csv(
            ruta_archivo,
            usecols=columnas_usar,
            sep=",",
            dtype={"record_id": "str"},
            blocksize="256MB",
        )

        print("Contando registros (paciencia para archivos grandes)...")
        total_registros = ddf.shape[0].compute()

        print(f"\n✅ Análisis completado:")
        print(f"Archivo analizado: {ruta_archivo}")
        print(f"Total de registros: {total_registros:,}")

    except Exception as e:
        print(f"\n❌ Ocurrió un error inesperado: {str(e)}")

if __name__ == "__main__":
    print("=== Contador de registros en archivos CSV grandes ===")

    if len(sys.argv) < 2:
        print("Uso: python csv_count_registers.py <nombre_del_archivo.csv>")
        sys.exit(1)

    archivo = sys.argv[1]
    contar_registros(archivo)
