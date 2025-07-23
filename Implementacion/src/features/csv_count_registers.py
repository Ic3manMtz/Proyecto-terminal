import dask.dataframe as dd

def contar_registros():
    """
    Solicita al usuario el nombre del archivo y cuenta los registros usando Dask.
    """
    
    # 1. Solicitar el nombre del archivo al usuario
    ruta_archivo = input("Por favor, ingresa el nombre del archivo CSV a analizar: ").strip()
    
    # Verificar si el usuario ingresó algo
    if not ruta_archivo:
        print("❌ No se ingresó ningún nombre de archivo. Saliendo...")
        return
    
    # Configuración de columnas a usar (ligeras para minimizar memoria)
    columnas_usar = ["record_id"]  # Usamos una columna ligera para contar
    
    try:
        # 2. Cargar solo una columna (para minimizar memoria)
        print(f"\nCargando archivo {ruta_archivo}...")
        ddf = dd.read_csv(
            ruta_archivo,
            usecols=columnas_usar,  # Solo cargar columnas necesarias
            sep=",",               # Ajusta el delimitador si es necesario
            dtype={"record_id": "str"},  # Corregido para coincidir con columnas_usar
            blocksize="256MB",     # Tamaño de bloque (ajusta según tu RAM)
        )
        
        # 3. Contar registros
        print("Contando registros (paciencia para archivos grandes)...")
        total_registros = ddf.shape[0].compute()
        
        # 4. Mostrar resultados
        print(f"\n✅ Análisis completado:")
        print(f"Archivo analizado: {ruta_archivo}")
        print(f"Total de registros: {total_registros:,}")
        
    except FileNotFoundError:
        print(f"\n❌ Error: No se encontró el archivo '{ruta_archivo}'")
    except Exception as e:
        print(f"\n❌ Ocurrió un error inesperado: {str(e)}")

if __name__ == "__main__":
    print("=== Contador de registros en archivos CSV grandes ===")
    contar_registros()