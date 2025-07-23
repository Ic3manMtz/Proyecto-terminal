import dask.dataframe as dd

def eliminar_duplicados(input_file, output_file):
    """
    Elimina registros duplicados basados en las columnas 'identifier', 'device_lon' y 'device_lat'.
    
    Args:
        input_file (str): Ruta del archivo CSV de entrada
        output_file (str): Ruta del archivo CSV de salida sin duplicados
    """
    
    # Cargar el archivo CSV usando Dask
    ddf = dd.read_csv(input_file)
    
    # Mostrar información inicial
    print(f"\nProcesando archivo: {input_file}")
    print(f"Número inicial de registros: {len(ddf):,}")
    
    # Eliminar duplicados basados en las columnas especificadas
    # Mantenemos el primer registro de cada grupo de duplicados
    ddf_sin_duplicados = ddf.drop_duplicates(
        subset=['identifier', 'device_lon', 'device_lat'],
        keep='first'
    )
    
    # Mostrar información después de eliminar duplicados
    print(f"Número de registros después de eliminar duplicados: {len(ddf_sin_duplicados):,}")
    
    # Guardar el resultado en un nuevo archivo CSV
    ddf_sin_duplicados.to_csv(
        output_file,
        index=False,
        single_file=True  # Para que el resultado sea un solo archivo
    )
    
    print(f"\nArchivo sin duplicados guardado en: {output_file}")

if __name__ == "__main__":
    # Configurar rutas de archivos (modificar según sea necesario)
    input_csv = "Mobility_Data_Slim.csv"  # Cambiar por tu archivo de entrada
    output_csv = "Mobility_Data_woDuplicates.csv"  # Nombre del archivo de salida
    
    # Ejecutar la función principal
    eliminar_duplicados(input_csv, output_csv)