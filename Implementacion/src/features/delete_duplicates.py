import dask.dataframe as dd
import sys
import os
from tqdm import tqdm

def delete_duplicates(input_file, output_file):
    tqdm.pandas(desc="Procesando datos")
    
    # Cargar el archivo CSV usando Dask
    print(f"\nCargando archivo: {input_file}")
    ddf = dd.read_csv(input_file)
    
    with tqdm(total=1, desc="Contando registros iniciales") as pbar:
        initial_count = len(ddf.compute())
        pbar.update(1)
    
    print(f"Número inicial de registros: {initial_count:,}")
    
    # Eliminar duplicados con barra de progreso
    print("\nEliminando duplicados...")
    with tqdm(total=3, desc="Progreso") as pbar:
        # Paso 1: Identificar duplicados 
        ddf_deduplicate = ddf.drop_duplicates(
            subset=['identifier', 'timestamp', 'device_lon', 'device_lat'],
            keep='first'
        )
        pbar.update(1)
        
        # Paso 2: Calcular recuento después de deduplicación
        final_count = len(ddf_deduplicate.compute())
        pbar.update(1)
        
        # Paso 3: Guardar resultados
        ddf_deduplicate.to_csv(
            output_file,
            index=False,
            single_file=True
        )
        pbar.update(1)
    
    print(f"\nNúmero de registros después de eliminar duplicados: {final_count:,}")
    print(f"Registros eliminados: {initial_count - final_count:,}")
    print(f"Archivo sin duplicados guardado en: {output_file}")

if __name__ == "__main__":
    # Configuración
    if len(sys.argv) < 2:
        print("Error: Debe especificar un archivo CSV como argumento")
        sys.exit(1)

    input_csv = sys.argv[1]
    base_name = os.path.splitext(input_csv)[0]
    output_csv = f"{base_name}_DeDuplicate.csv" 
    
    # Ejecutar la función principal
    delete_duplicates(input_csv, output_csv)