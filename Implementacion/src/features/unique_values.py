import pandas as pd
from tqdm import tqdm
import os
from src.menus.menu import MainMenu  # Asumo que este módulo existe en tu estructura

def get_valid_filename(prompt):
    """Solicita al usuario un nombre de archivo válido."""
    while True:
        filename = input(prompt).strip()
        if not filename:
            print("❌ Error: Debes ingresar un nombre de archivo.")
            continue
        if not os.path.exists(filename):
            print(f"❌ Error: El archivo '{filename}' no existe.")
            continue
        if not filename.lower().endswith('.csv'):
            print("⚠ Advertencia: El archivo no tiene extensión .csv, pero se intentará leer igual.")
        return filename

def main():
    print("\n" + "="*50)
    print(" EXTRACTOR DE VALORES ÚNICOS DE COLUMNAS CSV")
    print("="*50 + "\n")
    
    # Solicitar archivo CSV al usuario
    csv_file = get_valid_filename("Ingrese el nombre/path del archivo CSV a analizar: ")
    output_file = "unique_values.txt"
    chunk_size = 1_000_000  # Tamaño del chunk para procesamiento

    try:
        # Leer solo los nombres de las columnas
        available_columns = pd.read_csv(csv_file, nrows=0).columns.tolist()
    except Exception as e:
        print(f"❌ Error leyendo el archivo: {e}")
        exit()

    try:
        # Mostrar menú para seleccionar columna (asumo que MainMenu.display_available_columns existe)
        selected_index = MainMenu.display_available_columns(available_columns)
        target_column = available_columns[selected_index]
    except (ValueError, IndexError):
        print("❌ Selección inválida.")
        exit()
    except Exception as e:
        print(f"❌ Error inesperado al seleccionar columna: {e}")
        exit()

    # Procesar el archivo en chunks
    unique_values = set()
    print(f"\n🔄 Procesando columna: {target_column}\n")
    
    try:
        for chunk in tqdm(pd.read_csv(csv_file, usecols=[target_column], chunksize=chunk_size)):
            unique_values.update(chunk[target_column].dropna().astype(str))
    except Exception as e:
        print(f"❌ Error durante el procesamiento: {e}")
        exit()

    # Guardar resultados
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(unique_values)))
    except Exception as e:
        print(f"❌ Error guardando los resultados: {e}")
        exit()

    # Mostrar resumen
    print(f"\n✅ Se encontraron {len(unique_values):,} valores únicos.")
    print(f"📄 Resultados guardados en: {output_file}")

    # Mostrar muestra de valores
    print("\n🔍 Muestra de valores únicos (primeros 10):")
    print("\n".join(sorted(unique_values)[:10]))

if __name__ == "__main__":
    main()