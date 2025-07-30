import pandas as pd
from tqdm import tqdm
import os
import sys
from src.menus.menu import MainMenu

def main():
    print("\n" + "="*50)
    print(" EXTRACTOR DE VALORES ÚNICOS DE COLUMNAS CSV")
    print("="*50 + "\n")

    # Validar argumentos
    if len(sys.argv) < 2:
        print("❌ Uso: python extract_unique.py <archivo.csv>")
        sys.exit(1)

    csv_file = sys.argv[1]

    # Validar existencia del archivo
    if not os.path.exists(csv_file):
        print(f"❌ Error: El archivo '{csv_file}' no existe.")
        sys.exit(1)

    if not csv_file.lower().endswith('.csv'):
        print("⚠ Advertencia: El archivo no tiene extensión .csv, pero se intentará leer igual.")

    chunk_size = 1_000_000  # Tamaño del chunk para procesamiento

    # Leer nombres de columnas
    try:
        available_columns = pd.read_csv(csv_file, nrows=0).columns.tolist()
    except Exception as e:
        print(f"❌ Error leyendo el archivo: {e}")
        sys.exit(1)

    # Menú para seleccionar columna
    try:
        selected_index = MainMenu.display_available_columns(available_columns)
        target_column = available_columns[selected_index]
    except (ValueError, IndexError):
        print("❌ Selección inválida.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error inesperado al seleccionar columna: {e}")
        sys.exit(1)

    # Generar nombre dinámico del archivo de salida
    safe_column_name = target_column.replace(" ", "_").replace("/", "_")
    output_file = f"valores_unicos_{safe_column_name}.txt"

    # Procesar valores únicos
    unique_values = set()
    print(f"\n🔄 Procesando columna: {target_column}\n")

    try:
        for chunk in tqdm(pd.read_csv(csv_file, usecols=[target_column], chunksize=chunk_size)):
            unique_values.update(chunk[target_column].dropna().astype(str))
    except Exception as e:
        print(f"❌ Error durante el procesamiento: {e}")
        sys.exit(1)

    # Convertir a lista ordenada numéricamente si es posible
    try:
        numeric_values = sorted([float(v) for v in unique_values])
        is_numeric = True
    except ValueError:
        is_numeric = False

    # Guardar archivo con rango y valores
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            if is_numeric:
                min_val = numeric_values[0]
                max_val = numeric_values[-1]
                f.write(f"# Rango de valores: {min_val} - {max_val}\n")
                f.write("\n".join(str(v) for v in numeric_values))
            else:
                sorted_values = sorted(unique_values)
                f.write("# Rango de valores: No numérico\n")
                f.write("\n".join(sorted_values))
    except Exception as e:
        print(f"❌ Error guardando los resultados: {e}")
        sys.exit(1)

    # Mostrar resumen
    print(f"\n✅ Se encontraron {len(unique_values):,} valores únicos.")
    print(f"📄 Resultados guardados en: {output_file}")

    print("\n🔍 Muestra de valores únicos (primeros 10):")
    print("\n".join(sorted(unique_values)[:10]))

if __name__ == "__main__":
    main()
