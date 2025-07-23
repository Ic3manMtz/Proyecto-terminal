import pandas as pd
from tqdm import tqdm
from collections import defaultdict
from src.menus.menu import MainMenu

csv_file = "Mobility_Data_Slim.csv" 
output_file = "unique_values.txt"
output_file_100 = "unique_values_100.txt"  # Archivo para valores con >100 repeticiones
chunk_size = 1_000_000  

try:
    available_columns = pd.read_csv(csv_file, nrows=0).columns.tolist()
except Exception as e:
    print(f"❌ Error reading the file: {e}")
    exit()

try:
    selected_index = MainMenu.display_available_columns(available_columns)
    target_column = available_columns[selected_index]
except (ValueError, IndexError):
    print("❌ Invalid selection.")
    exit()

# Diccionario para contar ocurrencias
value_counts = defaultdict(int)
total_rows = 0

print(f"\n🔄 Processing column: {target_column}\n")

# Contar ocurrencias de cada valor
for chunk in tqdm(pd.read_csv(csv_file, usecols=[target_column], chunksize=chunk_size), desc="Counting occurrences"):
    chunk_values = chunk[target_column].dropna().astype(str)
    for value in chunk_values:
        value_counts[value] += 1
    total_rows += len(chunk)

print(f"\n📊 Total rows processed: {total_rows:,}")
print(f"📊 Unique values found: {len(value_counts):,}")

# Filtrar valores con más de 100 repeticiones
values_over_100 = [k for k, v in value_counts.items() if v > 100]
print(f"📊 Values with >100 occurrences: {len(values_over_100):,}")

# Guardar todos los valores únicos (original)
with open(output_file, "w", encoding="utf-8") as f:
    f.write("\n".join(sorted(value_counts.keys())))

# Guardar SOLO los valores con más de 100 repeticiones (sin conteos)
with open(output_file_100, "w", encoding="utf-8") as f:
    # Ordenar alfabéticamente los valores
    f.write("\n".join(sorted(values_over_100)))

print(f"\n✅ Saved all unique values to: {output_file}")
print(f"✅ Saved values with >100 occurrences (values only) to: {output_file_100}")

# Mostrar estadísticas de los más frecuentes
top_values = sorted(value_counts.items(), key=lambda x: x[1], reverse=True)[:10]
print("\n🔍 Top 10 values by occurrence:")
for value, count in top_values:
    print(f"{value}: {count:,} occurrences")