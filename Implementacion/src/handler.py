import os
import subprocess
import sys

from menus.menu import MainMenu

class Handler:
    def __init__(self):
        self.csv = "Mobility_Data_Slim.csv" 

    def get_filename(self):
        return self.csv

    def main_menu(self, choice):
        if choice == '0':
            self.ask_filename()
        elif choice == '1':
            self.data_glance()
        elif choice == '2':
            self.count_registers()
        elif choice == '3':
            self.remove_columns()
        elif choice == '4':
            self.unique_values()
        elif choice == '5':
            self.accuracy_histrogram()
        elif choice == '6':
            self.identifier_histogram()
        elif choice == '7':
            self.identifier_analysis()
        elif choice == '8':
            self.delete_duplicates()
        elif choice == '9':
            print("Salir")
            sys.exit(1)
        else:
            print("\nOpcion invalida, intente de nuevo")

    def ask_filename(self):
        self.csv = MainMenu.display_ask_filename("Cargar archivo CSV")

    def data_glance(self):
        print(f"¿Desea usar el archivo por defecto? ({self.csv}) [s/n]")
        use_default = input(" ➤ ").strip().lower()

        if use_default == "s":
            filename = self.csv
        else:
            filename = MainMenu.display_ask_filename("Exploración inicial de datos")

        if not filename.endswith('.csv'):
            filename += '.csv'

        subprocess.run([
            "python3",
            "src/features/csv_glance.py",
            filename
        ])


    def count_registers(self):
        print(f"¿Desea usar el archivo por defecto? ({self.csv}) [s/n]")
        use_default = input(" ➤ ").strip().lower()

        if use_default == "s":
            filename = self.csv
        else:
            filename = MainMenu.display_ask_filename("Número de registros")

        if not filename.endswith('.csv'):
            filename += '.csv'

        subprocess.run([
            "python3",
            "src/features/csv_count_registers.py",
            filename
        ])


    def remove_columns(self):
        subprocess.run([
            "python3",
            "src/features/remove_columns.py"
        ])

    def unique_values(self):
        print(f"¿Desea usar el archivo por defecto? ({self.csv}) [s/n]")
        use_default = input(" ➤ ").strip().lower()

        if use_default == "s":
            filename = self.csv
        else:
            filename = MainMenu.display_ask_filename("Número de registros")

        if not filename.endswith('.csv'):
            filename += '.csv'

        subprocess.run([
            "python3",
            "src/features/unique_values.py",
            filename
        ])

    def accuracy_histrogram(self):
        print(f"¿Desea usar el archivo por defecto? ({self.csv}) [s/n]")
        use_default = input(" ➤ ").strip().lower()

        if use_default == "s":
            filename = self.csv
        else:
            filename = MainMenu.display_ask_filename("Número de registros")

        if not filename.endswith('.csv'):
            filename += '.csv'

        subprocess.run([
            "python3",
            "src/features/accuracy_histogram.py",
            filename
        ])

    def identifier_histogram(self):
        print(f"¿Desea usar el archivo por defecto? ({self.csv}) [s/n]")
        use_default = input(" ➤ ").strip().lower()

        if use_default == "s":
            filename = self.csv
        else:
            filename = MainMenu.display_ask_filename("Número de registros")

        if not filename.endswith('.csv'):
            filename += '.csv'

        subprocess.run([
            "python3",
            "src/features/identifier_histogram.py",
            filename
        ])

    def identifier_analysis(self):
        print(f"¿Desea usar el archivo por defecto? ({self.csv}) [s/n]")
        use_default = input(" ➤ ").strip().lower()

        if use_default == "s":
            filename = self.csv
        else:
            filename = MainMenu.display_ask_filename("Número de registros")

        if not filename.endswith('.csv'):
            filename += '.csv'

        subprocess.run([
            "python3",
            "src/features/identifier_histogram_detailed.py",
            filename
        ])


    def delete_duplicates(self):
        print(f"¿Desea usar el archivo por defecto? ({self.csv}) [s/n]")
        use_default = input(" ➤ ").strip().lower()

        if use_default == "s":
            filename = self.csv
        else:
            filename = MainMenu.display_ask_filename("Número de registros")

        if not filename.endswith('.csv'):
            filename += '.csv'

        subprocess.run([
            "python3",
            "src/features/delete_duplicates.py",
            filename
        ])