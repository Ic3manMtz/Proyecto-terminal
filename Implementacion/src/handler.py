import os
import subprocess
import sys

from menus.menu import MainMenu

class Handler:
    def __init__(self):
        self.var = None

    def main_menu(self, choice):
        if choice == '1':
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
            self.show_coordinates()
        elif choice == '9':
            self.unique_values_100()
        elif choice == '10':
            self.delete_duplicates()
        elif choice == '0':
            self.terminal()
        elif choice == '11':
            print("Salir")
            sys.exit(1)
        else:
            print("\nOpcion invalida, intente de nuevo")

    def data_glance(self):
        filename = MainMenu.display_ask_filename("Exploración inicial de datos")
        
        if not filename.endswith('.csv'):
            filename += '.csv'

        subprocess.run([
            "python3",
            "src/features/csv_glance.py",
            filename
        ])

    def count_registers(self):
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
        subprocess.run([
            "python3",
            "src/features/unique_values.py"
        ])

    def accuracy_histrogram(self):
        subprocess.run([
            "python3",
            "src/features/accuracy_histogram.py"
        ])

    def identifier_histogram(self):
        subprocess.run([
            "python3",
            "src/features/identifier_histogram.py"
        ])

    def identifier_analysis(self):
        subprocess.run([
            "python3",
            "src/features/identifier_histrogram_detailed.py"
        ])

    def show_coordinates(self):
        subprocess.run([
            "python3",
            "src/features/show_coordenates.py"
        ])

    def terminal(self):
        subprocess.run([ 
            "python3",
            "src/features/terminal.py"
        ])

    def unique_values_100(self):
        subprocess.run([
            "python3",
            "src/features/unique_values_100.py"
        ])

    def delete_duplicates(self):
        subprocess.run([
            "python3",
            "src/features/delete_duplicates.py"
        ])