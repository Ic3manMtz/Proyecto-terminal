import os
import subprocess
import sys

from menus.menu import MainMenu

class Handler:
    def __init__(self):
        self.var

    def main_menu(self, choice):
        if choice == '1':
            self.data_glance()
        elif choice == '2':
            print("\nNo se ha implementado esta opción aún.") 
        elif choice == '3':
            print("\nNo se ha implementado esta opción aún.")
        elif choice == '4':
            print("\nNo se ha implementado esta opción aún.")
        elif choice == '5':
            print("\nNo se ha implementado esta opción aún.")
        elif choice == '6':
            print("\nNo se ha implementado esta opción aún.")
        elif choice == '7':
            print("\nNo se ha implementado esta opción aún.")
        elif choice == '8':
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
         
        # Lista de tuplas que representan las opciones del menú principal.
        # Cada tupla contiene:
        #   - El número de la opción como cadena.
        #   - La descripción de la opción que se mostrará al usuario.
        #(
        #  ("2", "Número de registros"),
        #  ("3", "Eliminación de columnas"),
        #  ("4", "Valores únicos de una columna"),
        #  ("5", "Creación de histograma de frecuencias de la 'device_horizontal_accuracy'"),
        #  ("6", "Creación de histograma de frecuencias de la 'identifier'"),
        #  ("7", "Análisis de frecuencias de 'identifier'"),
        #  ("8", "Salir")
        #)