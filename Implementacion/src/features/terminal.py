import subprocess
import shlex
import sys

def run_command(comando):
    """Ejecuta un comando en la terminal y maneja los errores"""
    try:
        # Dividir el comando en partes para mayor seguridad
        args = shlex.split(comando)
        
        # Ejecutar el comando capturando salida y errores
        resultado = subprocess.run(
            args,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        return resultado.stdout
    
    except subprocess.CalledProcessError as e:
        # Manejar errores de ejecución
        error_msg = f"ERROR (Código {e.returncode}):\n"
        error_msg += e.stderr if e.stderr else "Comando falló sin mensaje de error"
        return error_msg
    
    except FileNotFoundError:
        return f"Error: Comando no encontrado - '{comando.split()[0]}'"
    
    except Exception as e:
        return f"Error inesperado: {str(e)}"

if __name__ == "__main__":
    print("Ejecutor de comandos seguro")
    print("Escribe 'salir' para terminar\n")
    
    while True:
        # Obtener comando del usuario
        entrada = input("$ ").strip()
        
        if not entrada:
            continue
            
        if entrada.lower() == "salir":
            print("Saliendo del ejecutor...")
            break
            
        # Ejecutar y mostrar resultado
        resultado = run_command(entrada)
        print(resultado)