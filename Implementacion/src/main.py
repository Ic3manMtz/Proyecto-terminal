from menus.menu import MainMenu
from handler import Handler

def main(handler):
  while True:
    choice = MainMenu.display_main_menu(handler.get_filename())
    handler.main_menu(choice)

if __name__ == "__main__":
  handler = Handler()
  main(handler)