import sys
import importlib

if __name__ == "__main__":
    print(sys.argv)
    importlib.import_module(f'commands.{sys.argv[1]}').__init__(*(sys.argv[2:]))
