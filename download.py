from enahodata import enahodata, Modulo
import os

script_dir = os.path.dirname(__file__)
output_dir = os.path.join(script_dir, "databases")

# TODO: This should download only dtas and no folder should be created
if __name__ == "__main__":
    enahodata(["01", "85"], ["2022", "2023"], only_dta=True, descomprimir=True, overwrite=True, output_dir=output_dir)