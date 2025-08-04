from typing import Any, Tuple

import pandas as pd
import pyreadstat
from inei_tools import Tendencias
from pathlib import Path
from icecream import ic
from pprint import pprint
import os

# Globals
script_dir = Path(__file__).parent  
#products_folder = os.path.join(script_dir, 'products')
DATABASES_FOLDER = script_dir.parent / "databases"

def find_columns_by_description(meta, keywords):
    """Encontrar columnas cuya descripción contenga ciertas palabras clave"""
    matching_columns = {}
    
    for variable, descripcion in meta.column_names_to_labels.items():
        if any(keyword.lower() in descripcion.lower() for keyword in keywords):
            matching_columns[variable] = descripcion
    
    return matching_columns

def read_stata(file_name: str)-> Tuple[pd.DataFrame, Any]:
    file = DATABASES_FOLDER / f"{file_name}.dta"
    df, metadata = pyreadstat.read_dta(file)
    return df, metadata


if __name__ == "__main__":
    # test = Path(__file__).parent
    # paths = [path for path in list(test.iterdir()) if path.is_file() and path.suffix == ".dta"][:1][0]
    df, meta = read_stata("enaho01-2022-100")
    for variable, descripcion in meta.column_names_to_labels.items():
        if variable == "p25_1".lower():
            print(descripcion)


   # paths = [path for path in list(test.iterdir()) if path.is_file() and path.suffix == ".dta"][:1][0]
    # df, meta = read_stata("enaho01-2022-100")
    # for variable, descripcion in meta.column_names_to_labels.items():
    #     if variable.startswith("p2"):
    #     #if variable == "p2_1_01".lower():
    #         print(variable)
            

    