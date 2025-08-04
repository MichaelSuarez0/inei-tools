from collections import defaultdict
from typing import Any, Tuple
import pandas as pd
import os
from functools import reduce
from ubigeos_peru import Ubigeo as ubg
from itertools import cycle

# Globals
script_dir = os.path.dirname(__file__)  
products_folder = os.path.join(script_dir, 'products')
database_local = os.path.join(script_dir, "databases")

# Nota: solo funciona para preguntas en las que se evalúa la confianza
# TODO: Agregar factor de expansión

def read(file_name: str)-> pd.DataFrame:
    file = os.path.join(database_local, f"{file_name}.csv")
    df = pd.read_csv(file, encoding="latin1", sep=",")
    return df

# def read_stata(file_name: str)-> Tuple[pd.DataFrame, Any]:
#     file = os.path.join(database_local, f"{file_name}.dta")
#     df, metadata = pyreadstat.read_dta(file)
#     return df, metadata

def create_label_mappings(df: pd.DataFrame, metadata: Any)-> Tuple[dict, dict]:
    mappings = defaultdict(dict)
    inverted = defaultdict(dict)
    for name, label in zip(df.columns, metadata.column_labels):
        mappings[name] = label
        inverted[label] = name
    return dict(mappings), dict(inverted)


if __name__ == "__main__":
    df, metadata = read_stata(file_name = "enaho01-2023-100")
    mappings, inverted = create_label_mappings(df, metadata)
    #performance: pyinstrument -o profile.html pandas_playground/dummies.py
