import subprocess
import pyreadstat
import pandas as pd

def read_dbf(input_path: str, output_path: str, encoding="cp850", out_encoding="utf8"):
    cmd = [
        "dbf2csv",
        input_path,
        output_path,
        "-ie", encoding,
        "-oe", out_encoding,
        "-q", "all",
        "-d", ","
    ]
    subprocess.run(cmd, check=True)

def read_spss(path, apply_labels=False):
    """
    Lee un archivo SPSS (.sav) y conserva etiquetas.
    
    Parámetros:
    - path (str): Ruta al archivo .sav o .zsav
    - apply_labels (bool): Si True, reemplaza los valores por etiquetas

    Retorna:
    - df (DataFrame): Datos
    - metadata (dict): Diccionario con value_labels y variable_labels
    """
    df, meta = pyreadstat.read_sav(path, apply_value_labels=apply_labels)
    
    labels = {
        "variable_labels": meta.column_labels,  # nombre descriptivo de variables
        "value_labels": meta.value_labels       # dict con codificación de etiquetas
    }
    
    return df, labels