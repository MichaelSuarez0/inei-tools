from typing import Any, Tuple
import pandas as pd
import os
from icecream import ic
from functools import reduce
from ubigeos_peru import Ubigeo as ubg
from itertools import cycle
import pyreadstat

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

def read_stata(file_name: str)-> Tuple[pd.DataFrame, Any]:
    file = os.path.join(database_local, f"{file_name}.dta")
    df, metadata = pyreadstat.read_dta(file)
    return df, metadata

def group_corrupcion_by_ubigeo(df: pd.DataFrame, ubigeo: int = 12, indicador: str = "P1$08")-> pd.DataFrame:
    "Check UBIGEO codes"
    df[indicador] = df[indicador].astype("category")
    df["UBIGEO"] = df["UBIGEO"].astype(str).str.zfill(6)
    df = df.groupby(by="UBIGEO")[indicador].value_counts().unstack().reset_index()
    df = df.query(f"UBIGEO.str.startswith('{ubigeo}')", engine = 'python')
    df.index.name = ubigeo
    return df

def convert_indicador_to_dummy(df: pd.DataFrame, index = str)-> pd.DataFrame:
    df["no_confia"] = df["1"] + df["2"]
    df["confia"] = df["3"] + df["4"]
    df = df[["UBIGEO", "no_confia", "confia"]]
    df = (df[["no_confia", "confia"]].sum() / df[["no_confia", "confia"]].sum().sum()) * 100
    df.index.name = index
    df = df.to_frame()
    return df

def final_cleaning(df : pd.DataFrame)-> pd.DataFrame:
    df = df.reset_index()
    year = list(df.columns)[0]
    df.columns = ["confianza", year]
    return df

def transpose(df: pd.DataFrame):
    df = df.T.reset_index()
    df.columns = df.iloc[0]
    df = df.iloc[1:,:]
    return df



def medir_confianza(ubigeo: int = 12, indicador: str = "P1$08", trimestrales= False)-> list[pd.DataFrame]:
    dfs = []
    if not trimestrales:
        numbers = list(range(14, 24))
        base = "Enaho01B-20{}-1"
        datasets = [base.format(number) for number in numbers]
    else:
        numbers = list(range(1, 5))
        base = "Enaho01B-2024-1-t{}"
        datasets = [base.format(number) for number in numbers]
        number_cycle = cycle(numbers)

    for file_name in datasets:
        year = file_name.split("-")[1] if not trimestrales else f"{file_name.split("-")[1]}-{next(number_cycle)} "
        df = read(file_name)
        df = group_corrupcion_by_ubigeo(df, ubigeo, indicador)
        df = convert_indicador_to_dummy(df, index = year)
        df = final_cleaning(df)
        dfs.append(df)

    file_name = f"confianza_{indicador}_{ubg.get_departamento(ubigeo).lower()}"
    final_df = reduce(lambda left, right: pd.merge(left, right, on="confianza"), dfs)
    final_df = transpose(final_df)
    final_df.to_excel(os.path.join(products_folder, f"{file_name}.xlsx"), index=False)
    print(f"Se ha guardado el archivo en {os.path.join(products_folder, f"{file_name}.xlsx")}")
    return final_df


if __name__ == "__main__":
    dfs = medir_confianza(indicador="P1$04", trimestrales=True)
    
    ic(dfs)
    