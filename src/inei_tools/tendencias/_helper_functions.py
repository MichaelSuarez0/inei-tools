from pathlib import Path
import pandas as pd
from itertools import cycle
from functools import reduce
from ubigeos_peru import Ubigeo as ubg

# Globals
SCRIPT_DIR = Path(__file__).parent.resolve()
PRODUCTS_FOLDER = SCRIPT_DIR.parent / 'products'
DATABASES_FOLDER = SCRIPT_DIR.parent / "databases"

def read(file_name: str)-> pd.DataFrame:
    file_s = DATABASES_FOLDER / f"{file_name}.csv"
    df = pd.read_csv(file_s, encoding="latin1", sep=",")
    return df

# def read_stata(file_name: str)-> Tuple[pd.DataFrame, Any]:
#     file = DATABASES_FOLDER / f"{file_name}.dta"
#     df, metadata = pyreadstat.read_dta(file)
#     return df, metadata

def transpose(df: pd.DataFrame):
    df = df.T.reset_index()
    df.columns = df.iloc[0]
    df = df.iloc[1:,:]
    return df

def load_into_memory(trimestrales= False)-> list[pd.DataFrame]:
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
        dfs.append(df)

def save(dfs: list[pd.DataFrame], indicador: str, ubigeo):
    file_name = f"enaho_{indicador}_{ubg.get_departamento(ubigeo).lower()}"
    final_df = reduce(lambda left, right: pd.merge(left, right, on="confianza"), dfs)
    final_df = transpose(final_df)
    final_df.to_excel(PRODUCTS_FOLDER / f"{file_name}.xlsx", index=False)
    print(f"Se ha guardado el archivo en {PRODUCTS_FOLDER / f"{file_name}.xlsx"}")
    return final_df