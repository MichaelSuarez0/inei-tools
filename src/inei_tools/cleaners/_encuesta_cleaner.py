# Nota: solo funciona para preguntas en las que se evalúa la confianza
# TODO: Agregar factor de expansión
# TODO: Reducir los métodos, confunde el haber varios
from abc import ABC, abstractmethod
from typing import Self
import numpy as np
import ubigeos_peru as ubg
import pandas as pd
from icecream import ic

# TODO: Forma más reliable de obtener el año
class EncuestaCleaner(ABC):
    def __init__(self, df: pd.DataFrame, target_variable_id: str):
        self.df = df
        self.df_original = df.copy()
        self.variable_id = target_variable_id
        self.year = None   
 
    @abstractmethod
    def add_departamentos(self)-> Self:
        # self.df["DPTO"] = self.df["UBIGEO"].astype(str).apply(ubg.get_departamento)
        return self

    def remove_nas(self)-> Self:
            #raise KeyError(f"No se encontró la variable '{self.variable_id}' en las columnas del DataFrame")
        self.df[self.variable_id] = self.df[self.variable_id].replace(["", " ", "nan", "NaN"], pd.NA)
        self.df[self.variable_id] = self.df[self.variable_id].astype("category")
        self.df = self.df.dropna(subset=[self.variable_id])
        return self
        # logging.info(f"Number of rows AFTER preprocessing category: {self.df.shape[0]}")

    def _test(self)-> Self:
        # self.df.columns = [str(col).upper() for col in self.df.columns]
        # logging.info(f"Number of rows BEFORE preprocessing category: {self.df.shape[0]}")
        # try:
        #     self.df[self.variable_id] = self.df[self.variable_id].astype(str).str.strip()
        # except KeyError:
        #     try:
        #         if "$" in self.variable_id:
        #             self.variable_id = self.variable_id.replace("$", "_")
        #         self.df[self.variable_id] = self.df[self.variable_id].astype(str).str.strip()
        #     except KeyError:
        #         logging.info(list(self.df.columns[0:5]))
        #         logging.info([col for col in self.df.columns if col.startswith(self.variable_id[:2])])
        pass

    def add_factor(self)-> Self:
        self.df["FACTOR"] = (
            self.df["FACTOR"]
            .astype(str)
            .str.replace(",", ".", regex=False)
        )
        self.df["FACTOR"] = pd.to_numeric(self.df["FACTOR"]).copy()
        self.df = self.df.groupby(self.variable_id)["FACTOR"].sum().sort_values(ascending=False)
        self.df = self.df.reset_index()
        return self
    
    # TODO: Column to convert to percentage is hardcoded
    def count_categories(self, with_factor: bool = True, percentage: bool = True)-> Self:
        if with_factor:
            self.add_factor()
        else:
            counts = (self.df[self.variable_id]
                      .astype(str).str.strip()
                      .replace({"": np.nan})
                      .value_counts(dropna=False)
                      .rename("count")
                      .to_frame()
                      .reset_index())
            counts.columns = [self.variable_id, "count"]
            self.df = counts
        

        if percentage:
            col = self.df.columns[-1]
            self.df[col] = (self.df[col] / self.df[col].sum() * 100).round(2)
  
        self.df.rename(columns={self.df.columns[-1]: self.year}, inplace=True)
        ic(self.df)
        return self
    
    def to_row_percentage(self)-> Self:
        cat_cols = [c for c in self.df.columns if c not in ["DPTO", "AÑO"]]
        self.df[cat_cols] = self.df[cat_cols].apply(pd.to_numeric, errors="coerce")
        row_totals = self.df[cat_cols].sum(axis=1)
        #self.df[cat_cols] = self.df[cat_cols].div(row_totals.replace(0, np.nan), axis=0) * 100
        self.df[cat_cols] = self.df[cat_cols].div(row_totals, axis=0) * 100
        return self




    def filter_by_variable(self)-> Self:
        #self.df = self.df.loc[:, [self.variable_id, "DPTO", "FACTOR"]]
        self.df = self.df[["ANIO", self.variable_id, "NOMBREDD", "FACTOR"]].copy()
        ic(self.df)
        return self

    def group_by_departamento(self, with_year = False)-> Self:
        self.df[self.variable_id] = self.df[self.variable_id].astype("category")
        self.df = self.df.groupby(by="DPTO")[self.variable_id].value_counts().unstack().reset_index()
        if with_year:
            self.df["AÑO"] = self.year
        #self.df.index.name = "DPTO"
        return self

    def filter_by_departamento(self, dep: str)-> Self:
        dep = ubg.validate_departamento(dep, normalize=True)
        self.df = self.df.query(f"DPTO == '{dep}'")
        return self
    
    # def preprocess_dataframe(self):
    #     self._remove_nas()
    #     self._add_departamentos()
    #     self._filter_by_variable()
    #     self._group_by_departamento()
    #     #self.df = self.df[self.target_variable_id].value_counts(normalize=True) * 100
    #     return self.df
    

class EnahoCleaner(EncuestaCleaner):
    def __init__(self, df: pd.DataFrame, target_variable_id: str):
        super().__init__(df, target_variable_id)
        if "AÑO" not in self.df.columns[:4]:
            import re
            for col in self.df.columns:
                if re.match(r'^A.*O$', col, flags=re.IGNORECASE):
                    self.df = self.df.rename(columns={col: 'AÑO'})
                    break
        print(self.df.columns[:4])
        self.year = int(self.df.loc[0,"AÑO"])
        
    def add_departamentos(self)-> Self:
        self.df.loc[:, "DPTO"] = self.df["UBIGEO"].astype(str).apply(ubg.get_departamento)
        return self

    def filter_by_variable(self)-> Self:
        #self.df = self.df.loc[:, [self.variable_id, "DPTO", "FACTOR"]]
        self.df = self.df[["AÑO", self.variable_id, "DPTO"]].copy()        
        return self
    
    def wide_format(self)-> Self:
        self.df[""]

    # def final_cleaning(self)-> pd.DataFrame:
    #     self.df = self.df.reset_index()
    #     self.df = self.df.rename(columns={0: self.year})
    #     return self.df
  
    # def calculate_proportions(self):
    #     self.df = (self.df[["no_confia", "confia"]].sum() / self.df[["no_confia", "confia"]].sum().sum()) * 100
    #     #self.df.index.name = index

    #     self.df = self.df.to_frame()
    #     return self.df

    
class EnapresCleaner(EncuestaCleaner):
    def __init__(self, df: pd.DataFrame, target_variable_id: str):
        super().__init__(df, target_variable_id)
        self.year = int(self.df.loc[0,"ANIO"])

    def add_departamentos(self)-> Self:
        self.df.loc[:, "DPTO"] = self.df["NOMBREDD"].astype(str).apply(ubg.validate_departamento)
        return self