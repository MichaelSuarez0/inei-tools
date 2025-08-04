from abc import ABC, abstractmethod
from pathlib import Path
from typing import Self
import pandas as pd
import ubigeos_peru as ubg
from ._helper_functions import DATABASES_FOLDER, read, transpose
import logging
from functools import reduce

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)



class Pregunta(ABC):
    pass

class Dummy(Pregunta):
    def __init__(self, df: pd.DataFrame, target_variable_id: str):
        super().__init__(df, target_variable_id)
        self.df: pd.DataFrame = df
    
    def recode_variable_to_dummy(self)-> pd.DataFrame:
        self.df = self.df.rename(columns={"0": "no_confia", "1": "confia"})
    
    def summarise(self):
        self.preprocess_dataframe()
        #self.df = self.filter_by_departamento(dep)
        self.recode_variable_to_dummy()
        self.calculate_proportions()
        self.final_cleaning()
        print(self.df.to_markdown())
        return self.df


class Confidence(Pregunta):
    def __init__(self, df: pd.DataFrame, target_variable_id: str):
        super().__init__(df, target_variable_id)
        self.df: pd.DataFrame = df

    def recode_variable_to_dummy(self)-> pd.DataFrame:
        self.df["no_confia"] = self.df["1"] + self.df["2"]
        self.df["confia"] = self.df["3"] + self.df["4"]
        self.df = self.df[["UBIGEO", "no_confia", "confia"]]
    
    def calculate_proportions(self):
        self.df = (self.df[["no_confia", "confia"]].sum() / self.df[["no_confia", "confia"]].sum().sum()) * 100
        self.df = self.df.to_frame()
        return self.df

    def final_cleaning(self)-> pd.DataFrame:
        self.df = self.df.reset_index()
        year = list(self.df.columns)[0]
        self.df.columns = ["confianza", year]
        return self.df

    def summarise(self)-> pd.DataFrame:
        self.preprocess_dataframe()
        self.recode_variable_to_dummy()
        self._group_by_departamento()
        self.calculate_proportions()
        self.final_cleaning()
        return self.df
