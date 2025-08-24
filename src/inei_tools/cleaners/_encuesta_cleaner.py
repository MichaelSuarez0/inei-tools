# Nota: solo funciona para preguntas en las que se evalúa la confianza
# TODO: Agregar factor de expansión
# TODO: Reducir los métodos, confunde el haber varios
import logging
from pathlib import Path
from typing import Literal, Self
import numpy as np
import ubigeos_peru as ubg
import pandas as pd
from icecream import ic
from ..utils import detect_delimiter, detect_encoding
from ..configs.encuesta_config import EnahoConfig, EnapresConfig, EndesConfig

# TODO: Forma más reliable de obtener el año
# TODO: Add departamento debería tener como args with_lima_metro
class EncuestaCleaner:
    def __init__(self, encuesta: Literal["enaho", "enapres", "endes"]):
        self.data_source = None
        self.df: pd.DataFrame = None
        self.df_original = None
        self.target_variable_id = None
        self.year: int = None

        if encuesta == "enaho":
            self.config = EnahoConfig()
        elif encuesta =="enapres":
            self.config = EnapresConfig()
        elif encuesta == "endes":
            self.config = EndesConfig()
        
        self.encuesta = encuesta

    def initialize(self, data_source: str | Path | pd.DataFrame) -> Self:
        if isinstance(data_source, pd.DataFrame):
            self.df = data_source
            self.df_original = self.df.copy()

        else:
            if isinstance(data_source, str):
                self.data_source = Path(data_source)

            if isinstance(data_source, Path):
                self.df = self._load_into_memory(data_source)
                self.df_original = self.df.copy()

            else:
                raise TypeError(
                    "Se debe definir o la data (lista de paths o str) "
                    "o los años y los modulos para descargar"
                )

        self._detect_year()
        return self

    def _detect_year(self):
        self.year = int(self.df.loc[0, self.config.year_column])

    def _load_into_memory(self, path: Path):
        logging.info(f"📖 Reading {path}")
        if path.suffix == ".dta":
            df = pd.read_stata(path)
        elif path.suffix == ".csv":
            delim = detect_delimiter(path)
            encoding = detect_encoding(path)
            print(f"Encoding detectado: {encoding}")
            df = pd.read_csv(path, encoding=encoding, sep=delim, low_memory=False)
        logging.info(f"📖 Finished reading {path}")

        return df

    def add_departamentos(self) -> Self:
        if not self.encuesta == "enapres":
            self.df.loc[:, "Departamento"] = (
                self.df[self.config.ubigeo_column]
                .astype(str)
                .apply(
                    lambda x: ubg.get_departamento(
                        x, with_lima_metro=True, with_lima_region=True
                    )
                )
            )
        else:
            self.df.loc[:, "Departamento"] = (
                self.df["NOMBREDD"].astype(str).apply(ubg.validate_departamento)
            )
            return self

    def add_provincia(self) -> Self:
        if not self.encuesta == "enapres":
            self.df.loc[:, "Provincia"] = self.df[self.config.ubigeo_column].astype(str).apply(ubg.get_provincia)
        else:
            self.df.loc[:, "Provincia"] = (
            self.df["NOMBREPP"].astype(str).apply(lambda x: ubg.validate_ubicacion(x, on_error='capitalize'))
        )
        return self

    def remove_nas(self) -> Self:
        # raise KeyError(f"No se encontró la variable '{self.variable_id}' en las columnas del DataFrame")
        self.df[self.target_variable_id] = self.df[self.target_variable_id].replace(
            ["", " ", "nan", "NaN"], pd.NA
        )
        self.df[self.target_variable_id] = self.df[self.target_variable_id].astype(
            "category"
        )
        self.df = self.df.dropna(subset=[self.target_variable_id])
        return self
        # logging.info(f"Number of rows AFTER preprocessing category: {self.df.shape[0]}")

    def _test(self) -> Self:
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

    def add_factor(self) -> Self:
        self.df[self.config.factor_column] = (
            self.df[self.config.factor_column].astype(str).str.replace(",", ".", regex=False)
        )
        self.df[self.config.factor_column] = pd.to_numeric(self.df[self.config.factor_column]).copy()
        self.df = (
            self.df.groupby(self.target_variable_id)[self.config.factor_column]
            .sum()
            .sort_values(ascending=False)
        )
        self.df = self.df.reset_index()
        return self

    # TODO: Column to convert to percentage is hardcoded
    def count_categories(
        self, with_factor: bool = True, percentage: bool = True
    ) -> Self:
        if with_factor:
            self.add_factor()
        else:
            counts = (
                self.df[self.target_variable_id]
                .astype(str)
                .str.strip()
                .replace({"": np.nan})
                .value_counts(dropna=False)
                .rename("count")
                .to_frame()
                .reset_index()
            )
            counts.columns = [self.target_variable_id, "count"]
            self.df = counts

        if percentage:
            col = self.df.columns[-1]
            self.df[col] = (self.df[col] / self.df[col].sum() * 100).round(2)

        self.df.rename(columns={self.df.columns[-1]: self.year}, inplace=True)
        ic(self.df)
        return self

    def to_row_percentage(self) -> Self:
        cat_cols = [c for c in self.df.columns if c not in ["Departamento", self.config.year_column]]
        self.df[cat_cols] = self.df[cat_cols].apply(pd.to_numeric, errors="coerce")
        row_totals = self.df[cat_cols].sum(axis=1)
        # Lo siguiente es como self.df[cat_cols] = self.df[cat_cols] / row_totals * 100 PERO PARA CADA FILA
        self.df[cat_cols] = self.df[cat_cols].div(row_totals, axis=0) * 100
        return self

    def filter_by_variable(self) -> Self:
        # self.df = self.df.loc[:, [self.variable_id, "DPTO", "FACTOR"]]
        self.df = self.df[
            [self.config.year_column, self.target_variable_id, "DEPARTAMENTO", self.factor_col]
        ].copy()
        return self

    def group_by_departamento(self, with_year=True, with_factor=True) -> Self:
        self.df[self.target_variable_id] = self.df[self.target_variable_id].astype(
            "category"
        )
        if not with_factor:
            self.df = (
                self.df.groupby(by="Departamento")[self.target_variable_id]
                .value_counts()
                .unstack()
                .reset_index()
            )
        else:
            self.df = (
                self.df.groupby(by=["Departamento", self.target_variable_id])[self.factor_col]
                .sum()
                .unstack()
                .reset_index()
            )

        if with_year:
            self.df["Año"] = self.year
            self.df.insert(1, "Año", self.df.pop("Año"))  # Mover "AÑO" a la segunda posición
        # self.df.index.name = "DPTO"
        return self

    def filter_by_departamento(self, dep: str) -> Self:
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

    def get_df(self) -> pd.DataFrame:
        return self.df

# TODO: Probar lo de detect year si no funciona
# class EnahoCleaner(EncuestaCleaner):
#     def __init__(self):
#         super().__init__()

#     def _detect_year(self) -> Self:
#         if "AÑO" not in self.df.columns[:4]:
#             import re

#             for col in self.df.columns:
#                 if re.match(r"^A.*O$", col, flags=re.IGNORECASE):
#                     self.df = self.df.rename(columns={col: "AÑO"})
#                     break
#         # print(self.df.columns[:4])
#         self.year = int(self.df.loc[0, "AÑO"])

#     def wide_format(self) -> Self:
#         self.df[""]

