# Nota: solo funciona para preguntas en las que se evalúa la confianza
# TODO: Agregar factor de expansión
# TODO: Reducir los métodos, confunde el haber varios
from abc import ABC, abstractmethod
import logging
from pathlib import Path
from typing import Self
import numpy as np
import ubigeos_peru as ubg
import pandas as pd
from icecream import ic
import csv


# TODO: Forma más reliable de obtener el año
# TODO: Add departamento debería tener como args with_lima_metro
class EncuestaCleaner(ABC):
    def __init__(self, data_source: pd.DataFrame | Path, target_variable_id: str):
        self.df = None
        self.df_original = None
        self.variable_id = target_variable_id
        self.year = None
        self.data_source = data_source

    def load(self) -> Self:
        if isinstance(self.data_source, pd.DataFrame):
            self.df = self.data_source
            self.df_original = self.df.copy()

        else:
            if isinstance(self.data_source, str):
                self.data_source = Path(self.data_source)

            if isinstance(self.data_source, Path):
                self.df = self._load_into_memory(self.data_source)
                self.df_original = self.df.copy()

            else:
                raise TypeError(
                    "Se debe definir o la data (lista de paths o str) "
                    "o los años y los modulos para descargar"
                )
        return self

    def _detect_delimiter(
        self,
        path: Path,
        *,
        encoding="latin1",
        candidates=(",", ";", "|", "\t"),
        sample_bytes=64 * 1024,
    ) -> str:
        # Read a small text sample
        with open(path, "r", encoding=encoding, newline="") as f:
            sample = f.read(sample_bytes)

        # Excel hint: first line like "sep=;"
        lines = sample.splitlines()
        if lines:
            first = lines[0].strip().lower()
            if first.startswith("sep=") and len(first) >= 5:
                return first[4]

        # Try Sniffer with restricted candidates
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters="".join(candidates))
            return dialect.delimiter
        except csv.Error:
            # Fallback: pick the candidate that appears most across first N lines
            probe = [
                ln
                for ln in lines[:20]
                if ln and not ln.startswith("#") and not ln.lower().startswith("sep=")
            ]
            if not probe:
                return ","  # final default
            scores = {c: sum(ln.count(c) for ln in probe) for c in candidates}
            return max(scores, key=scores.get)

    def _load_into_memory(self, path: Path):
        logging.info(f"📖 Reading {path}")
        if path.suffix == ".dta":
            df = pd.read_stata(path)
        elif path.suffix == ".csv":
            delim = self._detect_delimiter(path)
            df = pd.read_csv(path, encoding="latin1", sep=delim, low_memory=False)
            # if (
            #     len(df.columns) == 1
            # ):  # INEI es bien inconsistente y algunos csv antiguos pueden estar con el delimitador ","
            #     df = pd.read_csv(path, encoding="latin1", sep=",", low_memory=False)
        logging.info(f"📖 Finished reading {path}")

        return df

    @abstractmethod
    def add_departamentos(self) -> Self:
        # self.df["DPTO"] = self.df["UBIGEO"].astype(str).apply(ubg.get_departamento)
        return self

    def remove_nas(self) -> Self:
        # raise KeyError(f"No se encontró la variable '{self.variable_id}' en las columnas del DataFrame")
        self.df[self.variable_id] = self.df[self.variable_id].replace(
            ["", " ", "nan", "NaN"], pd.NA
        )
        self.df[self.variable_id] = self.df[self.variable_id].astype("category")
        self.df = self.df.dropna(subset=[self.variable_id])
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
        self.df["FACTOR"] = (
            self.df["FACTOR"].astype(str).str.replace(",", ".", regex=False)
        )
        self.df["FACTOR"] = pd.to_numeric(self.df["FACTOR"]).copy()
        self.df = (
            self.df.groupby(self.variable_id)["FACTOR"]
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
                self.df[self.variable_id]
                .astype(str)
                .str.strip()
                .replace({"": np.nan})
                .value_counts(dropna=False)
                .rename("count")
                .to_frame()
                .reset_index()
            )
            counts.columns = [self.variable_id, "count"]
            self.df = counts

        if percentage:
            col = self.df.columns[-1]
            self.df[col] = (self.df[col] / self.df[col].sum() * 100).round(2)

        self.df.rename(columns={self.df.columns[-1]: self.year}, inplace=True)
        ic(self.df)
        return self

    def to_row_percentage(self) -> Self:
        cat_cols = [c for c in self.df.columns if c not in ["DPTO", "AÑO"]]
        self.df[cat_cols] = self.df[cat_cols].apply(pd.to_numeric, errors="coerce")
        row_totals = self.df[cat_cols].sum(axis=1)
        # self.df[cat_cols] = self.df[cat_cols].div(row_totals.replace(0, np.nan), axis=0) * 100
        self.df[cat_cols] = self.df[cat_cols].div(row_totals, axis=0) * 100
        return self

    def filter_by_variable(self) -> Self:
        # self.df = self.df.loc[:, [self.variable_id, "DPTO", "FACTOR"]]
        self.df = self.df[["ANIO", self.variable_id, "NOMBREDD", "FACTOR"]].copy()
        ic(self.df)
        return self

    def group_by_departamento(self, with_year=False) -> Self:
        self.df[self.variable_id] = self.df[self.variable_id].astype("category")
        self.df = (
            self.df.groupby(by="DPTO")[self.variable_id]
            .value_counts()
            .unstack()
            .reset_index()
        )
        if with_year:
            self.df["AÑO"] = self.year
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


class EnahoCleaner(EncuestaCleaner):
    def __init__(self, data_source: pd.DataFrame | Path, target_variable_id: str):
        super().__init__(data_source, target_variable_id)
        if "AÑO" not in self.df.columns[:4]:
            import re

            for col in self.df.columns:
                if re.match(r"^A.*O$", col, flags=re.IGNORECASE):
                    self.df = self.df.rename(columns={col: "AÑO"})
                    break
        print(self.df.columns[:4])
        self.year = int(self.df.loc[0, "AÑO"])

    def add_departamentos(self) -> Self:
        self.df.loc[:, "DPTO"] = (
            self.df["UBIGEO"].astype(str).apply(ubg.get_departamento)
        )
        return self

    def filter_by_variable(self) -> Self:
        # self.df = self.df.loc[:, [self.variable_id, "DPTO", "FACTOR"]]
        self.df = self.df[["AÑO", self.variable_id, "DPTO"]].copy()
        return self

    def wide_format(self) -> Self:
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
    def __init__(self, data_source: pd.DataFrame | Path, target_variable_id: str):
        super().__init__(data_source, target_variable_id)
        self.year = int(self.df.loc[0, "ANIO"])

    def add_departamentos(self) -> Self:
        self.df.loc[:, "DPTO"] = (
            self.df["NOMBREDD"].astype(str).apply(ubg.validate_departamento)
        )
        return self


class EndesCleaner(EncuestaCleaner):
    def __init__(self, data_source: pd.DataFrame | Path, target_variable_id: str):
        super().__init__(data_source, target_variable_id)
        self.load()
        self.year = int(self.df.iloc[0, 0])

    def add_departamentos(self) -> Self:
        self.df.loc[:, "DPTO"] = (
            self.df["UBIGEO"]
            .astype(str)
            .apply(
                lambda x: ubg.get_departamento(
                    x, with_lima_metro=True, with_lima_region=True
                )
            )
        )
        return self

    def add_provincia(self) -> Self:
        self.df.loc[:, "PROV"] = (
            self.df["UBIGEO"].astype(str).apply(ubg.get_provincia)
        )
        return self
