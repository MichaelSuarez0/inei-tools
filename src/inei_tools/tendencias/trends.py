from abc import ABC, abstractmethod
import logging
from typing import Literal, Optional
from functools import reduce
import pandas as pd
from pathlib import Path

import ubigeos_peru as ubg
from ..downloaders import Downloader
from ..encuestas import Encuesta
from .cleaners import EnapresCleaner

from .question_type import Dummy, Confidence
from ._helper_functions import (
    DATABASES_FOLDER,
    PRODUCTS_FOLDER,
    load_into_memory,
    read,
    transpose,
)


logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

# TODO: Rust script for File Manager, pyautogui to farm supports
# TODO: Evaluate data_source as list[pd.DataFrame] (drawback: no filenames to extract years)


class TendenciasABC(ABC):
    def __init__(
        self,
        data_source: list[Path] | Downloader | None = None,
        target_variable_id: str = "",
        question_type: Literal["dummy", "confidence"] = "dummy",
        output_dir: str = ".",
    ):
        self.data_source = data_source
        self.variable_id = target_variable_id.upper()
        self.question_type = question_type
        self.output_dir = Path(output_dir)

        self.filename_df_dict = {}
        self.downloader = None
        self.df_list_clean = []

    def _obtain_data_if_needed(self):
        if isinstance(self.data_source, Downloader):
            self.downloader = self.data_source
            self.downloader.overwrite = False
            path_list = self.downloader.download_all()
            self._load_into_memory(path_list)

        elif isinstance(self.data_source, list) or isinstance(self.data_source, str) or isinstance(self.data_source, Path):
            if not isinstance(self.data_source, list):
                self.data_source = [self.data_source]

            if all(isinstance(data, str) for data in self.data_source):
                self.data_source = [Path(data) for data in self.data_source]

            if all(isinstance(data, Path) for data in self.data_source):
                self.filename_df_dict = self._load_into_memory(self.data_source)
            # elif all(isinstance(data, pd.DataFrame) for data in self.data_source):

        else:
            raise TypeError(
                "Se debe definir o la data (lista de paths o str) "
                "o los años y los modulos para descargar"
            )
        

    def _load_into_memory(self, path_list: list[Path]):
        import pandas as pd

        logging.info("📖 Reading file paths")

        self.filename_df_dict = {}
        for file_path in path_list:
            logging.info(f"📖 Reading {file_path}")
            if file_path.suffix == ".dta":
                df = pd.read_stata(file_path)
                self.filename_df_dict[file_path.name] = df
            elif file_path.suffix == ".csv":
                df = pd.read_csv(
                    file_path, encoding="latin1", sep=";", low_memory=False
                )
                if (
                    len(df.columns) == 1
                ):  # INEI es bien inconsistente y algunos csv antiguos pueden estar con el delimitador ","
                    df = pd.read_csv(
                        file_path, encoding="latin1", sep=",", low_memory=False
                    )
                self.filename_df_dict[file_path.name] = df
            logging.info(f"📖 Finished reading {file_path}")

        return self.filename_df_dict
    

    def _merge_dfs(self, df_list: list[pd.DataFrame])-> pd.DataFrame:
        for i, df in enumerate(df_list):
            print(f"DF {i} columnas: {df.columns.tolist()}")
        merged_df = reduce(
            lambda left, right: pd.merge(left, right, on=self.variable_id),
            self.df_list_clean,
        )
        return merged_df

    # def _remove_percepcion_hogar(self):
    #     # Se mantiene: enaho01b-2022-1.dta -> GOBERNABILIDAD (PERSONAS DE 18 AÑOS Y MAS DE EDAD)
    #     # Se elimina: enaho01b-2022-2.dta -> PERCEPCIÓN DEL HOGAR (SÓLO PARA EL JEFE DEL HOGAR O CÓNYUGE MÓDULO)
    #     self.filename_df_dict = {
    #         f: df for f, df in self.filename_df_dict.items()
    #         if (f.split("-")[-1].split(".")[0]) != "2"
    #     }

    #     return self.filename_df_dict

    def _get_question_type(self, df: pd.DataFrame):
        if self.question_type == "dummy":
            return Dummy(df, self.variable_id)
        elif self.question_type == "confidence":
            return Confidence(df, self.variable_id)

    @abstractmethod
    def get_national_trends(self):
        self._obtain_data_if_needed()
        # self._remove_percepcion_hogar()

        for filename, df in self.filename_df_dict.items():
            logging.info(f"Reading {filename}")
            question_type = self._get_question_type(df)
            question_type.summarise()
            logging.info(f"Successfully read {filename}")
            self.df_list_clean.append(question_type.df)

        file_name = f"confianza_{self.variable_id}"
        file_path = PRODUCTS_FOLDER / f"{file_name}.xlsx"
        final_df = reduce(
            lambda left, right: pd.merge(left, right, on=self.variable_id),
            self.df_list_clean,
        )
        final_df = transpose(final_df)
        final_df.to_excel(file_path, index=False)
        logging.info(f"Se ha guardado el archivo en {file_name}")
        return final_df


class TendenciasEnapres(TendenciasABC):
    def __init__(
        self,
        data_source=None,
        target_variable_id="",
        question_type="dummy",
        output_dir=".",
    ):
        super().__init__(data_source, target_variable_id, question_type, output_dir)
    
    def get_national_trends(self, output_path: Optional[Path] = None):
        self._obtain_data_if_needed()
        # self._remove_percepcion_hogar()

        for filename, df in self.filename_df_dict.items():
            cleaner = EnapresCleaner(df, self.variable_id)
            # cleaner.remove_nas().add_departamentos().filter_by_variable()
            logging.info(f"Cleaning {filename}")
            cleaner.remove_nas().add_departamentos().filter_by_variable().count_categories()
            # return cleaner.df
           
            self.df_list_clean.append(cleaner.df)
        
        merged_df = self._merge_dfs(self.df_list_clean)

        #final_df = transpose(final_df)
        if output_path:
            file_name = f"confianza_{self.variable_id}"
            file_path = PRODUCTS_FOLDER / f"{file_name}.xlsx"
            merged_df.to_excel(file_path, index=False)
            logging.info(f"Se ha guardado el archivo en {file_name}")
        logging.info(f"Se terminó")
        return merged_df


class Tendencias:
    def __init__(
        self,
        data_source=None,
        target_variable_id="",
        question_type="dummy",
        output_dir=".",
        encuesta: Literal["enapres", "enaho"] = "enapres",
    ):
        self.data_source= data_source
        self.target_variable_id= target_variable_id
        self.question_type= question_type
        self.output_dir= output_dir
        self.encuesta= encuesta

    def get_national_trends(self)-> pd.DataFrame:
        pd.options.mode.copy_on_write = True
        if self.encuesta == "enapres":
            tendencia = TendenciasEnapres(
                data_source=self.data_source,
                target_variable_id=self.target_variable_id,
                question_type=self.question_type,
                output_dir=self.output_dir,
            )
            df = tendencia.get_national_trends()
            pd.options.mode.copy_on_write = False
            return df
        
        

    # def preprocess_dataframe(self):
    #     self._remove_nas()
    #     self._add_departamentos()
    #     self._filter_by_variable()
    #     self._group_by_departamento()
    #     #self.df = self.df[self.target_variable_id].value_counts(normalize=True) * 100
    #     return self.df