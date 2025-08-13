from abc import ABC, abstractmethod
from typing import Literal
from functools import reduce
import pandas as pd
from pathlib import Path
import logging
from ..downloaders import Downloader
from .question_type import Dummy, Confidence

# TODO: Rust script for File Manager
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

        elif (
            isinstance(self.data_source, list)
            or isinstance(self.data_source, str)
            or isinstance(self.data_source, Path)
        ):
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

    def _merge_dfs(self, df_list: list[pd.DataFrame]) -> pd.DataFrame:
        # for i, df in enumerate(df_list, start=1):
        #     print(f"DF {i} columnas: {df.columns.tolist()}")
        merged_df = reduce(
            lambda left, right: pd.merge(left, right, on=self.variable_id),
            self.df_list_clean,
        )
        return merged_df
    
    def _concat_dfs(self, df_list: list[pd.DataFrame]) -> pd.DataFrame:
        return pd.concat(df_list, ignore_index=True, sort=False)

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

    def _export_to_excel(self, output_path: Path, merged_df: pd.DataFrame):
        file_name = f"confianza_{self.variable_id}"
        file_path = output_path / f"{file_name}.xlsx"
        merged_df.to_excel(file_path, index=False)
        logging.info(f"Se ha guardado el archivo en {file_name}")

    @abstractmethod
    def get_national_trends(self):
        # self._obtain_data_if_needed()
        # # self._remove_percepcion_hogar()

        # for filename, df in self.filename_df_dict.items():
        #     logging.info(f"Reading {filename}")
        #     question_type = self._get_question_type(df)
        #     question_type.summarise()
        #     logging.info(f"Successfully read {filename}")
        #     self.df_list_clean.append(question_type.df)

        # file_name = f"confianza_{self.variable_id}"
        # file_path = f"{file_name}.xlsx"
        # final_df = reduce(
        #     lambda left, right: pd.merge(left, right, on=self.variable_id),
        #     self.df_list_clean,
        # )
        # final_df = transpose(final_df)
        # final_df.to_excel(file_path, index=False)
        # logging.info(f"Se ha guardado el archivo en {file_name}")
        pass

    @abstractmethod
    def get_department_trends(self):
        # self._obtain_data_if_needed()
        # # self._remove_percepcion_hogar()

        # for filename, df in self.filename_df_dict.items():
        #     logging.info(f"Reading {filename}")
        #     question_type = self._get_question_type(df)
        #     question_type.summarise()
        #     logging.info(f"Successfully read {filename}")
        #     self.df_list_clean.append(question_type.df)

        # file_name = f"confianza_{self.variable_id}"
        # file_path = PRODUCTS_FOLDER / f"{file_name}.xlsx"
        # final_df = reduce(
        #     lambda left, right: pd.merge(left, right, on=self.variable_id),
        #     self.df_list_clean,
        # )
        # final_df = transpose(final_df)
        # final_df.to_excel(file_path, index=False)
        # logging.info(f"Se ha guardado el archivo en {file_name}")
        pass
