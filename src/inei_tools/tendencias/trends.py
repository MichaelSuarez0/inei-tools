from abc import ABC, abstractmethod
from typing import Literal, Optional
from functools import reduce
import pandas as pd
from pathlib import Path
import logging
from ..downloaders import Downloader
from .t_enapres import TendenciasEnapres
from .question_type import Dummy, Confidence

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

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
        for i, df in enumerate(df_list, start=1):
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

class Tendencias:
    """
    Clase para la obtención de tendencias nacionales y departamentales
    a partir de encuestas ENAPRES o ENAHO.

    Esta clase instancia dinámicamente la clase de tendencias apropiada 
    (`TendenciasEnapres` o `TendenciasEnaho`) en función del tipo de 
    encuesta especificado, y expone métodos para obtener resultados 
    agregados a nivel nacional y departamental.

    Parameters
    ----------
    data_source : list[str | Path]
        Rutas de las bases de datos utilizada para calcular las tendencias.
    target_variable_id : str
        ID de la variable objetivo sobre la cual se generan las tendencias.
    question_type : str
        Tipo de pregunta asociada a la variable objetivo.
    output_dir : str | Path, optional
        Directorio de salida para almacenar los resultados generados.
    encuesta : {"enapres", "enaho"}, default="enapres"
        Tipo de encuesta a procesar.

    Attributes
    ----------
    tendencia_class : TendenciasABC or None
        Instancia de la clase de tendencias correspondiente a la encuesta.

    Methods
    -------
    get_national_trends(): pd.DataFrame
        Retorna un DataFrame con tendencias a nivel nacional.
    get_departament_trends(): pd.DataFrame
        Retorna un DataFrame con tendencias a nivel departamental.
    """
    def __init__(
        self,
        data_source=list[str | Path] | Downloader,
        target_variable_id="",
        question_type="dummy",
        output_dir: Optional[str | Path] = None,
        encuesta: Literal["enapres", "enaho"] = "enapres",
    ):
        self.data_source= data_source
        self.target_variable_id= target_variable_id
        self.question_type= question_type
        self.output_dir= output_dir
        self.encuesta= encuesta

        self.tendencia_class: TendenciasABC = None

    def _assert_encuesta(self):
        if self.encuesta == "enapres":
            self.tendencia_class = TendenciasEnapres(
                data_source=self.data_source,
                target_variable_id=self.target_variable_id,
                question_type=self.question_type,
                output_dir=self.output_dir,
            )
        elif self.encuesta == "enaho":
            self.tendencia_class = TendenciasEnaho(
                data_source=self.data_source,
                target_variable_id=self.target_variable_id,
                question_type=self.question_type,
                output_dir=self.output_dir,
            )
    
    def get_national_trends(self)-> pd.DataFrame:
        self._assert_encuesta()
        pd.options.mode.copy_on_write = True
        df = self.tendencia_class.get_national_trends()
        pd.options.mode.copy_on_write = False
        logging.info(f"Se terminó")
        return df
      
    def get_departament_trends(self)-> pd.DataFrame:
        self._assert_encuesta()
        pd.options.mode.copy_on_write = True
        df = self.tendencia_class.get_department_trends()
        pd.options.mode.copy_on_write = False
        logging.info(f"Se terminó")
        return df
        

    # def preprocess_dataframe(self):
    #     self._remove_nas()
    #     self._add_departamentos()
    #     self._filter_by_variable()
    #     self._group_by_departamento()
    #     #self.df = self.df[self.target_variable_id].value_counts(normalize=True) * 100
    #     return self.df