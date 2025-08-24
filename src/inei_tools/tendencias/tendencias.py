from typing import Literal, Optional, OrderedDict
from functools import reduce
import pandas as pd
from pathlib import Path
import logging
from ..downloaders import Downloader
from .question_type import Dummy, Confidence
from ..cleaners import EncuestaCleaner
from functools import wraps

def deactivate_warnings(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        pd.options.mode.copy_on_write = True
        try:
            return func(*args, **kwargs)
        finally:
            pd.options.mode.copy_on_write = False
    return wrapper
    

# TODO: Rust script for File Manager
# TODO: Evaluate data_source as list[pd.DataFrame] (drawback: no filenames to extract years)
class Tendencias:
    """
    Clase para la obtención de tendencias nacionales y departamentales
    a partir de encuestas del INEI.

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

    Methods
    -------
    get_national_trends(): pd.DataFrame
        Retorna un DataFrame con tendencias a nivel nacional.
    get_departament_trends(): pd.DataFrame
        Retorna un DataFrame con tendencias a nivel departamental.
    """
    def __init__(
        self,
        encuesta: Literal["enaho", "enapres", "endes"],
        data_source: list[Path] | Downloader | None = None,
        target_variable_id: str = "",
        # question_type: Literal["dummy", "confidence"] = "dummy",
        output_dir: str = ".",
    ):
        self.data_source = data_source
        self.variable_id = target_variable_id.upper()
        
        self.question_type = None
        self.output_dir = Path(output_dir)

        self.cleaner = EncuestaCleaner(encuesta)
        self.cleaner.target_variable_id= self.variable_id

        self.filename_df_dict = OrderedDict()
        self.downloader = None
        self.df_list_clean = []

    def _obtain_data_if_needed(self):

        if isinstance(self.data_source, Downloader):
            self.downloader = self.data_source
            self.downloader.overwrite = False
            path_list = self.downloader.download_all()
            self.filename_df_dict = {
                path.name: self.cleaner._load_into_memory(path) for path in path_list
            }

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
                self.filename_df_dict = {
                    path.name: self.cleaner._load_into_memory(path)
                    for path in self.data_source
                }
            # elif all(isinstance(data, pd.DataFrame) for data in self.data_source):

        else:
            raise TypeError(
                "Se debe definir o la data (lista de paths o str) "
                "o los años y los modulos para descargar"
            )


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

    @deactivate_warnings
    def get_national_trends(self, output_path: Optional[Path] = None):
        for filename, df in self.filename_df_dict.items():
            logging.info(f"Cleaning {filename}")
            self.cleaner.initialize(df).remove_nas().add_departamentos().filter_by_variable().count_categories(with_factor=False)
           
            self.df_list_clean.append(self.cleaner.df)
        
        merged_df = self._merge_dfs(self.df_list_clean)

        #final_df = transpose(final_df)
        if output_path:
            self._export_to_excel(output_path, merged_df)
        return merged_df
    
    @deactivate_warnings
    def get_department_trends(self, output_path: Optional[Path] = None):
        for filename, df in self.filename_df_dict.items():
            logging.info(f"Cleaning {filename}")
            self.cleaner.initialize(df).remove_nas().add_departamentos().group_by_departamento().to_row_percentage()
           
            self.df_list_clean.append(self.cleaner.df)
        
        merged_df = self._concat_dfs(self.df_list_clean)

        # #final_df = transpose(final_df)
        if output_path:
            self._export_to_excel(output_path, merged_df)
        return merged_df

