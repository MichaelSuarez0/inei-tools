from typing import Literal, Optional, TYPE_CHECKING
import pandas as pd
from pathlib import Path
import logging
from ..downloaders import Downloader
from .t_enapres import TendenciasEnapres
from .t_enaho import TendenciasEnaho
from .t_endes import TendenciasEndes

if TYPE_CHECKING:
    from ._trends_abc import TendenciasABC
    

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

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
        elif self.encuesta == "endes":
            self.tendencia_class = TendenciasEndes(
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
        self.tendencia_class._obtain_data_if_needed()
        try:
            pd.options.mode.copy_on_write = True
            df = self.tendencia_class.get_national_trends()   
            logging.info(f"Se terminó")
        finally:
            pd.options.mode.copy_on_write = False
        return df
      
    def get_department_trends(self)-> pd.DataFrame:
        self._assert_encuesta()
        self.tendencia_class._obtain_data_if_needed()
        try:
            pd.options.mode.copy_on_write = True
            df = self.tendencia_class.get_department_trends()
            logging.info(f"Se terminó")
        finally:
            pd.options.mode.copy_on_write = False
        return df
        

    # def preprocess_dataframe(self):
    #     self._remove_nas()
    #     self._add_departamentos()
    #     self._filter_by_variable()
    #     self._group_by_departamento()
    #     #self.df = self.df[self.target_variable_id].value_counts(normalize=True) * 100
    #     return self.df