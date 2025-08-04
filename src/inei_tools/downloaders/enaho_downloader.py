import logging
from pathlib import Path
from .inei_downloader import Downloader
from ..encuestas import Encuesta


logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

class EnahoDownloader:
    """
    Wrapper alrededor de la librería enahodata de Maykol Medrano
    con configuraciones predeterminadas para EnahoTrends.

    Enlace al repositorio original: https://github.com/MaykolMedrano/enahodata_py
    """
    base_file_name = "Enaho01B-{}-1".lower()

    def __init__(self, years: list[int], modulos: list[Encuesta|str], output_dir: str = "."):
        self.years = years
        self.files = [self.base_file_name.format(year) for year in self.years]
        self.modulos = modulos
        self.output_dir = Path(output_dir)
        self.filename_df_dict = {}
        self.downloader = None
        

    def download_all(self)-> list[Path]:
        ed = Downloader(
            modulos=self.modulos,
            anios= self.years,
            output_dir=self.output_dir,
            file_type="csv",
            data_only=True,
            descomprimir=True,
            overwrite=True,
            parallel_downloads=True,
        )
        path_list = ed.download_all()
        # for datos in diccionario.values():
        #     for filename, df in datos.items():
        #         self.filename_df_dict[filename] = df

        return path_list
