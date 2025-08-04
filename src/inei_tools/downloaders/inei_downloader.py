from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import sqlite3
from typing import Literal
import requests
import zipfile
import logging
from tqdm import tqdm
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..encuestas import Encuesta, Enaho, EnahoPanel, Enapres, Endes
from requests.exceptions import Timeout, ConnectionError
from .exceptions import NoFilesExtractedError

from icecream import ic

# Para forzar conexiones IPv4
requests.packages.urllib3.util.connection.HAS_IPV6 = False

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


@dataclass
class ArchivoINEI:
    anio: str
    codigo_encuesta: str
    codigo_modulo: str
    encuesta_name: str
    file_path: Path
    status: Literal["exists", "download"] = "download"


class DBManager:
    RESOURCES_PATH = Path(__file__).parent.parent / "resources"

    def __init__(self):
        self.cursor: sqlite3.Cursor = None
        self.conn: sqlite3.Connection = None

    def connect(self, db_name: str):
        database = self.RESOURCES_PATH / f"{db_name}.sqlite"
        self.conn = sqlite3.connect(database)
        self.cursor = self.conn.cursor()

    @staticmethod
    def get_encuesta_code(año: str):
        return f"SELECT codigo_encuesta FROM modulos WHERE año == {str(año)} LIMIT 1"

    @staticmethod
    def get_module_code(año: str, modulo: str):
        return f"SELECT codigo_modulo FROM modulos WHERE modulo = '{str(modulo)}' AND año = {str(año)}"

    @staticmethod
    def verify_download_format(años: list[str], format: str):
        años_str = ", ".join(años)
        return f"""
            SELECT DISTINCT año
            FROM modulos
            WHERE {format} = 0 AND año IN ({años_str})
        """

    def execute_query(self, query: str):
        self.cursor.execute(query)
        resultado = self.cursor.fetchall()
        return resultado


# TODO: Que al descomprimir un zip con todos los archivos, de todas formas se cambie el nombre del csv
# TODO: Verificar lo de "se descargaron 0 archivos" y raise ValueError
# TODO: Diagnosticar Permission Error
class Downloader:
    """
    Clase principal para descargar módulos de encuestas del INEI (ENAHO, ENAPRES, ENDES),
    con opciones para descomprimir, seleccionar formato de archivo y realizar descargas en paralelo.

    Parameters
    ----------
    modulos : list[str | Encuesta]
        Lista de módulos a descargar. Pueden especificarse como strings (ej. "01") o como
        miembros de las clases `Enaho`, `EnahoPanel`, `Enapres` o `Endes`.
    anios : list[str]
        Lista de años a descargar. Ejemplo: ["2022", "2023"] o range(2020, 2024).
    output_dir : str, optional
        Carpeta donde se guardarán los archivos descargados. Por defecto: carpeta actual (`"."`).
    overwrite : bool, optional
        Determina si se deben sobrescribir los archivos existentes.
        - Si **True**, descarga y reemplaza cualquier archivo previamente guardado.
        - Si **False**, omite la descarga de archivos ya presentes y simplemente retorna sus rutas.
        Por defecto: False.
    descomprimir : bool, optional
        Si True, descomprime los archivos ZIP descargados y los guarda en una carpeta. Por defecto: False.
    parallel_downloads : bool, optional
        Si True, activa la descarga en paralelo utilizando múltiples hilos. Por defecto: False.
    file_type : {"csv", "dta", "stata", "dbf"}, optional
        Formato de archivo a descargar. Usa "stata" como alias de "dta". Por defecto: "csv".
    data_only : bool, optional
        Si True, conserva únicamente el archivo de datos con extensión especificada (ignora otros).
        Solo tiene efecto cuando `descomprimir=True`. Por defecto: False.

    Attributes
    ---------
    archivos_a_descargar : list[ArchivoINEI]
        Lista de objetos que representan cada módulo/año a descargar.
    downloaded_files : list[Path]
        Lista de rutas a los archivos o carpetas descargados exitosamente.
    encuesta_name : str
        Nombre de la encuesta inferido a partir del primer módulo (ej. "Enaho").
    exceptions : list[Exception]
        Lista de excepciones capturadas durante el proceso de descarga.

    Retorna
    -------
    list[Path]  
        Lista de rutas a los archivos o carpetas descargadas exitosamente.  
        El contenido depende de los parámetros utilizados:

        - Si `descomprimir=False`: retorna archivos ZIP descargados.
        - Si `descomprimir=True`: retorna carpetas extraídas (una por módulo/año).
        - Si `data_only=True`: retorna únicamente archivos de datos (.csv, .dta, etc.) sin subcarpetas.

    Ejemplos
    --------
    Descargar módulos CSV ya descomprimidos y con solo los archivos de datos:

    >>> downloader = Downloader(
    ...     modulos=Enaho.M85_GOBERNABILIDAD_DEMOCRACIA_TRANSPARENCIA,
    ...     anios=[2022, 2023],
    ...     output_dir="data/",
    ...     descomprimir=True,
    ...     overwrite=True,
    ...     file_type="csv",
    ...     data_only=True,
    ...     parallel_downloads=True,
    ... )
    >>> archivos = downloader.download_all()

    Descargar archivos ZIP en formato Stata sin descomprimir:

    >>> downloader = Downloader(
    ...     modulos=Enaho.M01_CARACTERISTICAS_VIVIENDA_HOGAR,
    ...     anios=range(2020, 2024),
    ...     output_dir="zips/",
    ...     descomprimir=False,
    ...     overwrite=True,
    ...     file_type="stata",
    ...     data_only=False,
    ...     parallel_downloads=False,
    ... )
    >>> archivos = downloader.download_all()

    Notas
    -----
    Esta clase es una versión extendida del proyecto original `enahodata`,
    desarrollado por Maykol Medrano.
    Repositorio original: https://github.com/MaykolMedrano/enahodata_py
    PyPI: https://pypi.org/project/enahodata/

    Esta implementación permite descarga paralela, manejo de errores, verificación de formatos
    disponibles por año y descompresión con aplanamiento automático de carpetas.

    Para obtener los módulos disponibles, consulta los enumerados:
    `inei.Enaho`, `inei.EnahoPanel`, `inei.Enapres` o `inei.Endes`.
    """

    BASE_URL = "https://proyectos.inei.gob.pe/iinei/srienaho/descarga/{file_type}/{encuesta_code}-Modulo{modulo}.zip"
    FILE_NAME_BASE = "{encuesta}_{modulo}_{anio}.{ext}"

    def __init__(
        self,
        modulos: list[str | Encuesta],
        anios: list[str],
        output_dir: str = ".",
        overwrite: bool = False,
        descomprimir: bool = False,
        parallel_downloads: bool = False,
        file_type: Literal["csv", "stata", "dta", "dbf"] = "csv",
        data_only: bool = False,
    ):
        self.modulos = modulos
        self.anios = anios
        self.descomprimir = descomprimir
        self.output_dir = Path(output_dir)
        self.overwrite = overwrite
        self.parallel_downloads = parallel_downloads
        self.file_type = "stata" if file_type == "dta" else file_type
        self.data_only = data_only
        self.encuesta = None

        self._assert_types()
        self.exceptions = []
        self.archivos_a_descargar: list[ArchivoINEI] = []
        self.downloaded_files: set[Path] = set()
        self.db: DBManager = None

    def _assert_types(self) -> None:
        # Años
        if isinstance(self.anios, Iterable):
            self.anios = list(self.anios)
        elif not isinstance(self.anios, list):
            self.anios = [self.anios]
        else:
            raise TypeError("`anios` debe ser un int, str o un iterable")

        self.anios = [str(anio) for anio in self.anios]

        # Validar rango
        for anio in self.anios:
            if not anio.isdigit() or not (2000 <= int(anio) <= 2025):
                raise ValueError(f"Año fuera de rango permitido (2000-2025): {anio}")

        # Módulos
        if not isinstance(self.modulos, list):
            self.modulos = [self.modulos]

        if all(isinstance(modulo, int) for modulo in self.modulos):
            self.modulos = [str(modulo) for modulo in self.modulos]

        if all(isinstance(modulo, str) for modulo in self.modulos):
            self.modulos = [
                modulo.zfill(2) if len(modulo) == 1 else modulo
                for modulo in self.modulos
            ]

            if not all(2 <= len(modulo) <= 4 for modulo in self.modulos):
                raise ValueError("Los modulos deben tener un longitud entre 2 y 4")

            # Convert to Encuesta
            converted_modulos = []
            for modulo in self.modulos:
                found = False
                for encuesta in [Enaho, EnahoPanel, Enapres, Endes]:
                    try:
                        enum = encuesta(modulo)
                        converted_modulos.append(enum)
                        found = True
                        break
                    except ValueError:
                        continue
                if not found:
                    raise ValueError(
                        f"Modulo '{modulo}' no se encontró en ninguna encuesta"
                    )

            self.modulos = converted_modulos

        if all(isinstance(modulo, Enum) for modulo in self.modulos):
            self._obtain_encuesta()
            self.modulos = [modulo.value for modulo in self.modulos]

        else:
            raise TypeError(
                "Modulos debe ser una lista de Encuesta, str o int; no combinar tipos"
            )

    def _obtain_encuesta(self):
        self.encuesta = self.modulos[0].__class__
        self.encuesta_name = self.modulos[0].__class__.__name__.capitalize()
        return self.encuesta

    def _conect_to_db(self):
        self.db = DBManager()
        self.db.connect(self.encuesta_name.lower())

    def download_all(self) -> list[Path]:
        # Elegir diccionario según panel
        logging.info(f"Descargando '{self.encuesta_name}'")

        self._conect_to_db()

        # Crear la carpeta de salida
        self.output_dir.mkdir(exist_ok=True)

        años_sin_formato = self.db.execute_query(
            DBManager.verify_download_format(self.anios, format=self.file_type)
        )
        if años_sin_formato:
            años = [str(a[0]) for a in años_sin_formato]
            años_str = ", ".join(años)
            raise ValueError(
                f"El formato '{self.file_type}' no está disponible para la {self.encuesta_name} para los años {años_str}."
            )

        for anio in self.anios:
            try:
                codigo_encuesta = str(
                    self.db.execute_query(DBManager.get_encuesta_code(anio))[0][0]
                )
            except KeyError:
                raise KeyError(
                    f"No se encontro el año {anio} para la {self.encuesta_name}."
                )
            for modulo in self.modulos:
                ic(modulo)
                codigo_modulo = str(
                    self.db.execute_query(DBManager.get_module_code(anio, modulo))[0][0]
                )
                file_name = self.FILE_NAME_BASE.format(
                    encuesta=self.encuesta_name.lower(),
                    modulo=codigo_modulo,
                    anio=anio,
                    ext=self.file_type if self.data_only else "zip",
                )
                if not self.data_only and self.descomprimir:
                    target_path = self.output_dir / file_name.split(".")[0]
                else:
                    target_path = self.output_dir / file_name

                self.archivos_a_descargar.append(
                    ArchivoINEI(
                        anio=anio,
                        codigo_encuesta=codigo_encuesta,
                        codigo_modulo=codigo_modulo,
                        file_path=target_path,
                        encuesta_name=self.encuesta_name,
                    )
                )

        self._assert_overwrite()
        ic(self.archivos_a_descargar)

        if self.parallel_downloads:
            self._download_parallel()
        else:
            self._download_sequential()

        self.downloaded_files = list(self.downloaded_files)
        self.downloaded_files.sort(reverse=False)
        return self.downloaded_files

    # @staticmethod
    # def _sort_paths(self, paths: list[Path])-> list[Path]:
    #     for path in paths:

    def _assert_overwrite(self):
        for archivo_inei in self.archivos_a_descargar:
            file_path = archivo_inei.file_path

            if file_path.exists() and not self.overwrite:
                logging.info(
                    f"Archivo '{file_path}' ya existe y overwrite=False. No se descargará de nuevo."
                )
                archivo_inei.status = "exists"
                self.downloaded_files.add(file_path)

    def _download_parallel(self):
        print(f"🚀 Iniciando descarga paralela")
        print("-" * 60)
        completed = 0

        with ThreadPoolExecutor(max_workers=5) as executor:
            # Enviar todas las tareas
            future_to_task = {}
            for archivo_inei in self.archivos_a_descargar:
                if archivo_inei.status == "exists":
                    continue
                future = executor.submit(self._download_zip, archivo_inei)
                future_to_task[future] = archivo_inei

            # Procesar resultados conforme se completen
            for future in as_completed(future_to_task):
                archivo_inei = future_to_task[future]
                result = future.result()
                completed += 1

            # if len(self.downloaded_files) == 0:
            #     raise
            self._print_success_message(completed)

    def _download_sequential(self):
        # Descarga secuencial
        print(f"🚀 Iniciando descarga secuencial")
        print("-" * 60)
        completed = 0

        for archivo_inei in self.archivos_a_descargar:
            if archivo_inei.status == "completed":
                continue
            self._download_zip(archivo_inei)
            completed += 1

        self._print_success_message(completed)

        return self.downloaded_files

    def _download_zip(self, archivo_inei: ArchivoINEI):
        """
        Descarga un solo archivo (para un año y un módulo)
        usando el código dado (sea panel o corte transversal),
        y opcionalmente lo descomprime, elimina el .zip,
        aplana la carpeta al extraer, y permite cargar los .dta.

        Retorna:
        --------
        - Si load_dta=True, retorna un diccionario { nombre_archivo: DataFrame, ... }
        - De lo contrario, retorna None.
        """
        # -- Descargar con barra de progreso --
        # start_request = time.time()

        URL = self.BASE_URL.format(
            file_type=self.file_type.upper(),
            encuesta_code=archivo_inei.codigo_encuesta,
            modulo=archivo_inei.codigo_modulo,
        )
        zip_name = self.FILE_NAME_BASE.format(
            encuesta=self.encuesta_name.lower(),
            modulo=archivo_inei.codigo_modulo,
            anio=archivo_inei.anio,
            ext="zip",
        )
        if archivo_inei.file_path.suffix:
            zip_path = archivo_inei.file_path.parent / zip_name
        else:
            archivo_inei.file_path.mkdir(parents=True, exist_ok=True)
            zip_path = archivo_inei.file_path / zip_name

        try:
            with requests.get(URL, stream=True, timeout=5) as r:
                if r.status_code == 200:
                    total_size_in_bytes = int(r.headers.get("content-length", 0))
                    # end_request = time.time()
                    # logging.info(f"El request demoró {(end_request - start_request):.4f}s")
                    desc_tqdm = f"Descargando {zip_path.name}"
                    with open(zip_path, "wb") as f, tqdm(
                        total=total_size_in_bytes,
                        unit="iB",
                        unit_scale=True,
                        desc=desc_tqdm,
                    ) as bar:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                bar.update(len(chunk))

                    # -- Descomprimir si se solicita --
                    if self.descomprimir:
                        self._decompress_and_flatten(archivo_inei, zip_path)
                    else:
                        self.downloaded_files.add(zip_path)

                    # # -- Cargar los .dta si se pide --
                    # if self.load_into_memory:
                    #     self._load_into_memory()

                elif r.status_code == 404:
                    # soup = BeautifulSoup(r.text, "html.parser")
                    # h2 = soup.find("h2")
                    # summary = h2.get_text(strip=True) if h2 else "Sin detalle"
                    logging.error(
                        f"Error al descargar {zip_path.name}. No se encontró el URL, verifica si '{self.file_type}' está disponible para el {archivo_inei.anio}."
                    )
                    logging.info(URL)

        except (Timeout, ConnectionError) as e:
            raise ConnectionError(
                f"Error al descargar {zip_path.name}. El servidor tardó mucho en responder, verifica tu internet e intenta nuevamente en unos minutos"
            )
        except requests.exceptions.RequestException as e:
            logging.error(f"Error durante la conexión o la descarga: {e}")

        return None

    def _print_success_message(self, completed):
        if not self.downloaded_files:
            raise NoFilesExtractedError("Rayos")
        else:
            archivos_a_descargar = [
                archivo
                for archivo in self.archivos_a_descargar
                if archivo.status != "exists"
            ]
            print("-" * 60)
            print(
                f"Descarga completada: {completed}/{len(archivos_a_descargar)} zips descargados"
            )
            sample_file = next(iter(self.downloaded_files))
            if sample_file.is_dir():
                print(
                    f"🎉 Se obtuvieron {len(self.downloaded_files)} carpetas en total"
                )
            elif sample_file.is_file():
                print(
                    f"🎉 Se obtuvieron {len(self.downloaded_files)} archivos en total"
                )
            print()

    def _decompress_and_flatten(self, archivo_inei: ArchivoINEI, zip_path: Path):
        """
        Descarga un solo archivo (para un año y un módulo)
        usando el código dado (sea panel o corte transversal),
        y opcionalmente lo descomprime, elimina el .zip,
        aplana la carpeta al extraer, y permite cargar los .dta.

        Retorna:
        --------
        - Si load_dta=True, retorna un diccionario { nombre_archivo: DataFrame, ... }
        - De lo contrario, retorna None.
        """
        # Directorio de extracción (uno por cada módulo+año, sin subcarpetas anidadas)
        # extract_dir = os.path.join(output_dir, f"modulo_{modulo}_{anio}")
        # os.makedirs(extract_dir, exist_ok=True)

        # Extraer solo archivos deseados (flattened)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            for zinfo in zip_ref.infolist():
                if zinfo.is_dir():
                    continue

                filename = Path(zinfo.filename).name
                if not filename:
                    continue

                if archivo_inei.file_path.suffix:
                    file_path = archivo_inei.file_path.parent / filename
                else:
                    file_path = archivo_inei.file_path / filename

                if self.data_only:
                    if not filename.lower().endswith(f"{self.file_type}"):
                        continue
                    file_path = file_path.with_name(archivo_inei.file_path.name)

                    self.downloaded_files.add(file_path.resolve())
                else:
                    self.downloaded_files.add(archivo_inei.file_path)

                with zip_ref.open(zinfo) as source, open(file_path, "wb") as target:
                    shutil.copyfileobj(source, target)
        # # Guardar la ruta de extracción
        # self.downloaded_files.append(self.output_dir.resolve())

        # if verbose:
        #     logging.info(f"Archivo descomprimido (aplanado) en: {output_dir}")

        # -- Eliminar el .zip una vez descomprimido --
        Path(zip_path).unlink()
        # if verbose:
        #     logging.info(f"Archivo .zip eliminado: {zip_path}")
