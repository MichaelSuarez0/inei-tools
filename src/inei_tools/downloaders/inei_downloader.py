from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Literal, Optional, Union
import warnings
import requests
import zipfile
import logging
import shutil
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import Timeout, ConnectionError
from ..encuestas import Encuesta, Endes
from .exceptions import NoFilesExtractedError, FormatoNoDisponibleError
from .db_manager import DBManager, Queries

# Para forzar conexiones IPv4
requests.packages.urllib3.util.connection.HAS_IPV6 = False

@dataclass
class ArchivoINEI:
    año: str
    encuesta_name: str
    codigo_encuesta: str
    modulo: str
    codigo_modulo: str
    file_path: Optional[Path] = None
    status: Literal["exists", "download"] = "download"


# NOTE: el codigo_modulo puede ser único o repetirse con el año, y siempre se utiliza para descargar
# NOTE: en cambio, el capítulo siempre se repite con el año y solo sirve para el usuario; no sirve para descargar
# TODO: Verificar lo de "se descargaron 0 archivos" y raise ValueError
# TODO: Diagnosticar Permission Error
# TODO: overwrite=True no funciona si es un zip y si se coloca data_only, tal vez considerar poner data_only=False si no se descomprime o al revés
# TODO: Considerar descargar solo pdfs
# TODO: Quitar lo de "Iniciando descarga" si no se va a descargar nada
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
        Lista de años a descargar. Ejemplo: ["2022", "2023"] o range(2020, 2024). En el caso de la Enapres,
        se puede dejar en blanco si se especifica el código módulo (ya que es único para todos los años).
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
    logger : bool | logging.Logger, optional
        Controla el manejo de logs durante la ejecución:
        - Si **False**, no se imprime nada (modo silencioso).
        - Si **True**, se configura un logger básico con nivel INFO y salida a consola.
        - Si se pasa una instancia de `logging.Logger`, se usará dicho logger personalizado.
        Por defecto: True.

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
    FILE_NAME_BASE = "{encuesta}_{modulo}_{anio}{ext}"
    ENCUESTAS = ["enaho", "enaho_panel", "enapres", "endes"]

    def __init__(
        self,
        modulos: int | str | Encuesta | list[str | Encuesta],
        anios: int | str | list[str] | None = None,
        output_dir: str = ".",
        overwrite: bool = False,
        descomprimir: bool = False,
        parallel_downloads: bool = False,
        file_type: Literal["csv", "stata", "dta", "dbf", "spss", "stata"] = "csv",
        data_only: bool = False,
        logger: Union[bool, logging.Logger] = True,
    ):
        self.modulos = modulos
        self.anios = anios if anios is not None else []
        self.descomprimir = descomprimir
        self.output_dir = Path(output_dir)
        self.overwrite = overwrite
        self.parallel_downloads = parallel_downloads
        self.file_type = file_type.lower()
        self.data_only = data_only
        self.encuesta = None

        # Configuración de logger según lo que pase el usuario
        if isinstance(logger, logging.Logger):
            self.logger = logger
        elif logger is True:
            logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
            self.logger = logging.getLogger(self.__class__.__name__)
        else:
            self.logger = logging.getLogger(self.__class__.__name__)
            self.logger.addHandler(logging.NullHandler())

        self._assert_types()
        self.archivos_a_descargar: list[ArchivoINEI] = []
        self.downloaded_files: set[Path] = set()
        self.db: DBManager = None

    def _assert_types(self) -> None:
        # Conversión de file_type
        if self.file_type in ("dta", "stata"):
            self.ext = "dta"
            self.file_type = "stata"
        elif self.file_type in ("spss", "sav"):
            self.file_type = "spss"
            self.ext = "sav"
        else:
            self.ext = self.file_type
        self.ext = f".{self.ext}"

        # Assert data_only y self.descomprimir
        if self.data_only == True and self.descomprimir == False:
            warnings.warn(
                "Opción 'data_only' activada: la descompresión se habilitó automáticamente para extraer los archivos de datos."
            )
            self.descomprimir = True


        # Años
        if self.anios:
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
                    raise ValueError(
                        f"Año fuera de rango permitido (2000-2025): {anio}"
                    )

        # Módulos
        # Conversiones básicas
        if isinstance(self.modulos, Iterable):
            self.modulos = list(self.modulos)
        elif not isinstance(self.modulos, list):
            self.modulos = [self.modulos]

        if all(isinstance(modulo, int) for modulo in self.modulos):
            self.modulos = [str(modulo) for modulo in self.modulos]

        # Assertions
        if all(isinstance(modulo, Enum) for modulo in self.modulos):
            if self.modulos[0].__class__.__name__ == "Endes":
                old_map = Endes.OLD_MODULE_MAP.value
                modulos_converted = []
                for año in self.anios:
                    for modulo in self.modulos:
                        modulos_converted.append(
                            old_map[modulo.value] if int(año) < 2020 else modulo.value
                        )
                self.modulos = modulos_converted
            else:
                self.modulos = [modulo.value for modulo in self.modulos]

        elif all(isinstance(modulo, str) for modulo in self.modulos):
            self.modulos = [
                modulo.zfill(2) if len(modulo) == 1 else modulo
                for modulo in self.modulos
            ]

            if not all(2 <= len(modulo) <= 4 for modulo in self.modulos):
                raise ValueError("Los modulos deben tener un longitud entre 2 y 4")

        else:
            raise TypeError(
                "Modulos debe ser una lista de Encuesta, str o int; no combinar tipos"
            )

    def _get_archivo_inei(self, codigo_o_modulo: str, año: str) -> ArchivoINEI:
        values = self.db.execute_query(
            Queries.get_encuesta_metadata(año, codigo_o_modulo)
        )
        if not values:
            # Para el caso de que se ingresó el código_modulo de Enapres
            values = self.db.execute_query(
                Queries.get_encuesta_metadata_from_module(año, codigo_o_modulo)
            )
            if not values:
                raise ValueError(
                    f"No se encontraron resultados para el módulo {codigo_o_modulo} del año {str(año)}."
                )
        values = values[0]
        archivo_inei = ArchivoINEI(
            año=año,
            encuesta_name=values[0],
            codigo_encuesta=values[1],
            modulo=values[2],
            codigo_modulo=values[3],
        )

        return archivo_inei

    def _conect_to_db(self):
        self.db = DBManager()
        self.db.connect("encuestas")

    # def _convert_modulos_to_encuesta(self):
    #     # Convert to Encuesta
    #     converted_modulos = []
    #     for modulo in self.modulos:
    #         self._conect_to_db()
    #         found = False
    #         for encuesta in [Enaho, EnahoPanel, Enapres, Endes]:
    #             try:
    #                 enum = encuesta(modulo)
    #                 converted_modulos.append(enum)
    #                 found = True
    #                 break
    #             except ValueError:
    #                 continue
    #         if not found:
    #             raise ValueError(
    #                 f"Modulo '{modulo}' no se encontró en ninguna encuesta"
    #             )

    #     self.modulos = converted_modulos

    def download_all(self) -> list[Path]:
        # Crear la carpeta de salida
        self.output_dir.mkdir(exist_ok=True)
        self._conect_to_db()
        no_disponible = set()

        # Resolver años si se escogen módulos de Enapres sin especificar años
        if not self.anios:
            self.anios = []
            for codigo_o_modulo in self.modulos:
                anio = self.db.execute_query(
                    Queries.get_año_from_module_code(codigo_o_modulo)
                )
                if anio and anio[0][0] not in self.anios:
                    self.anios.append(anio[0][0])

        # Bucle principal
        for codigo_o_modulo in self.modulos:
            # Obtener todas las variables necesarias
            if not self.anios:
                anio = self.db.execute_query(
                    Queries.get_año_from_module_code(codigo_o_modulo)
                )
                self.anios.append(anio[0][0])
            for anio in self.anios:
                archivo_inei = self._get_archivo_inei(codigo_o_modulo, anio)
                # Verificar que existan los formatos antes de descargar
                errors = self.db.execute_query(
                    Queries.verify_download_format(
                        codigo_modulo=archivo_inei.codigo_modulo,
                        año=anio,
                        format=self.file_type,
                    )
                )
                if errors:
                    encuesta_error, año_error = errors[0]
                    no_disponible.add((self.ext, encuesta_error, año_error))
                    continue

                file_name = self.FILE_NAME_BASE.format(
                    encuesta=archivo_inei.encuesta_name.lower(),
                    modulo=archivo_inei.modulo,
                    anio=anio,
                    ext=self.ext if self.data_only else ".zip",
                )
                if not self.data_only and self.descomprimir:
                    target_path = self.output_dir / file_name.split(".")[0]
                else:
                    target_path = self.output_dir / file_name

                archivo_inei.file_path = target_path
                self.archivos_a_descargar.append(archivo_inei)

        if no_disponible:
            raise FormatoNoDisponibleError(no_disponible)

        self._assert_overwrite()
        # ic(self.archivos_a_descargar)

        if self.parallel_downloads:
            self._download_parallel()
        else:
            self._download_sequential()

        downloaded_files = list(self.downloaded_files)
        downloaded_files.sort(reverse=False)
        return downloaded_files

    # @staticmethod
    # def _sort_paths(self, paths: list[Path])-> list[Path]:
    #     for path in paths:

    def _assert_overwrite(self):
        for archivo_inei in self.archivos_a_descargar:
            file_path = archivo_inei.file_path
            dir_path = file_path.parent
            base_name = file_path.stem

            original_exists = file_path.exists()
            variants_exist = any(
                p.suffix == self.ext and p.stem.startswith(base_name + "_")
                for p in dir_path.iterdir()
            )

            # Si existe algo y overwrite=True → borrar original + variantes
            if self.overwrite:
                if original_exists or variants_exist:
                    for p in dir_path.iterdir():
                        if p.suffix == self.ext and (
                            p.stem == base_name or p.stem.startswith(base_name + "_")
                        ):
                            p.unlink()
                continue

            # overwrite=False → marcar como existente si original o variante existe
            if original_exists and not variants_exist:
                logging.info(
                    f"Archivo '{file_path}' ya existe y overwrite=False. No se descargará de nuevo."
                )
                archivo_inei.status = "exists"
                self.downloaded_files.add(file_path)

            elif original_exists and variants_exist:
                logging.info(
                    f"Archivo '{file_path}' y variantes ya existen y overwrite=False. No se descargará de nuevo."
                )
                archivo_inei.status = "exists"
                self.downloaded_files.add(file_path)
                self.downloaded_files.add(
                    next(
                        p
                        for p in dir_path.iterdir()
                        if p.suffix == self.ext and p.stem.startswith(base_name + "_")
                    )
                )

            elif not original_exists and variants_exist:
                warnings.warn(
                    f"Existe variante(s) de '{file_path}' y overwrite=False. No se descargará de nuevo."
                )
                archivo_inei.status = "exists"
                self.downloaded_files.add(
                    next(
                        p
                        for p in dir_path.iterdir()
                        if p.suffix == self.ext and p.stem.startswith(base_name + "_")
                    )
                )

    def _download_parallel(self):
        completed = 0
        if all(archivo_inei.status == "exists" for archivo_inei in self.archivos_a_descargar):
            self._print_success_message(completed, full=False)
            return None
        else:
            self.logger.info(f"🚀 Iniciando descarga paralela")
            self.logger.info("-" * 60)
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
        completed = 0
        if all(archivo_inei.status == "exists" for archivo_inei in self.archivos_a_descargar):
            self._print_success_message(completed, full=False)
            return None
        else:
            self.logger.info(f"🚀 Iniciando descarga secuencial")
            self.logger.info("-" * 60)

        for archivo_inei in self.archivos_a_descargar:
            if archivo_inei.status == "exists":
                continue
            self._download_zip(archivo_inei)
            completed += 1

        self._print_success_message(completed)


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
            encuesta=archivo_inei.encuesta_name,
            modulo=archivo_inei.modulo,
            anio=archivo_inei.año,
            ext=".zip",
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
                        f"Error al descargar {zip_path.name}. No se encontró el URL, verifica si '{self.file_type}' está disponible para el {archivo_inei.año}."
                    )
                    logging.info(URL)

        except (Timeout, ConnectionError) as e:
            raise ConnectionError(
                f"Error al descargar {zip_path.name}. El servidor tardó mucho en responder, verifica tu internet e intenta nuevamente en unos minutos"
            )
        except requests.exceptions.RequestException as e:
            logging.error(f"Error durante la conexión o la descarga: {e}")

        return None

    def _print_success_message(self, completed, full=True):
        if not self.downloaded_files:
            raise NoFilesExtractedError("No se extrajeron archivos, revisar errores.")
        else:
            sample_file = next(iter(self.downloaded_files))
            if full:
                archivos_a_descargar = [
                    archivo
                    for archivo in self.archivos_a_descargar
                    if archivo.status != "exists"
                ]
                self.logger.info("-" * 60)
                self.logger.info(
                    f"Descarga completada: {completed}/{len(archivos_a_descargar)} zips descargados"
                )
            if sample_file.is_dir():
                self.logger.info(
                    f"🎉 Se obtuvieron {len(self.downloaded_files)} carpetas en total"
                )
            elif sample_file.is_file():
                self.logger.info(
                    f"🎉 Se obtuvieron {len(self.downloaded_files)} archivos en total"
                )
            if not all(isinstance(h, logging.NullHandler) for h in self.logger.handlers):
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
            exist_index = 1
            for zinfo in zip_ref.infolist():
                if zinfo.is_dir():
                    continue

                # Borrar directorios dentro del zip
                filename = Path(zinfo.filename).name
                if not filename:
                    continue

                # Determinar la ruta de destino donde se guardará el archivo extraído (dependiendo si es dir o archivo)
                if archivo_inei.file_path.suffix:
                    file_path = archivo_inei.file_path.parent / filename
                else:
                    file_path = archivo_inei.file_path / filename

                if self.data_only:
                    if not filename.lower().endswith(f"{self.ext}"):
                        continue
                    # Para no sobreescribir archivos (no sé por qué el INEI a veces divide una base de datos en varios archivos)
                    path_to_assert = archivo_inei.file_path
                    if path_to_assert.exists():
                        file_path = file_path.with_name(
                            f"{path_to_assert.stem}_{exist_index}{path_to_assert.suffix}"
                        )
                        exist_index += 1
                    else:
                        file_path = file_path.with_name(path_to_assert.name)

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
