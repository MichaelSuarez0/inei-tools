from enum import Enum


# TODO: Falta hasta el 2004
# TODO: Desde el 2020 recién tienen csv Y CAMBIARON LOS CÓDIGOS DE MÓDULO
class Endes(Enum):
    M1629_CARACTERISTICAS_HOGAR = "1629"
    M1630_CARACTERISTICAS_VIVIENDA = "1630"
    M1631_DATOS_BASICOS_MEF = "1631"
    M1632_HISTORIA_NACIMIENTO_TABLA_CONOCIMIENTO_METODO = "1632"
    M1633_EMBARAZO_PARTO_PUERPERIO_LACTANCIA = "1633"
    M1634_INMUNIZACION_SALUD = "1634"
    M1635_NUPCIALIDAD_FECUNDIDAD_CONYUGE_MUJER = "1635"
    M1636_CONOCIMIENTO_SIDA_USO_CONDON = "1636"
    M1637_MORTALIDAD_MATERNA_VIOLENCIA_FAMILIAR = "1637"
    M1638_PESO_TALLA_ANEMIA = "1638"
    M1639_DISCIPLINA_INFANTIL = "1639"
    M1640_ENCUESTA_SALUD = "1640"
    M1641_PROGRAMAS_SOCIALES = "1641"

    # YEAR_TO_ENCUESTA = {
    #     "2024": "968",
    #     "2023": "910",
    #     "2022": "786",
    #     "2021": "760",
    #     "2020": "739",
    #     "2019": "691",
    #     "2018": "638",
    #     "2017": "605",
    #     "2016": "548",
    #     "2015": "504",
    #     "2014": "441",
    #     "2013": "407",
    #     "2012": "323",
    # }

    # OLD_MODULE_MAP = {
    #     "1629": "64",
    #     "1630": "65",
    #     "1631": "66",
    #     "1632": "67",
    #     "1633": "69",
    #     "1634": "70",
    #     "1635": "71",
    #     "1636": "72",
    #     "1637": "73",
    #     "1638": "74",
    #     "1639": "413",
    #     "1640": "414",
    #     "1641": "569",
    # }

    # @classmethod
    # def get_encuesta_code(cls, anio: str) -> str:
    #     return cls.YEAR_TO_ENCUESTA.value[anio]
    
    # @classmethod
    # def get_module_code(cls, modulo: str, anio: str) -> str:
    #     anio = int(anio)
    #     if anio > 2019:
    #         return modulo
    #     else:
    #         return cls.OLD_MODULE_MAP.value[modulo]
    
