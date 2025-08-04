from enum import Enum

class Enaho(Enum):
    M01_CARACTERISTICAS_VIVIENDA_HOGAR = "01"
    M02_CARACTERISTICAS_MIEMBROS_HOGAR = "02"
    M03_EDUCACION = "03"
    M04_SALUD = "04"
    M05_EMPLEO_INGRESOS = "05"
    M07_GASTOS_ALIMENTOS_BEBIDAS = "07"
    M08_INSTITUCIONES_BENEFICAS = "08"
    M09_MANTENIMIENTO_VIVIENDA = "09"
    M10_TRANSPORTES_COMUNICACIONES = "10"
    M11_SERVICIOS_VIVIENDA = "11"
    M12_ESPARCIMIENTO_CULTURA = "12"
    M13_VESTIDO_CALZADO = "13"
    M15_GASTOS_TRANSFERENCIAS = "15"
    M16_MUEBLES_ENSERE = "16"
    M17_OTROS_BIENES_SERVICIOS = "17"
    M18_EQUIPAMIENTO_HOGAR = "18"
    M22_PRODUCCION_AGRICOLA = "22"
    M23_SUBPRODUCTOS_AGRICOLAS = "23"
    M24_PRODUCCION_FORESTAL = "24"
    M25_GASTOS_ACTIVIDADES_AGRICOLAS_FORESTALES = "25"
    M26_PRODUCCION_PECUARIA = "26"
    M27_SUBPRODUCTOS_PECUARIOS = "27"
    M28_GASTOS_ACTIVIDADES_PECUARIAS = "28"
    M34_SUMARIAS = "34"
    M37_BIENES_SERVICIOS_CUIDADOS_PERSONALES = "37"
    M84_PARTICIPACION_CIUDADANA = "84"
    M85_GOBERNABILIDAD_DEMOCRACIA_TRANSPARENCIA = "85"
    M1825_BENEFICIARIOS_INSTITUCIONES_SIN_LUCRO = "1825"

    # YEAR_TO_ENCUESTA = {
    #     "2024": "966",
    #     "2023": "906",
    #     "2022": "784",
    #     "2021": "759",
    #     "2020": "737",
    #     "2019": "687",
    #     "2018": "634",
    #     "2017": "603",
    #     "2016": "546",
    #     "2015": "498",
    #     "2014": "440",
    #     "2013": "404",
    #     "2012": "324",
    #     "2011": "291",
    #     "2010": "279",
    #     "2009": "285",
    #     "2008": "284",
    #     "2007": "283",
    #     "2006": "282",
    #     "2005": "281",
    #     "2004": "280",
    # }

    # @classmethod
    # def get_encuesta_code(cls, anio: str) -> str:    
    #     return cls.YEAR_TO_ENCUESTA.value[anio]
        
    # @classmethod
    # def get_module_code(cls, modulo: str, anio: str = "") -> str:
    #     return modulo
        

class EnahoPanel(Enum):
    M1474_PANEL_CARACTERISTICAS_VIVIENDA_HOGAR = "1474"
    M1475_PANEL_EDUCACION = "1475"
    M1476_PANEL_SALUD = "1476"
    M1477_PANEL_EMPLEO_INGRESOS = "1477"
    M1478_PANEL_SUMARIAS = "1478"
    M1479_PANEL_CARACTERISTICAS_MIEMBROS_HOGAR = "1479"

    YEAR_TO_ENCUESTA = {
        "2023": "912",
        "2022": "845",
        "2021": "763",
        "2020": "743",
        "2019": "699",
        "2018": "651",
        "2017": "612",
        "2016": "614",
        "2015": "529",
        "2011": "302",
    }

    # @classmethod
    # def get_encuesta_code(cls, anio: str) -> str:
    #     return cls.YEAR_TO_ENCUESTA[anio]
        
    # @classmethod
    # def get_module_code(cls, modulo: str, anio: str = "") -> str:
    #     return modulo
        
        
