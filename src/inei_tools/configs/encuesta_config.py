
from abc import ABC
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

@dataclass
class EncuestaConfig(ABC):
    """Configuración base para todas las encuestas"""
    #modules: List[Union[Enaho, Enapres, Endes]] = field(default_factory=list)
    
    # Columnas estándar (se pueden override por encuesta)
    factor_column: str = "FACTOR"
    year_column: str = "AÑO"
    ubigeo_column: str = "UBIGEO"
    
    # Configuraciones específicas por encuesta
    _survey_specific: dict[str, Any] = field(default_factory=dict)


class EnahoConfig(EncuestaConfig):
    factor_column = "FACTOR07" # Considerar también FACTORA07

class EnapresConfig(EncuestaConfig):
    year_column = "ANIO"
    ubigeo_column = "NOMBREDD"
    factor_column = "P130ZA" # Este es el factor de expansión anual para enapres_100 según alvaro, revisar

class EndesConfig(EncuestaConfig):
    year_column = "ID1"

class EncuestaType(Enum):
    ENAHO = "enaho"
    ENAPRES = "enapres" 
    ENDES = "endes"