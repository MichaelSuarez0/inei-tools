from typing import Protocol

class Encuesta(Protocol):
    """Protocol defining the interface for encuesta enums"""
    YEAR_MAP: dict[str, str]

    @classmethod
    def get_encuesta_code(cls, anio: str) -> str:
        pass
    
    @classmethod
    def get_module_code(cls, modulo: str, anio: str = "") -> str:
        pass
        