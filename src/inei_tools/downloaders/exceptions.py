from typing import Tuple


class DataExtractionError(Exception):
    """Error base para problemas de extracción de datos"""
    pass

class FormatoNoDisponibleError(DataExtractionError):
    """Error lanzado cuando formatos no están disponibles."""
    def __init__(self, errores: list[Tuple]):
        self.errores = errores  # Ej: [{"file_type": "CSV", "encuesta": "ENOE", "año": 2023}, ...]
        print(errores)
        mensaje = "Errores de formato no disponible:\n"
        mensaje += "\n".join(
            f"- Formato '{e[0]}' no disponible para {e[1]} (año {e[2]})"
            for e in errores
        )
        super().__init__(mensaje)

# # Uso en tu código
# errores = []
# for archivo in archivos_a_procesar:
#     if not formato_disponible(archivo):
#         errores.append({
#             "file_type": archivo.tipo,
#             "encuesta": archivo.encuesta,
#             "año": archivo.año
#         })

# if errores:
#     raise FormatoNoDisponibleError(errores)
class NoFilesExtractedError(DataExtractionError):
    """Error cuando no se extrajeron archivos del ZIP"""
    pass

class EmptyZipFileError(DataExtractionError):
    """Error cuando el archivo ZIP está vacío o corrupto"""
    pass

class UnsupportedFileTypeError(DataExtractionError):
    """Error cuando no se encuentran archivos del tipo esperado"""
    pass
