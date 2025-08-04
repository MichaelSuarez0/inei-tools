class DataExtractionError(Exception):
    """Error base para problemas de extracción de datos"""
    pass

class NoFilesExtractedError(DataExtractionError):
    """Error cuando no se extrajeron archivos del ZIP"""
    pass

class EmptyZipFileError(DataExtractionError):
    """Error cuando el archivo ZIP está vacío o corrupto"""
    pass

class UnsupportedFileTypeError(DataExtractionError):
    """Error cuando no se encuentran archivos del tipo esperado"""
    pass
