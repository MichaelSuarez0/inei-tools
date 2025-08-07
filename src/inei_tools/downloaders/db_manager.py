
from pathlib import Path
import sqlite3


class DBManager:
    RESOURCES_PATH = Path(__file__).parent.parent / "resources"

    def __init__(self):
        self.cursor: sqlite3.Cursor = None
        self.conn: sqlite3.Connection = None

    def connect(self, db_name: str):
        database = self.RESOURCES_PATH / f"{db_name}.sqlite"
        self.conn = sqlite3.connect(database)
        self.cursor = self.conn.cursor()

    def execute_query(self, query: str):
        self.cursor.execute(query)
        resultado = self.cursor.fetchall()
        return resultado

class Queries:
    @staticmethod
    def get_encuesta_code(encuesta):
        return f"SELECT codigo_encuesta FROM modulos WHERE encuesta == '{str(encuesta)}' LIMIT 1"
    
    @staticmethod
    def get_año_from_module_code(codigo_modulo: str):
        return f"SELECT año FROM modulos WHERE encuesta = 'enapres' and codigo_modulo == '{str(codigo_modulo)}'"

    @staticmethod
    def get_encuesta_metadata(año: str, codigo_modulo: str):
        return f"SELECT encuesta, codigo_encuesta FROM modulos WHERE año == '{str(año)}' and codigo_modulo == '{str(codigo_modulo)}'"
    
    @staticmethod
    def get_encuesta_metadata_from_module(año: str, codigo_modulo: str):
        return f"SELECT encuesta, codigo_encuesta FROM modulos WHERE año == '{str(año)}' and modulo == '{str(codigo_modulo)}'"

    @staticmethod
    def get_module_code(año: str, modulo: str):
        return f"SELECT codigo_modulo FROM modulos WHERE modulo = '{str(modulo)}' AND año = '{str(año)}'"

    @staticmethod
    def verify_download_format(codigo_modulo: str, año: str, format: str):
        return f"""
            SELECT encuesta, año
            FROM modulos
            WHERE codigo_modulo = '{str(codigo_modulo)}' AND año = '{año}' AND {format} = 0
        """
