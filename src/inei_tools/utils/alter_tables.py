from pathlib import Path
import pandas as pd
import sqlite3



# 1. Cargar el CSV
df = pd.read_csv(r"C:\Users\micha\OneDrive\Python\inei-tools\src\inei_tools\utils\enaho_modulos_2004_2024.csv", dtype={"codigo_modulo": str, "modulo": str})

# 2. Crear o abrir base de datos SQLite
conn = sqlite3.connect(r"C:\Users\micha\OneDrive\Python\inei-tools\src\inei_tools\utils\enaho.sqlite")

# 3. Escribir a una tabla llamada modulos
df.to_sql("modulos", conn, if_exists="replace", index=False, dtype={
    "año": "INTEGER",
    "periodo": "INTEGER",
    "codigo_encuesta": "TEXT",
    "encuesta": "TEXT",
    "codigo_modulo": "TEXT",   # Aquí el fix
    "modulo": "TEXT",          # Aquí el fix
    "spss": "INTEGER",
    "stata": "INTEGER",
    "csv": "INTEGER",
    "dbf": "INTEGER"
})

# 4. (Opcional) Crear índice para consultas rápidas
conn.execute("CREATE INDEX IF NOT EXISTS idx_modulos_año_modulo ON modulos(año, modulo)")

# 5. Cerrar conexión
conn.close()

print("Listo: base de datos creada'")

# def connect(db_name: str):
#     RESOURCES_PATH = Path(__file__).parent.parent / "resources"
#     database = RESOURCES_PATH / f"{db_name}.sqlite"
#     conn = sqlite3.connect(database)
#     cursor = conn.cursor()
#     cursor.execute("ALTER TABLE modulos_enapres RENAME TO modulos")
#     conn.commit()
#     cursor.close()
#     return cursor, conn

# if __name__ == "__main__":
#     cursor, conn = connect("enaho")