from pathlib import Path
import pandas as pd
import sqlite3

def connect(db_name: str):
    RESOURCES_PATH = Path(__file__).parent.parent / "resources"
    database = RESOURCES_PATH / f"{db_name}.sqlite"
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    return cursor, conn

if __name__ == "__main__":
    cursor, conn = connect("encuestas")

    #cursor.execute("UPDATE modulos SET modulo = '100D' WHERE encuesta = 'enapres' AND codigo_modulo = '1504'")
    cursor.execute("UPDATE modulos SET modulo = '200A' WHERE encuesta = 'enapres' AND codigo_modulo = '1246'")
    conn.commit()

    conn.close()

# # 1. Cargar el CSV
# df = pd.read_csv(r"C:\Users\micha\OneDrive\Python\inei-tools\src\inei_tools\utils\enaho_modulos_2004_2024.csv", dtype={"codigo_modulo": str, "modulo": str})

# 2. Crear o abrir base de datos SQLite

# # 3. Escribir a una tabla llamada modulos
# df.to_sql("modulos", conn, if_exists="replace", index=False, dtype={
#     "año": "INTEGER",
#     "periodo": "INTEGER",
#     "codigo_encuesta": "TEXT",
#     "encuesta": "TEXT",
#     "codigo_modulo": "TEXT",   # Aquí el fix
#     "modulo": "TEXT",          # Aquí el fix
#     "spss": "INTEGER",
#     "stata": "INTEGER",
#     "csv": "INTEGER",
#     "dbf": "INTEGER"
# })

# # 4. (Opcional) Crear índice para consultas rápidas
# conn.execute("CREATE INDEX IF NOT EXISTS idx_modulos_año_modulo ON modulos(año, modulo)")

# # 5. Cerrar conexión
# conn.close()

# print("Listo: base de datos creada'")


# def merge_databases_pandas(db_paths: list[str], output_db: str):
#     """
#     Mergea usando pandas - más confiable
#     """
#     all_dataframes = []
    
#     for db_path in db_paths:
#         print(f"Leyendo: {db_path}")
        
#         # Verificar que el archivo existe
#         if not Path(db_path).exists():
#             print(f"⚠️  Archivo no encontrado: {db_path}")
#             continue
            
#         try:
#             conn = sqlite3.connect(db_path)
#             df = pd.read_sql("SELECT * FROM modulos", conn)
#             all_dataframes.append(df)
#             conn.close()
#             print(f"✓ Leídos {len(df)} registros de {db_path}")
            
#         except Exception as e:
#             print(f"❌ Error leyendo {db_path}: {e}")
#             continue
    
#     if not all_dataframes:
#         print("❌ No se pudieron leer datos de ninguna base de datos")
#         return
    
#     # Concatenar todos los DataFrames
#     merged_df = pd.concat(all_dataframes, ignore_index=True)
#     print(f"Total de registros a insertar: {len(merged_df)}")
    
#     # Guardar en la nueva base de datos
#     conn = sqlite3.connect(output_db)
#     merged_df.to_sql("modulos", conn, if_exists="replace", index=False, dtype={
#         "año": "INTEGER",
#         "periodo": "INTEGER", 
#         "codigo_encuesta": "TEXT",
#         "encuesta": "TEXT",
#         "codigo_modulo": "TEXT",
#         "modulo": "TEXT",
#         "spss": "INTEGER",
#         "stata": "INTEGER",
#         "csv": "INTEGER",
#         "dbf": "INTEGER"
#     })
#     conn.close()
    
#     print(f"✅ Merge completado: {len(merged_df)} registros en {output_db}")

# # Uso
# RESOURCES_PATH = Path(__file__).parent.parent / "resources"
# db_files = [RESOURCES_PATH / "enaho.sqlite", RESOURCES_PATH / "enaho_panel.sqlite", RESOURCES_PATH / "enapres.sqlite", RESOURCES_PATH / "endes.sqlite"]
# merge_databases_pandas(db_files, RESOURCES_PATH / "encuestas.sqlite")