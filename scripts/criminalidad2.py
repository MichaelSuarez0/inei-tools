from pathlib import Path
import inei_tools as inei

def enapres_download():
    downloader = inei.Downloader(
        anios=list(range(2022, 2024)),
        modulos=[inei.Enapres.M500_SEGURIDAD_CIUDADANA],
        output_dir=Path(__file__).parent.parent / "encuestas_csv",
        descomprimir=True,
        data_only=True,
        parallel_downloads=True,
        overwrite=False
    )
    paths = downloader.download_all()
    return paths


# NOTE: Separador es ;
# NOTE: NO hya colomuna UBIGEO, pero hay NOMBREDD;CCPP;NOMBREPP;CCDI;NOMBREDI
# def enapres_trends():
#     trend = inei.Tendencias(
#         r"C:\Users\micha\OneDrive\Python\inei-tools\encuestas_csv\736-Modulo1622\736-Modulo1622\CAP_600_URBANO_7.csv",
#         target_variable_id="P611D_5",
#         question_type="confidence",
#         output_dir=Path(__file__).parent
#     )

#     trend.obtain_trends()

# TODO: usecols = ["P611B", "FACTOR", "NOMBREDD"]  # agrega las mínimas necesarias
# TODO: Renombrar la pendejada de nombres del inei del csv que hacen que se sobreescriba el diccionario
if __name__ == "__main__":
# print(Enaho.M01_CARACTERISTICAS_VIVIENDA_HOGAR.__class__.__name__)
    paths = enapres_download()
    print(paths)
    #enapres_trends()

    tendencias = inei.Tendencias(paths, target_variable_id="P611B", question_type="confidence", encuesta="enapres")
    df = tendencias.get_national_trends()

    print(df)
    # print(df["P611B"].unique())
