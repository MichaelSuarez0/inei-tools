from pathlib import Path
import inei_tools as inei
from pprint import pprint


def enapres_download():
    downloader = inei.Downloader(
        anios=list(range(2022, 2025)),
        modulos=[inei.Enapres.M500_SEGURIDAD_CIUDADANA],
        output_dir=Path(__file__).parent.parent / "encuestas_csv",
        descomprimir=True,
        data_only=True,
        parallel_downloads=True,
        overwrite=False
    )
    paths = downloader.download_all()
    pprint(paths)

# NOTE: Separador es ;
# NOTE: NO hya colomuna UBIGEO, pero hay NOMBREDD;CCPP;NOMBREPP;CCDI;NOMBREDI
def enapres_trends():
    trend = inei.Tendencias(
        r"C:\Users\micha\OneDrive\Python\inei-tools\encuestas_csv\736-Modulo1622\736-Modulo1622\CAP_600_URBANO_7.csv",
        target_variable_id="P611D_5",
        question_type="confidence",
        output_dir=Path(__file__).parent
    )

    trend.get_national_trends()


if __name__ == "__main__":
    # print(Enaho.M01_CARACTERISTICAS_VIVIENDA_HOGAR.__class__.__name__)
    enapres_download()
    #enapres_trends()
