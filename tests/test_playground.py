import inei_tools as inei
from pathlib import Path
import inei_tools as inei


OUTPUT_DIR = Path(__file__).parent / "downloads"


def assert_valid_paths(paths: list[Path]):
    assert len(paths) > 0, "No se descargó ningún archivo"
    for path in paths:
        assert path.exists(), f"Ruta no existe: {path}"
        print(path)

def test_download_zip():
    downloader = inei.Downloader(
        modulos=[inei.Enaho.M01_CARACTERISTICAS_VIVIENDA_HOGAR],
        anios=list(range(2020, 2024)),
        output_dir=".",
        descomprimir=False,
        overwrite=True,
        file_type="stata",
        data_only=False,
        parallel_downloads=False,
    )
    paths = downloader.download_all()
    assert_valid_paths(paths)

def test_download_zip_decompress():
    downloader = inei.Downloader(
        modulos=inei.Enaho.M85_GOBERNABILIDAD_DEMOCRACIA_TRANSPARENCIA,
        anios=[2022, 2023],
        output_dir=OUTPUT_DIR,
        descomprimir=True,
        overwrite=True,
        file_type="dbf",
        data_only=False,
        parallel_downloads=True,
    )
    paths = downloader.download_all()
    assert_valid_paths(paths)

def test_no_available_format():
    downloader = inei.Downloader(
        anios=list(range(2017, 2020)),
        modulos=[inei.Endes.M1638_PESO_TALLA_ANEMIA],
        output_dir=OUTPUT_DIR,
        descomprimir=True,
        overwrite=True,
        parallel_downloads=True,
        file_type="csv"
    )
    paths = downloader.download_all()

if __name__ == "__main__":
    test_no_available_format()