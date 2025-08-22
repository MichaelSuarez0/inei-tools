import inei_tools as inei
from pathlib import Path
import shutil
import pytest


OUTPUT_DIR = Path(__file__).parent / "downloads"

@pytest.fixture(scope="class", autouse=True)
def clean_output_dir(request):
    """Fixture de clase: limpia y prepara OUTPUT_DIR una vez por clase."""
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True)

    def fin():
        shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
    request.addfinalizer(fin)


class TestEnahoDownloader:
    def assert_valid_paths(self, paths: list[Path]):
        assert len(paths) > 0, "No se descargó ningún archivo"
        for path in paths:
            assert path.exists(), f"Ruta no existe: {path}"

    def test_download_zip(self):
        downloader = inei.Downloader(
            modulos=inei.Enaho.M01_CARACTERISTICAS_VIVIENDA_HOGAR,
            anios=range(2020, 2024),
            output_dir=OUTPUT_DIR,
            descomprimir=False,
            overwrite=True,
            file_type="stata",
            data_only=False,
            parallel_downloads=False,
        )
        paths = downloader.download_all()
        self.assert_valid_paths(paths)

    def test_download_zip_decompress(self):
        downloader = inei.Downloader(
            modulos=inei.Enaho.M85_GOBERNABILIDAD_DEMOCRACIA_TRANSPARENCIA,
            anios=[2022, 2023],
            output_dir=OUTPUT_DIR,
            descomprimir=True,
            overwrite=True,
            file_type="csv",
            data_only=False,
            parallel_downloads=True,
        )
        paths = downloader.download_all()
        self.assert_valid_paths(paths)

    def test_download_data_only(self):
        downloader = inei.Downloader(
            modulos=inei.Enaho.M85_GOBERNABILIDAD_DEMOCRACIA_TRANSPARENCIA,
            anios=[2022, 2023],
            output_dir=OUTPUT_DIR,
            descomprimir=True,
            overwrite=True,
            file_type="csv",
            data_only=True,
            parallel_downloads=True,
        )
        paths = downloader.download_all()
        self.assert_valid_paths(paths)

    def test_download_no_overwrite(self):
        downloader1 = inei.Downloader(
            modulos=inei.Enaho.M85_GOBERNABILIDAD_DEMOCRACIA_TRANSPARENCIA,
            anios=[2022],
            output_dir=OUTPUT_DIR,
            descomprimir=True,
            overwrite=True,
            file_type="csv",
            data_only=True,
            parallel_downloads=False,
        )
        initial_paths = downloader1.download_all()

        downloader2 = inei.Downloader(
            modulos=inei.Enaho.M85_GOBERNABILIDAD_DEMOCRACIA_TRANSPARENCIA,
            anios=[2022],
            output_dir=OUTPUT_DIR,
            descomprimir=True,
            overwrite=False,
            file_type="csv",
            data_only=True,
            parallel_downloads=False,
        )
        second_paths = downloader2.download_all()

        assert initial_paths == second_paths, "Los paths deberían coincidir si no se sobrescribe"
        self.assert_valid_paths(second_paths)


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
    
    #pytest.main([__file__, "-v", "-k", "test_download_zip"])

# def test_enapres_downloader():
#     ed = inei.Downloader(
#         anios=list(range(2023, 2025)),
#         modulos=[inei.Enapres.M500_SEGURIDAD_CIUDADANA],
#         output_dir=OUTPUT_DIR,
#         descomprimir=True,
#         file_type="dbf"
#     )
#     paths = ed.download_all()
#     pprint(paths)


# def test_remove_indented_folders():
#     ed = inei.Downloader(
#         anios=list(range(2023, 2025)),
#         modulos=[inei.Enapres.M500_SEGURIDAD_CIUDADANA],
#         output_dir=OUTPUT_DIR,
#         descomprimir=True,
#         file_type="dbf",
#         parallel_downloads=True,
#         data_only=True
#     )
#     paths = ed.download_all()
#     pprint(paths)

# # NOTE: No existe CSV para antes del 2020
# def test_endes_downloader():
#     ed = inei.Downloader(
#         anios=list(range(2018, 2022)),
#         modulos=[inei.Endes.M1638_PESO_TALLA_ANEMIA, inei.Endes.M1634_INMUNIZACION_SALUD],
#         output_dir=OUTPUT_DIR,
#         descomprimir=True,
#         overwrite=True,
#         parallel_downloads=True
#     )
#     paths = ed.download_all()
#     pprint(paths)


# def test_modulo():
#     ed = inei.Downloader(
#         modulos=inei.Enaho.M85_GOBERNABILIDAD_DEMOCRACIA_TRANSPARENCIA,
#         anios=list(range(2022, 2024)),
#         output_dir=OUTPUT_DIR,
#         descomprimir=False,
#         overwrite=True,
#         file_type="csv",
#         data_only=False,
#         parallel_downloads=True,
#     )
#     print(ed._obtain_encuesta())

# if __name__ == "__main__":
#     tests = EnahoDownloaderTests()
#     tests.test_download_no_overwrite()

