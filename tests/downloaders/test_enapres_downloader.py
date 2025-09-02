import inei_tools as inei
from pathlib import Path
import shutil
import inei_tools as inei
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


class TestEnapresDownloader:
    def assert_valid_paths(self, paths: list[Path]):
        assert len(paths) > 0, "No se descargó ningún archivo"
        for path in paths:
            assert path.exists(), f"Ruta no existe: {path}"

    def test_download_sin_años(self):
        """
        Enapres tiene esta particularidad de que sus 'códigos módulos' son únicos para cada año,
        por lo que se puede descargar una encuesta sin especificar el año.
        """
        downloader = inei.Downloader(
            modulos=1815,
            anios=None,
            output_dir=OUTPUT_DIR,
            descomprimir=True,
            overwrite=True,
            file_type="spss",
            data_only=True,
            parallel_downloads=False,
        )
        paths = downloader.download_all()
        self.assert_valid_paths(paths)


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
    