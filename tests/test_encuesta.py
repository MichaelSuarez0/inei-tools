import time
from inei_tools import Downloader, Enaho
from pathlib import Path
from pprint import pprint

def medir_tiempo(func):
    def wrapper(*args, **kwargs):
        inicio = time.perf_counter()  # mide con precisión alta
        resultado = func(*args, **kwargs)
        fin = time.perf_counter()
        print(f"La función '{func.__name__}' tardó {fin - inicio:.6f} segundos")
        return resultado
    return wrapper

@medir_tiempo
def test_encuesta():
    ed = Downloader(
        modulos=Enaho.M85_GOBERNABILIDAD_DEMOCRACIA_TRANSPARENCIA,
        anios=list(range(2022, 2024)),
    )
    encuesta = ed._obtain_encuesta()
    assert type(encuesta.YEAR_MAP.value) == dict

@medir_tiempo
def test_encuesta_str():
    ed = Downloader(
        modulos=Enaho.M85_GOBERNABILIDAD_DEMOCRACIA_TRANSPARENCIA.value,
        anios=list(range(2022, 2024)),
    )
    encuesta = ed._obtain_encuesta()
    assert type(encuesta.YEAR_MAP.value) == dict


if __name__ == "__main__":
    #print(Enaho.M01_CARACTERISTICAS_VIVIENDA_HOGAR.__class__.__name__)
    # test_inei_downloader()
    test_encuesta()
    test_encuesta_str()
    #print(Enaho.YEAR_MAP.value)