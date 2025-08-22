import inei_tools as inei
from pathlib import Path
from icecream import ic
from pprint import pprint

def test_init_years_modulos():
    et = inei.Tendencias(
        years = list(range(2022,2025)), 
        modulos= inei.Enaho.M85_GOBERNABILIDAD_DEMOCRACIA_TRANSPARENCIA,
        target_variable_id="p2_1$01",
        output_dir=Path(__file__).parent,
        question_type="dummy"
    )
    pprint(et.filename_df_dict)
    #df = et.obtain_trends()

def test_init_data_source():
    et = inei.Tendencias(
        data_source=[],
        target_variable_id="p2_1$01",
        output_dir=Path(__file__).parent,
        question_type="dummy"
    )
    #df = et.obtain_trends()


if __name__ == "__main__":
    test = Path(__file__).parent
    paths = [path for path in list(test.iterdir()) if path.is_file() and path.suffix == ".csv"][:3]
    et = inei.Tendencias(paths, target_variable_id = "P1171$09", question_type="dummy")
    et.get_national_trends()
    #test_init_years_modulos()
    # performance: pyinstrument -o profile.html pandas_playground/dummies.py
