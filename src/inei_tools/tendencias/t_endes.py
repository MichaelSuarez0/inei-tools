from pathlib import Path
from typing import Optional
from ._trends_abc import TendenciasABC
from ..cleaners import EndesCleaner
from ..downloaders import Downloader
import logging

class TendenciasEndes(TendenciasABC):
    def __init__(
        self,
        data_source=list[Path] | Downloader,
        target_variable_id="",
        question_type="dummy",
        output_dir: Optional[str | Path] = None,
    ):
        super().__init__(data_source, target_variable_id, question_type, output_dir)
    
    def _load_into_memory(self, path_list: list[Path]):
        import pandas as pd

        logging.info("📖 Reading file paths")

        self.filename_df_dict = {}
        for file_path in path_list:
            self.filename_df_dict[file_path.name] = EndesCleaner(file_path, target_variable_id=self.target_variable_id)
            logging.info(f"📖 Reading {file_path}")
            if file_path.suffix == ".dta":
                df = pd.read_stata(file_path)
                self.filename_df_dict[file_path.name] = df
            elif file_path.suffix == ".csv":
                df = pd.read_csv(
                    file_path, encoding="latin1", sep=";", low_memory=False
                )
                if (
                    len(df.columns) == 1
                ):  # INEI es bien inconsistente y algunos csv antiguos pueden estar con el delimitador ","
                    df = pd.read_csv(
                        file_path, encoding="latin1", sep=",", low_memory=False
                    )
                self.filename_df_dict[file_path.name] = df
            logging.info(f"📖 Finished reading {file_path}")

        return self.filename_df_dict
    
    def get_national_trends(self, output_path: Optional[Path] = None):
        self._obtain_data_if_needed()
        # self._remove_percepcion_hogar()

        for filename, df in self.filename_df_dict.items():
            cleaner = EndesCleaner(df, self.variable_id)
            # cleaner.remove_nas().add_departamentos().filter_by_variable()
            logging.info(f"Cleaning {filename}")
            cleaner.remove_nas().add_departamentos().filter_by_variable().count_categories()
            # return cleaner.df
           
            self.df_list_clean.append(cleaner.df)
        
        merged_df = self._merge_dfs(self.df_list_clean)

        #final_df = transpose(final_df)
        if output_path:
            self._export_to_excel(output_path, merged_df)
        return merged_df
