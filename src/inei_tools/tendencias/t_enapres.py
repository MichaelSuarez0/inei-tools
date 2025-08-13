from pathlib import Path
from typing import Optional
from ._trends_abc import TendenciasABC
from ..cleaners import EnapresCleaner
import logging

class TendenciasEnapres(TendenciasABC):
    def __init__(
        self,
        data_source=None,
        target_variable_id="",
        question_type="dummy",
        output_dir: Optional[str | Path] = None,
    ):
        super().__init__(data_source, target_variable_id, question_type, output_dir)
    
    def get_national_trends(self, output_path: Optional[Path] = None):
        self._obtain_data_if_needed()
        # self._remove_percepcion_hogar()

        for filename, df in self.filename_df_dict.items():
            cleaner = EnapresCleaner(df, self.variable_id)
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
