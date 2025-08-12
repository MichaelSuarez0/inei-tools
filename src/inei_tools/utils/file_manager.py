
from pathlib import Path


class FileManager:
    def __init__(self, search_directory: str | Path):
        """
        Initializes File Manager for managing files in the specified directory.

        Args:
            search_directory (str): Directory to search for files. Should be a valid path string.
        """
        self.search_directory = Path(search_directory)

    # def _convert_to_path(self):
    #     if isinstance(self.search_directory, str):
    #         self.search_directory = Path(self.search_directory)
    #     if not isinstance(self.search_directory, (str, Path)):
    #         raise TypeError("Solo se permiten str o Path")

    def keep(self, file_names: list[str], with_ext: bool = True):
        if not isinstance(file_names, list):
            file_names = [file_names]

        files_to_keep = []
        for file in self.search_directory.iterdir():
            if with_ext:
                if file.name in file_names:
                    files_to_keep.append(file)
            else:
                if file.stem in file_names:
                    files_to_keep.append(file)

        return files_to_keep
