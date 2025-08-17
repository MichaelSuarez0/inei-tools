
from pathlib import Path
from typing import Iterable, Optional


class FileManager:
    # def _convert_to_path(self):
    #     if isinstance(self.search_directory, str):
    #         self.search_directory = Path(self.search_directory)
    #     if not isinstance(self.search_directory, (str, Path)):
    #         raise TypeError("Solo se permiten str o Path")

     def keep(
        self,
        search_directory: str | Path,
        file_names: Iterable[str],
        *,
        max_depth: Optional[int] = 0,   # 0 = solo raíz, None = sin límite
        dry_run: bool = False,
    ) -> list[Path]:
        """
        Mantiene solo los archivos cuyo nombre esté en `file_names` y elimina el resto.
        Recorre subdirectorios hasta `max_depth`.

        Parameters
        ----------
        search_directory : str | Path
            Carpeta raíz donde filtrar.
        file_names : Iterable[str]
            Nombres de archivo a conservar (coincidencia exacta por nombre, con extensión).
        max_depth : int | None, default 0
            0 = solo raíz; 1 = baja un nivel; ...; None = sin límite.
        dry_run : bool, default False
            Si True, no borra nada; solo devuelve qué se conservaría.
        """
        root = Path(search_directory)
        if not root.exists() or not root.is_dir():
            raise NotADirectoryError(f"No existe o no es directorio: {root}")

        keep_set = set(file_names)
        kept: list[Path] = []

        # Recorrido con límite de profundidad
        stack: list[tuple[Path, int]] = [(root, 0)]
        while stack:
            current, depth = stack.pop()

            for entry in current.iterdir():
                if entry.is_dir():
                    can_descend = (max_depth is None) or (depth < max_depth)
                    if can_descend:
                        stack.append((entry, depth + 1))
                    continue

                # Es archivo
                if entry.name in keep_set:
                    kept.append(entry)
                else:
                    if not dry_run:
                        entry.unlink(missing_ok=True)

        return kept
