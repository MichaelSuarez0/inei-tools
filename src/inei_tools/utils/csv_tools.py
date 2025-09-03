
import csv
from pathlib import Path


@staticmethod
def detect_delimiter(
    path: Path,
    *,
    encoding="latin1",
    candidates=(",", ";", "|", "\t"),
    sample_bytes=64 * 1024,
) -> str:
    # Read a small text sample
    with open(path, "r", encoding=encoding, newline="") as f:
        sample = f.read(sample_bytes)

    # Excel hint: first line like "sep=;"
    lines = sample.splitlines()
    if lines:
        first = lines[0].strip().lower()
        if first.startswith("sep=") and len(first) >= 5:
            return first[4]

    # Try Sniffer with restricted candidates
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters="".join(candidates))
        return dialect.delimiter
    except csv.Error:
        # Fallback: pick the candidate that appears most across first N lines
        probe = [
            ln
            for ln in lines[:20]
            if ln and not ln.startswith("#") and not ln.lower().startswith("sep=")
        ]
        if not probe:
            return ","  # final default
        scores = {c: sum(ln.count(c) for ln in probe) for c in candidates}
        return max(scores, key=scores.get)

@staticmethod
def detect_encoding(
    path: Path,
    *,
    sample_bytes: int = 256 * 1024,  # 256 KB para decidir bien
    candidates: tuple[str, ...] = ("utf-8", "utf-8-sig", "cp1252", "latin1"),
) -> str:
    """
    Devuelve un nombre de encoding estimado para el archivo.
    Estrategia:
    1) Detectar BOM (UTF-8/UTF-16/UTF-32).
    2) Probar candidatos comunes (utf-8, utf-8-sig, cp1252, latin1) con errors='strict'.
    3) Si hay charset-normalizer o chardet, usarlos como fallback.
    4) Último recurso: 'latin1'.
    """
    # --- 1) Leer muestra binaria
    with open(path, "rb") as fb:
        raw = fb.read(sample_bytes)

    # --- 2) BOMs frecuentes
    boms = {
        b"\xef\xbb\xbf": "utf-8-sig",
        b"\xff\xfe": "utf-16-le",
        b"\xfe\xff": "utf-16-be",
        b"\xff\xfe\x00\x00": "utf-32-le",
        b"\x00\x00\xfe\xff": "utf-32-be",
    }
    for bom, enc in boms.items():
        if raw.startswith(bom):
            return enc

    # --- 3) Probar candidatos "duros" (sin errores)
    for enc in candidates:
        try:
            raw.decode(enc, errors="strict")
            return enc
        except UnicodeDecodeError:
            continue

    # --- 4) Fallback “inteligente” con charset-normalizer o chardet (si existen)
    try:
        from charset_normalizer import from_bytes  # type: ignore
        res = from_bytes(raw)
        best = res.best()
        if best and best.encoding:
            return best.encoding
    except Exception:
        pass

    try:
        import chardet  # type: ignore
        guess = chardet.detect(raw)
        if guess and guess.get("encoding"):
            return guess["encoding"]
    except Exception:
        pass

    # --- 5) Último recurso: latin1 nunca falla al decodificar bytes 0x00-0xFF
    return "latin1"