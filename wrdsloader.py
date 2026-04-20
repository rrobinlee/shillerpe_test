from __future__ import annotations
import logging
from pathlib import Path
from typing import Optional
import pandas as pd
logger = logging.getLogger(__name__)

def _save_to_parquet(df: pd.DataFrame, base_dir: Path, library: str, table: str, 
                     year: Optional[int]) -> Path:
    dest_dir = base_dir / library / table
    dest_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{table}_{year}.parquet" if year else f"{table}.parquet"
    dest = dest_dir / file_name
    df.to_parquet(dest, index=False)
    logger.info("Saved %d rows - %s", len(df), dest)
    return dest

def _build_query(library: str, table: str, columns: str, date_col: Optional[str], 
                 year: Optional[int], extra_query: str,) -> str:
    query = f"SELECT {columns} FROM {library}.{table} WHERE 1=1"
    if date_col and year:
        query += f" AND {date_col} BETWEEN '{year}-01-01' AND '{year}-12-31'"
    if extra_query:
        query += f" {extra_query.strip()}"
    return query

class WRDSloader:
    def __init__(self, wrds_username: Optional[str] = None, output_dir: str | Path = "./data/wrds",
                 autoconnect: bool = True) -> None:
        self.output_dir = Path(output_dir)
        self._db: Optional[object] = None
        if autoconnect:
            self.connect(wrds_username)

    def connect(self, wrds_username: Optional[str] = None) -> None:
        try:
            import wrds 
        except ImportError as exc:
            raise ImportError("The wrds package is required") from exc
        kwargs = {}
        if wrds_username:
            kwargs["wrds_username"] = wrds_username
        self._db = wrds.Connection(**kwargs)
        logger.info("Connected to WRDS")
    
    def disconnect(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None
            logger.info("WRDS connection closed")

    def __enter__(self) -> "WRDSDownloader":
        return self
        
    def __exit__(self, *_) -> None:
        self.disconnect()

    def download(self, library: str, table: str, date_col: Optional[str] = None, start_year: int = 1990,
                 end_year: int = 2025, extra_query: str = "", signal_cols: Optional[str] = None,
                 chunksize: Optional[int] = None) -> list[Path]:
        if self._db is None:
            raise RuntimeError("Not connected to WRDS - call .connect() first")

        columns = signal_cols if signal_cols else "*"
        years: list[Optional[int]] = list(range(start_year, end_year + 1)) if date_col else [None]
        written: list[Path] = []

        for year in years:
            query = _build_query(library, table, columns, date_col, year, extra_query)
            logger.debug("Query: %s", query)
            df = self._fetch(query, chunksize)
            if df.empty:
                label = f" year={year}" if year else ""
                logger.warning("[SKIP] %s.%s%s — empty result", library, table, label)
                print(f"[SKIP] {library}.{table}{' year=' + str(year) if year else ''} — empty result")
                continue
            path = _save_to_parquet(df, self.output_dir, library, table, year)
            written.append(path)
        return written

    def list_libraries(self) -> list[str]:
        self._require_connection()
        return self._db.list_libraries()
        
    def list_tables(self, library: str) -> list[str]:
        self._require_connection()
        return self._db.list_tables(library=library)
        
    def describe_table(self, library: str, table: str) -> pd.DataFrame:
        self._require_connection()
        return self._db.describe_table(library=library, table=table)
        
    def raw_sql(self, query: str, **kwargs) -> pd.DataFrame:
        self._require_connection()
        return self._db.raw_sql(query, **kwargs)
        
    def _require_connection(self) -> None:
        if self._db is None:
            raise RuntimeError("Not connected to WRDS - call .connect() first")
            
    def _fetch(self, query: str, chunksize: Optional[int]) -> pd.DataFrame:
        if chunksize:
            chunks = list(self._db.raw_sql(query, chunksize=chunksize))
            return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        return self._db.raw_sql(query)
