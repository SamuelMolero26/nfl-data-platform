from abc import ABC, abstractmethod
from pathlib import Path
import pandas as pd


class SourceLoader(ABC):
    def __init__(self, source_path: Path):
        self.source_path = source_path

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Read raw source into a DataFrame."""

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean, cast, and normalize the DataFrame."""

    def load(self, df: pd.DataFrame, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        print(f"  Wrote {len(df):,} rows → {output_path}")

    def run(self, output_path: Path) -> pd.DataFrame:
        print(f"[{self.__class__.__name__}] Extracting from {self.source_path.name}...")
        raw = self.extract()
        print(f"[{self.__class__.__name__}] Transforming {len(raw):,} rows...")
        transformed = self.transform(raw)
        self.load(transformed, output_path)
        return transformed
