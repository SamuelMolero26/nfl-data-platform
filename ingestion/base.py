import logging
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


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
        logger.info("Wrote %s rows → %s", f"{len(df):,}", output_path)

    def run(self, output_path: Path) -> pd.DataFrame:
        logger.info(
            "[%s] Extracting from %s", self.__class__.__name__, self.source_path.name
        )
        raw = self.extract()
        logger.info(
            "[%s] Transforming %s rows", self.__class__.__name__, f"{len(raw):,}"
        )
        transformed = self.transform(raw)
        self.load(transformed, output_path)
        return transformed
