import csv
import pathlib
from io import BytesIO, StringIO
from typing import Callable, Iterable, Optional, Type

import pandas as pd
import pytest
from polyfactory.factories.pydantic_factory import ModelFactory
from pydantic import BaseModel


@pytest.fixture
def create_excel_file() -> Callable:
    def _create_excel_file(row_data: Iterable):
        df = pd.DataFrame(row_data)
        excel_file = BytesIO()
        df.to_excel(excel_file)
        excel_file.seek(0)
        return excel_file

    return _create_excel_file


@pytest.fixture
def create_csv_file() -> Callable:
    def _create_file(row_data: Iterable):
        df = pd.DataFrame(row_data)
        csv_file = StringIO()
        df.to_csv(csv_file)
        csv_file.seek(0)
        return csv_file

    yield _create_file


@pytest.fixture
def simple_row_model():
    class RowModel(BaseModel):
        col1: int
        col2: str
        col3: Optional[str]

    yield RowModel


@pytest.fixture
def make_test_file(tmp_path):
    file_path = tmp_path / "testfile.csv"

    def _make_test_file(model: Type[BaseModel], nrows) -> pathlib.Path:
        class Factory(ModelFactory[model]):
            # This will only work with quite simple models
            pass

        with file_path.open("w") as f:
            writer = csv.DictWriter(f, model.model_fields.keys())
            writer.writeheader()
            writer.writerows((Factory.build().model_dump() for _ in range(nrows)))
        return file_path

    yield _make_test_file
