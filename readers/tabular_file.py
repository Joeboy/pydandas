import csv
from pathlib import Path
from typing import BinaryIO, Iterable, TextIO, Type

import openpyxl
import pandas as pd
import pydantic
from pydantic import BaseModel


class TabularDataFileReader:
    """Reader for a straightforward CSV file with header row"""

    STOP_AFTER_ERRORS_COUNT = 0
    columns: list[str] | None
    bailed_due_to_excessive_errors: bool
    _validation_errors: list[str]

    def __init__(
        self, source: str | Path | TextIO | BinaryIO, model: Type[BaseModel]
    ) -> None:
        self._source = source
        self._model = model
        self._validation_errors = []
        self.columns = None
        self.bailed_due_to_excessive_errors = False

    def process_rows(self) -> Iterable[dict]:
        for row in self.iter_rows():
            row_number = row["row_number"]
            try:
                entry = self._model(**row)
            except pydantic.ValidationError as e:
                for error in e.errors():
                    col_name = error["loc"][0]
                    input_value = error["input"]
                    self._validation_errors.append(
                        f"Error at line {row_number}, column '{col_name}', input '{input_value}': {error['msg']}"
                    )
                    if len(self._validation_errors) > self.STOP_AFTER_ERRORS_COUNT:
                        self.bailed_due_to_excessive_errors = True
                        break
            else:
                yield entry.model_dump()

    def get_dataframe_and_errors(self) -> tuple[Iterable, list[str]]:
        df = pd.DataFrame(self.process_rows())
        return df, self._validation_errors

    def iter_rows(self) -> Iterable[dict]:
        raise NotImplementedError


class SimpleCsvReader(TabularDataFileReader):
    _source: str | Path | BinaryIO

    def iter_rows(self):
        for i, row in enumerate(csv.DictReader(self._source)):
            row["row_number"] = 1 + i
            row["sheet_name"] = None
            yield row


class SimpleExcelReader(TabularDataFileReader):
    _source: str | Path | BinaryIO

    def iter_rows(self):
        wb = openpyxl.load_workbook(self._source)
        for sheet in wb:
            row_iterator = sheet.iter_rows()
            self.columns = [str(r.value) for r in next(row_iterator)]
            self.columns.pop(0)  # Remove null header for row number
            for row in row_iterator:
                row = [c.value for c in row]
                row_number = 1 + int(row.pop(0))  # First entry is row number
                d = dict(zip(self.columns, row))
                d["sheet_name"] = sheet.title
                d["row_number"] = row_number
                yield d
