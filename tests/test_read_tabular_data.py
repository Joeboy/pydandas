import datetime
import timeit

from pydantic import BaseModel

from readers.pydantic_types import CustomDatetime
from readers.tabular_file import SimpleCsvReader, SimpleExcelReader


class TimestampedRowModel(BaseModel):
    col1: int
    col2: str
    timestamp: CustomDatetime
    sheet_name: str | None
    row_number: int


def test_read_excel(create_excel_file):
    excel_file = create_excel_file(
        [
            {
                "col1": 1,
                "col2": "col2-1",
                "timestamp": datetime.datetime.now() - datetime.timedelta(days=1),
            },
            {
                "col1": "2",
                "col2": "col2-2",
                "timestamp": datetime.datetime.now(),
            },
            {
                "col1": 3,
                "col2": "col2-3",
                "timestamp": "Novtember the 32nd",
            },
            {
                "col1": 4,
                "col2": "col2-4",
                "timestamp": "2024-02-15",
            },
        ]
    )

    reader = SimpleExcelReader(excel_file, TimestampedRowModel)
    df, error_list = reader.get_dataframe_and_errors()
    row1 = df.iloc[0].to_dict()
    timestamp = row1.pop("timestamp")
    assert isinstance(timestamp, datetime.datetime)
    assert row1 == {
        "col1": 1,
        "col2": "col2-1",
        "sheet_name": "Sheet1",
        "row_number": 1,
    }
    row2 = df.iloc[1].to_dict()
    timestamp = row2.pop("timestamp")
    assert isinstance(timestamp, datetime.datetime)
    assert row2 == {
        "col1": 2,
        "col2": "col2-2",
        "sheet_name": "Sheet1",
        "row_number": 2,
    }
    row3 = df.iloc[2].to_dict()
    timestamp = row3.pop("timestamp")
    assert isinstance(timestamp, datetime.datetime)
    assert row3 == {
        "col1": 4,
        "col2": "col2-4",
        "sheet_name": "Sheet1",
        "row_number": 4,
    }

    assert error_list == [
        "Error at line 3, column 'timestamp', input 'Novtember the 32nd': Could not parse as datetime"
    ]


def test_read_csv(create_csv_file):
    csv_file = create_csv_file(
        [
            {
                "col1": 1,
                "col2": "col2-1",
                "timestamp": (
                    datetime.datetime.now() - datetime.timedelta(days=1)
                ).strftime("%Y-%m-%d"),
            },
            {
                "col1": "2",
                "col2": "col2-2",
                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d"),
            },
            {
                "col1": 3,
                "col2": "col2-3",
                "timestamp": "Novtember the 32nd",
            },
            {
                "col1": 4,
                "col2": "col2-4",
                "timestamp": "2024-02-15",
            },
        ]
    )

    reader = SimpleCsvReader(csv_file, TimestampedRowModel)
    df, error_list = reader.get_dataframe_and_errors()
    row1 = df.iloc[0].to_dict()
    timestamp = row1.pop("timestamp")
    assert isinstance(timestamp, datetime.datetime)
    assert row1 == {
        "col1": 1,
        "col2": "col2-1",
        "sheet_name": None,
        "row_number": 1,
    }
    row2 = df.iloc[1].to_dict()
    timestamp = row2.pop("timestamp")
    assert isinstance(timestamp, datetime.datetime)
    assert row2 == {
        "col1": 2,
        "col2": "col2-2",
        "sheet_name": None,
        "row_number": 2,
    }
    row3 = df.iloc[2].to_dict()
    timestamp = row3.pop("timestamp")
    assert isinstance(timestamp, datetime.datetime)
    assert row3 == {
        "col1": 4,
        "col2": "col2-4",
        "sheet_name": None,
        "row_number": 4,
    }

    assert error_list == [
        "Error at line 3, column 'timestamp', input 'Novtember the 32nd': Could not parse as datetime"
    ]


def test_with_real_file(make_test_file, simple_row_model):
    # For reference, with nrows=100k, on my laptop, it takes ~60s to write the
    # test file, and 0.7s to read it.
    nrows = 1_000

    t1 = timeit.default_timer()
    test_file = make_test_file(simple_row_model, nrows)
    dt = timeit.default_timer() - t1
    print(f"Wrote test file with {nrows} rows in {dt}s")

    t1 = timeit.default_timer()
    with open(test_file) as f:
        reader = SimpleCsvReader(f, simple_row_model)
        df, validation_errors = reader.get_dataframe_and_errors()

    dt = timeit.default_timer() - t1
    print(f"Read test file with {nrows} rows in {dt}s")

    assert df.shape[0] == nrows
    first_entry = df.iloc[0].to_dict()
    assert isinstance(first_entry["col1"], int)
    assert isinstance(first_entry["col2"], str)
    assert isinstance(first_entry["col3"], (str, type(None)))
