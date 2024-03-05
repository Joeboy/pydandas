import datetime
from typing import Annotated, Any

from pydantic import AfterValidator
from pydantic_core import PydanticCustomError


def datetime_interpreter(input_value: Any) -> datetime.datetime:
    date_formats = [
        "%Y-%m-%d",
        # ... etc ...
    ]
    if isinstance(input_value, datetime.datetime):
        # Probably, we got a datetime value from Excel. We can just return it as is
        return input_value
    elif isinstance(input_value, str):
        for date_format in date_formats:
            try:
                return datetime.datetime.strptime(input_value, date_format)
            except ValueError:
                continue
        raise PydanticCustomError("datetime_parsing", "Could not parse as datetime")
    else:
        raise PydanticCustomError(
            "value_error",
            f"Can't interpret this kind of data ('{type(input_value).__name__}')",
        )


CustomDatetime = Annotated[Any, AfterValidator(datetime_interpreter)]
