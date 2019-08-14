import datetime
import typing

FORMAT_STRING = "%Y-%m-%dT%H%M%S.%fZ"


def get_datetime_now(as_string: bool = False) -> typing.Union[datetime.datetime, str]:
    now = datetime.datetime.utcnow()
    return to_string(now) if as_string else now


def to_string(date: datetime.datetime) -> str:
    return date.strftime(FORMAT_STRING)


def to_datetime(date_string: str) -> datetime.datetime:
    return datetime.datetime.strptime(date_string, FORMAT_STRING)
