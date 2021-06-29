import config as C
import datetime


class IncorrectArgumentException(Exception):
    pass


def check_value(value, possible_values, exception_text):
    if value not in possible_values:
        raise IncorrectArgumentException(exception_text)
    return value


def get_report_name(arg):
    return check_value(arg, C.ARG_REPORT_VALUES, C.ARG_REPORT_NAME_EXCEPTION)


def get_format(arg):
    return check_value(arg, C.ARG_FORMAT_VALUES, C.ARG_FORMAT_VALUE_EXCEPTION)


def get_date(arg):
    try:
        datetime.datetime.strptime(arg, '%Y-%m-%d')
        return arg
    except ValueError:
        raise IncorrectArgumentException(C.ARG_DATE_EXCEPTION)
