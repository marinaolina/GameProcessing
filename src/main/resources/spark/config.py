"""
Configuration file for the proccesing.py
"""
from collections import OrderedDict

ARG_REPORT_VALUES = ["input"]
ARG_REPORT_NAME_EXCEPTION = "Input should equal to {}".format(" or ".join(ARG_REPORT_VALUES))
ARG_FORMAT_VALUES = ["json"]
ARG_FORMAT_VALUE_EXCEPTION = "Format should equal to {}".format(" or ".join(ARG_FORMAT_VALUES))
ARG_DATE_EXCEPTION = "Report date should be specified in YYYY-MM-DD format"

DATA_URL = "https://snap.datastream.center/techquest"
FILE_CALL = lambda r,d,f: "{}/{}-{}.{}.gz".format(DATA_URL, r, d, f)
TABLE_PREPARED_TARGET = "report_input"
TABLE_PREPARED_ERROR = "data_error"


FIELDS_PREPARED_TARGET = OrderedDict([
    ("user", "int"), ("ts", "timestamp"), ("context", "struct"), ("ip", "string")
])


API_REPORT = "api_report"
API_DATE = "api_date"
RECORD_CONCATENATED = "row_text"
ERROR_TEXT = "error_text"
INS_TS = "ins_ts"
FIELDS_PREPARED_ERROR = OrderedDict([
    (API_REPORT, "string"),
    (API_DATE, "date"),
    (RECORD_CONCATENATED, "string"),
    (ERROR_TEXT, "string"),
    (INS_TS, "timestamp")
])

# Cols list to check or convert in Date type or Check pattern
MALFORMED_COLUMN_NAMES = ["row_text"]
MALFORMED_COLUMN_ERROR = "Malformed detected on reading field."

# Cols list to convert in timestamp
TIMESTAMP_COLUMN_NAMES = ["ts"]
TIMESTAMP_COLUMN_ERROR = "STRING for column \"TS\" has wrong pattern."

# Cols list to convert in Integer
INTEGER_COLUMN_NAMES = ["user"]

