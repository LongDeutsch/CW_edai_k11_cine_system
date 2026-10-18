"""Hợp đồng timestamp: nhận timestamp_ntz, từ chối kiểu không phải thời gian."""

from __future__ import annotations

import os
import unittest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from transformation.batch.contracts.validators import USERS, ContractError, check_schema, normalize_timestamps

os.environ.setdefault("TZ", "UTC")


def _users(*fields: StructField) -> StructType:
    by_name = {field.name: field for field in fields}
    ordered = []
    for name in USERS.required:
        if name in by_name:
            ordered.append(by_name[name])
        elif name == "user_id":
            ordered.append(StructField(name, LongType(), True))
        else:
            ordered.append(StructField(name, StringType(), True))
    return StructType(ordered)


class TimestampContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("timestamp-contract")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_timestamp_ntz_becomes_utc_timestamp(self):
        schema = _users(StructField("signup_ts", TimestampNTZType(), True))
        frame = self.spark.createDataFrame(
            [(1, datetime(2018, 6, 1, 3, 4, 5), "VN", "Free", "vi", "18-24", "F")],
            schema=schema,
        )
        normalized = normalize_timestamps(frame, USERS)
        field = next(item for item in normalized.schema.fields if item.name == "signup_ts")
        self.assertIsInstance(field.dataType, TimestampType)
        self.assertEqual(normalized.collect()[0]["signup_ts"], datetime(2018, 6, 1, 3, 4, 5))

    def test_string_timestamp_is_rejected(self):
        schema = _users(StructField("signup_ts", StringType(), True))
        with self.assertRaises(ContractError):
            check_schema(schema, USERS)

    def test_integer_timestamp_is_rejected(self):
        schema = _users(StructField("signup_ts", LongType(), True))
        with self.assertRaises(ContractError):
            check_schema(schema, USERS)

    def test_date_is_not_a_timestamp(self):
        schema = _users(StructField("signup_ts", DateType(), True))
        with self.assertRaises(ContractError):
            check_schema(schema, USERS)


if __name__ == "__main__":
    unittest.main()
