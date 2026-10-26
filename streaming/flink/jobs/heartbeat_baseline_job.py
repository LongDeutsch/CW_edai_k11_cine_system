"""Baseline heartbeat: processing time, không watermark, không dedup, không DLQ.

Cùng topic ``movie.playback.heartbeat`` với job tối ưu
``heartbeat_window_job``. Cửa sổ 1 phút đóng theo đồng hồ xử lý, nên event
tới muộn vẫn vào cửa sổ đang mở và bản trùng ``heartbeat_id`` được đếm hai
lần. Không có side output, không có state TTL.

Chạy local::

    PYTHONPATH=. python -m streaming.flink.jobs.heartbeat_baseline_job --engine local --scenario proof

Submit lên cùng JobManager, sink tách khỏi bảng production::

    python -m streaming.flink.jobs.heartbeat_baseline_job --engine flink \\
        --sessions-table cinestream.silver.it_baseline_heartbeats \\
        --windows-table cinestream.silver.it_baseline_windows
"""

from __future__ import annotations

import argparse
import json

from streaming.flink.config import StreamJobConfig
from streaming.flink.jobs.heartbeat_window_job import _parse_heartbeat, build_parser, proof_events
from streaming.flink.sinks.iceberg_sink import (
    apply_s3a,
    catalog_ddl,
    heartbeat_insert_sql,
    heartbeat_table_ddl,
    windows_table_ddl,
)


def baseline_parser() -> argparse.ArgumentParser:
    parser = build_parser()
    parser.description = "Baseline processing-time. Không watermark, không dedup, không DLQ."
    parser.set_defaults(
        group_id="cinestream-heartbeat-baseline",
        sessions_table="cinestream.silver.it_baseline_heartbeats",
        windows_table="cinestream.silver.it_baseline_windows",
        job_name="heartbeat-baseline",
        dedup_ttl_ms=0,
        allowed_lateness_ms=0,
        out_of_orderness_ms=0,
    )
    return parser


def config_from_args(args: argparse.Namespace) -> StreamJobConfig:
    from streaming.flink.jobs.heartbeat_window_job import config_from_args as optimized_config

    config = optimized_config(args)
    known = set(getattr(StreamJobConfig, "__dataclass_fields__", {}))
    values = {name: getattr(config, name) for name in known}
    values.update(
        {
            "dlq_topic": "it.baseline.unused-dlq",
            "group_id": args.group_id,
            "out_of_orderness_ms": 0,
            "allowed_lateness_ms": 0,
            "silver_sessions_table": args.sessions_table,
            "silver_windows_table": args.windows_table,
            "dedup_ttl_ms": 0,
            "transactional_id_prefix": "cinestream-heartbeat-baseline",
        }
    )
    if "job_name" in known:
        values["job_name"] = getattr(args, "job_name", "") or "heartbeat-baseline"
    return StreamJobConfig(**{key: value for key, value in values.items() if key in known})


def run_local(payloads: list[dict]) -> dict:
    """Giữ mọi record. Không có dead letter và không khử heartbeat_id."""
    identifiers = [int(item["heartbeat_id"]) for item in payloads]
    report = {
        "job": "heartbeat-baseline",
        "time_characteristic": "processing_time",
        "watermark": False,
        "allowed_lateness_ms": None,
        "dedup": False,
        "dlq": False,
        "accepted": len(payloads),
        "duplicates_kept": len(identifiers) - len(set(identifiers)),
        "dead_letters": 0,
        "dead_letter_ids": [],
    }
    print(json.dumps(report, indent=2))
    return report


def run_flink(config: StreamJobConfig) -> None:
    """Tumbling processing-time 1 phút. Không assign watermark, không side output."""
    from streaming.flink.runtime_guard import require_streaming_approval

    require_streaming_approval(
        source_topic=config.source_topic,
        heartbeats_table=config.silver_sessions_table,
        windows_table=config.silver_windows_table,
    )
    from pyflink.common import Configuration, RestartStrategies, Time, Types
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
    from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
    from pyflink.datastream.window import TumblingProcessingTimeWindows
    from pyflink.table import StreamTableEnvironment

    class CountIn(MapFunction):
        def open(self, runtime_context) -> None:
            self.accepted = runtime_context.get_metrics_group().counter("baselineAccepted")

        def map(self, value):
            self.accepted.inc()
            return value

    class WindowAgg(ProcessWindowFunction):
        def process(self, key, context, elements):
            rows = list(elements)
            users = {event[1] for event in rows}
            watched = sum(int(event[6]) for event in rows)
            window = context.window()
            return [
                (
                    int(key),
                    int(window.start),
                    int(window.end),
                    watched,
                    len(rows),
                    len(users),
                )
            ]

    print(
        "baseline time=processing_time watermark=false dedup=false "
        "allowed_lateness_ms=0 dlq=false "
        f"parallelism={config.parallelism}"
    )
    configuration = Configuration()
    configuration.set_string("pipeline.name", getattr(config, "job_name", "") or "heartbeat-baseline")
    configuration.set_string("state.backend", "hashmap")
    env = StreamExecutionEnvironment.get_execution_environment(configuration)
    env.set_parallelism(config.parallelism)
    env.set_restart_strategy(
        RestartStrategies.fixed_delay_restart(config.restart_attempts, config.restart_delay_ms)
    )
    env.enable_checkpointing(config.checkpoint.interval_ms)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_topics(config.source_topic)
        .set_group_id(config.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    raw = env.from_source(source, WatermarkStrategy_no(), "kafka-heartbeats").uid("kafka-heartbeats")
    parsed = raw.map(
        lambda raw_line: _parse_heartbeat(raw_line, config.heartbeat_seconds),
        output_type=_event_type(),
    ).name("json").uid("json")
    counted = parsed.map(CountIn(), output_type=_event_type()).name("baseline-count").uid("baseline-count")
    tumbling = (
        counted.key_by(lambda event: event[2], key_type=Types.LONG())
        .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
        .process(WindowAgg(), output_type=_window_type())
        .name("baseline-processing-time-1m")
        .uid("baseline-processing-time-1m")
    )
    table_env = StreamTableEnvironment.create(env)
    apply_s3a(table_env, config)
    table_env.execute_sql(catalog_ddl(config))
    table_env.execute_sql(f"USE CATALOG {config.catalog}")
    table_env.execute_sql("CREATE DATABASE IF NOT EXISTS silver")
    table_env.execute_sql(heartbeat_table_ddl(config))
    table_env.execute_sql(windows_table_ddl(config))
    heartbeats = table_env.from_data_stream(counted).alias(
        "heartbeat_id",
        "user_id",
        "movie_id",
        "event_ts_ms",
        "session_id",
        "playback_position_seconds",
        "watched_delta_seconds",
        "raw_json",
    )
    table_env.create_temporary_view("baseline_heartbeats", heartbeats)
    windows = table_env.from_data_stream(tumbling).alias(
        "movie_id",
        "window_start_ms",
        "window_end_ms",
        "total_watch_seconds",
        "heartbeat_count",
        "active_users",
    )
    table_env.create_temporary_view("baseline_windows", windows)
    statement = table_env.create_statement_set()
    statement.add_insert_sql(heartbeat_insert_sql(config, "baseline_heartbeats"))
    statement.add_insert_sql(
        f"""
        INSERT INTO {config.silver_windows_table}
        SELECT movie_id,
               TO_TIMESTAMP_LTZ(window_start_ms, 3),
               TO_TIMESTAMP_LTZ(window_end_ms, 3),
               total_watch_seconds, heartbeat_count, active_users
        FROM baseline_windows
        """
    )
    result = statement.execute()
    client = result.get_job_client()
    if client is not None:
        print(f"job_id={client.get_job_id()}")


def WatermarkStrategy_no():
    from pyflink.common import WatermarkStrategy

    return WatermarkStrategy.no_watermarks()


def _event_type():
    from pyflink.common import Types

    return Types.TUPLE(
        [
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.STRING(),
        ]
    )


def _window_type():
    from pyflink.common import Types

    return Types.TUPLE(
        [
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
            Types.LONG(),
        ]
    )


def main() -> None:
    args = baseline_parser().parse_args()
    config = config_from_args(args)
    if args.engine == "local":
        payloads = proof_events() if args.scenario == "proof" else [json.loads(line) for line in __import__("sys").stdin if line.strip()]
        run_local(payloads)
        return
    run_flink(config)


if __name__ == "__main__":
    main()
