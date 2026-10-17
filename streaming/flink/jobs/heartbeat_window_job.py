"""Job heartbeat: tumbling event-time 1 phút, DLQ, Iceberg exactly-once.

Chạy local (không cần cụm) để kiểm chứng watermark::

    PYTHONPATH=. uv run python -m streaming.flink.jobs.heartbeat_window_job --engine local --scenario proof

Submit lên JobManager (trong mạng ``cinestream_default``)::

    python -m streaming.flink.jobs.heartbeat_window_job --engine flink \\
        --bootstrap kafka:29092 --iceberg-rest http://iceberg-rest:8181
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from streaming.flink.config import CheckpointConfig, StreamJobConfig, flink_checkpoint_configs
from streaming.flink.operators.late_policy import WindowSpec, dead_letter_record, is_too_late
from streaming.flink.operators.watch_metrics import watched_delta_seconds
from streaming.flink.operators.watermark_assigner import (
    advance_aligned_watermark,
    extract_event_time_ms,
    restored_watermark_ahead,
)
from streaming.flink.operators.window_engine import HeartbeatWindowEngine
from streaming.flink.sinks.iceberg_sink import (
    apply_s3a,
    catalog_ddl,
    heartbeat_insert_sql,
    heartbeat_table_ddl,
    windows_table_ddl,
)
from streaming.flink.sinks.kafka_dlq_sink import dead_letter_value, kafka_dlq_table_ddl


def _dlq_partition_key(_event: tuple) -> int:
    """Một key để cổng thấy watermark đã căn, tức min watermark các subtask nguồn."""
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CineStream Flink heartbeat windows.")
    parser.add_argument("--engine", choices=("local", "flink"), default="local")
    parser.add_argument("--scenario", choices=("proof", "stdin"), default="proof")
    parser.add_argument("--bootstrap", default=None, help="Mặc định kafka:29092 trong mạng, localhost:19092 trên host.")
    parser.add_argument("--source-topic", default="movie.playback.heartbeat")
    parser.add_argument("--dlq-topic", default="movie.playback.deadletter")
    parser.add_argument("--group-id", default="cinestream-heartbeat-windows")
    parser.add_argument("--out-of-orderness-ms", type=int, default=15_000)
    parser.add_argument("--allowed-lateness-ms", type=int, default=15_000)
    parser.add_argument("--heartbeat-seconds", type=int, default=10)
    parser.add_argument("--iceberg-rest", default="http://iceberg-rest:8181")
    parser.add_argument("--warehouse", default="s3a://lakehouse/")
    parser.add_argument("--s3-endpoint", default="http://minio:9000")
    parser.add_argument("--checkpoint-dir", default="file:///opt/flink/checkpoints")
    parser.add_argument("--checkpoint-interval-ms", type=int, default=10_000)
    parser.add_argument("--state-backend", choices=("rocksdb", "hashmap"), default="rocksdb")
    parser.add_argument(
        "--dedup-ttl-ms",
        type=int,
        default=3_600_000,
        help="TTL processing time của state heartbeat_id. 0 tắt TTL. Không phải SLA nghiệp vụ.",
    )
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--idle-timeout-s", type=int, default=30)
    parser.add_argument("--restart-attempts", type=int, default=10)
    parser.add_argument("--restart-delay-ms", type=int, default=5_000)
    parser.add_argument("--transactional-id-prefix", default="cinestream-heartbeat-dlq")
    parser.add_argument(
        "--sessions-table",
        default="cinestream.silver.stg_playback_heartbeats",
        help="Bảng heartbeat silver. Không trỏ vào stg_watch_sessions của batch.",
    )
    parser.add_argument("--windows-table", default="cinestream.silver.stg_playback_windows")
    return parser


def config_from_args(args: argparse.Namespace) -> StreamJobConfig:
    bootstrap = args.bootstrap or ("localhost:19092" if args.engine == "local" else "kafka:29092")
    return StreamJobConfig(
        bootstrap_servers=bootstrap,
        source_topic=args.source_topic,
        dlq_topic=args.dlq_topic,
        group_id=args.group_id,
        out_of_orderness_ms=args.out_of_orderness_ms,
        allowed_lateness_ms=args.allowed_lateness_ms,
        heartbeat_seconds=args.heartbeat_seconds,
        iceberg_rest_uri=args.iceberg_rest,
        warehouse=args.warehouse,
        s3_endpoint=args.s3_endpoint,
        silver_sessions_table=args.sessions_table,
        silver_windows_table=args.windows_table,
        checkpoint=CheckpointConfig(
            interval_ms=args.checkpoint_interval_ms,
            storage_dir=args.checkpoint_dir,
            state_backend=args.state_backend,
        ),
        dedup_ttl_ms=args.dedup_ttl_ms,
        parallelism=args.parallelism,
        idle_timeout_s=args.idle_timeout_s,
        restart_attempts=args.restart_attempts,
        restart_delay_ms=args.restart_delay_ms,
        transactional_id_prefix=args.transactional_id_prefix,
    )


def proof_events() -> list[dict]:
    """Chuỗi tới lộn xộn. Mốc 0 là epoch; trễ 10s tới sau khi watermark đã vượt 60s."""

    def row(heartbeat_id: int, user_id: int, event_ms: int) -> dict:
        stamp = datetime.fromtimestamp(event_ms / 1000, tz=timezone.utc).isoformat()
        return {
            "heartbeat_id": heartbeat_id,
            "session_id": f"ses-{heartbeat_id:06d}",
            "user_id": user_id,
            "movie_id": 42,
            "event_ts": stamp,
            "current_position_seconds": 10,
            "playback_speed": 1.0,
            "bitrate_kbps": 4000,
        }

    return [
        row(1, 10, 0),
        row(2, 11, 30_000),
        row(3, 12, 90_000),
        row(4, 13, 10_000),
        row(5, 14, 70_000),
        row(2, 11, 30_000),
        row(6, 15, 180_000),
    ]


def run_local(config: StreamJobConfig, payloads: list[dict]) -> dict:
    result = HeartbeatWindowEngine(config).process_all(payloads)
    report = {
        "accepted": result.accepted,
        "duplicates": result.duplicates,
        "dead_letters": len(result.dead_letters),
        "dead_letter_ids": [int(item["heartbeat_id"]) for item in result.dead_letters],
        "final_watermark_ms": result.final_watermark_ms,
        "closed_windows": [row.as_dict() for row in result.closed_windows],
        "open_windows": [row.as_dict() for row in result.open_windows],
        "session_rows": len(result.session_rows),
        "checkpoint": flink_checkpoint_configs(config.checkpoint),
        "dedup_ttl_ms": config.dedup_ttl_ms,
        "dedup_ttl_time": "processing_time",
        "dlq_topic": config.dlq_topic,
        "sessions_table": config.silver_sessions_table,
        "windows_table": config.silver_windows_table,
    }
    print(json.dumps(report, indent=2))
    return report


def _parse_heartbeat(raw: str, heartbeat_seconds: int = 10):
    payload = json.loads(raw)
    event_ms = extract_event_time_ms(payload)
    session = payload.get("session_id", "0")
    digits = "".join(character for character in str(session) if character.isdigit())
    session_id = int(digits) if digits else 0
    return (
        int(payload["heartbeat_id"]),
        int(payload["user_id"]),
        int(payload["movie_id"]),
        event_ms,
        session_id,
        int(payload.get("current_position_seconds") or 0),
        watched_delta_seconds(payload, heartbeat_seconds),
        raw,
    )


def run_flink(config: StreamJobConfig) -> None:
    """Tumbling 1 phút. DLQ là ``side_output_late_data``. Cổng recovery bù watermark không checkpoint."""
    from pyflink.common import Duration, RestartStrategies, Time, Types, WatermarkStrategy
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream import OutputTag, StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
    from pyflink.datastream.window import TumblingEventTimeWindows
    from pyflink.table import StreamTableEnvironment

    from streaming.flink.observability import job_configuration

    if config.dedup_ttl_ms <= 0:
        print("dedup_ttl_ms<=0: state khử trùng không hết hạn")
    print(
        f"dedup_ttl_ms={config.dedup_ttl_ms} ttl_time=processing_time "
        f"parallelism={config.parallelism} idle_timeout_s={config.idle_timeout_s} "
        f"restart=fixed-delay attempts={config.restart_attempts} delay_ms={config.restart_delay_ms}"
    )
    dedup_fn, window_fn, gate_fn, late_json_fn = _operator_classes()
    env = StreamExecutionEnvironment.get_execution_environment(job_configuration(config))
    env.set_parallelism(config.parallelism)
    env.set_restart_strategy(
        RestartStrategies.fixed_delay_restart(config.restart_attempts, config.restart_delay_ms)
    )
    _apply_checkpoint(env, config)
    # Kafka connector và Iceberg runtime đã nằm trong /opt/flink/lib.
    # add_jars đưa iceberg-flink-runtime lên ChildFirstClassLoader của user code,
    # trong khi app classloader cũng nạp JAR đó. Histogram của Dropwizard bị định
    # nghĩa hai lần và checkpoint ném LinkageError.

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_topics(config.source_topic)
        .set_group_id(config.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    # UID tường minh là mốc savepoint của graph này. Không có savepoint production.
    # TTL bọc serializer của state "seen", nên checkpoint lấy trước khi có TTL
    # không restore được vào operator dedup.
    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-heartbeats")
    parsed = raw.map(
        lambda raw_line: _parse_heartbeat(raw_line, config.heartbeat_seconds),
        output_type=_event_type(),
    ).name("json").uid("json")
    # with_idleness() trả về WatermarkStrategy mới và xóa _timestamp_assigner.
    # Assigner phải gắn sau, nếu không mọi record giữ timestamp Long.MIN_VALUE.
    # Watermark nằm sau parse: source không đọc được event_ts từ JSON.
    timed = parsed.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(config.out_of_orderness_ms))
        .with_idleness(Duration.of_seconds(config.idle_timeout_s))
        .with_timestamp_assigner(_EventTimeAssigner())
    ).name("bounded-out-of-orderness").uid("bounded-out-of-orderness")

    deduped = timed.key_by(lambda event: event[0], key_type=Types.LONG()).process(
        dedup_fn(config.dedup_ttl_ms), output_type=_event_type()
    ).name("dedup-heartbeat-id").uid("dedup-heartbeat-id")
    recovery_tag = OutputTag("recovery-dead-letter", Types.STRING())
    late_tag = OutputTag("too-late", _event_type())
    spec = WindowSpec.from_config(config)
    gated = deduped.key_by(_dlq_partition_key, key_type=Types.INT()).process(
        gate_fn(spec, recovery_tag),
        output_type=_event_type(),
    ).name("recovery-late-gate").uid("recovery-late-gate")
    tumbling = (
        gated.key_by(lambda event: event[2], key_type=Types.LONG())
        .window(TumblingEventTimeWindows.of(Time.milliseconds(config.tumbling_ms)))
        .allowed_lateness(config.allowed_lateness_ms)
        .side_output_late_data(late_tag)
        .process(window_fn(), output_type=_window_type())
        .name("tumbling-1m")
        .uid("tumbling-1m")
    )
    late_json = tumbling.get_side_output(late_tag).map(
        late_json_fn(config.tumbling_ms),
        output_type=Types.STRING(),
    ).name("late-json").uid("late-json")
    dlq_json = gated.get_side_output(recovery_tag).union(late_json)
    table_env = StreamTableEnvironment.create(env)
    apply_s3a(table_env, config)
    # Tạo sink Kafka khi catalog hiện hành vẫn là default_catalog.
    table_env.execute_sql(kafka_dlq_table_ddl(config))
    table_env.execute_sql(catalog_ddl(config))
    table_env.execute_sql(f"USE CATALOG {config.catalog}")
    table_env.execute_sql("CREATE DATABASE IF NOT EXISTS silver")
    table_env.execute_sql(heartbeat_table_ddl(config))
    table_env.execute_sql(windows_table_ddl(config))
    sessions = table_env.from_data_stream(deduped).alias(
        "heartbeat_id",
        "user_id",
        "movie_id",
        "event_ts_ms",
        "session_id",
        "playback_position_seconds",
        "watched_delta_seconds",
        "raw_json",
    )
    table_env.create_temporary_view("deduped_heartbeats", sessions)
    windows = table_env.from_data_stream(tumbling).alias(
        "movie_id",
        "window_start_ms",
        "window_end_ms",
        "total_watch_seconds",
        "heartbeat_count",
        "active_users",
    )
    table_env.create_temporary_view("playback_windows", windows)
    late_table = table_env.from_data_stream(dlq_json).alias("payload")
    table_env.create_temporary_view("late_events", late_table)
    statement = table_env.create_statement_set()
    statement.add_insert_sql(heartbeat_insert_sql(config))
    statement.add_insert_sql(
        "INSERT INTO default_catalog.default_database.dlq_sink SELECT payload FROM late_events"
    )
    statement.add_insert_sql(
        f"""
        INSERT INTO {config.silver_windows_table}
        SELECT movie_id,
               TO_TIMESTAMP_LTZ(window_start_ms, 3),
               TO_TIMESTAMP_LTZ(window_end_ms, 3),
               total_watch_seconds, heartbeat_count, active_users
        FROM playback_windows
        """
    )
    result = statement.execute()
    client = result.get_job_client()
    if client is not None:
        print(f"job_id={client.get_job_id()}")


def _apply_checkpoint(env, config: StreamJobConfig) -> None:
    from pyflink.datastream import CheckpointingMode

    env.enable_checkpointing(config.checkpoint.interval_ms)
    checkpoint = env.get_checkpoint_config()
    checkpoint.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    checkpoint.set_min_pause_between_checkpoints(config.checkpoint.min_pause_ms)
    checkpoint.set_checkpoint_timeout(config.checkpoint.timeout_ms)
    if hasattr(checkpoint, "set_checkpoint_storage_dir"):
        checkpoint.set_checkpoint_storage_dir(config.checkpoint.storage_dir)
    else:
        checkpoint.set_checkpoint_storage(config.checkpoint.storage_dir)
    if config.checkpoint.state_backend == "rocksdb":
        from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend

        env.set_state_backend(EmbeddedRocksDBStateBackend(config.checkpoint.incremental))


def _event_time_ms(value, record_timestamp: int) -> int:
    return int(value[3])


class _EventTimeAssigner:
    """Gán event time từ field ``event_ts_ms``. Không import PyFlink để test local vẫn nạp được module."""

    extract_timestamp = staticmethod(_event_time_ms)


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


def _operator_classes():
    """Lớp toán tử chỉ nạp khi chạy ``--engine flink`` (cần PyFlink)."""
    from pyflink.common import Types
    from pyflink.datastream.functions import KeyedProcessFunction, MapFunction, ProcessWindowFunction
    from pyflink.datastream.state import ValueStateDescriptor

    class Dedup(KeyedProcessFunction):
        def __init__(self, ttl_ms: int):
            self.ttl_ms = ttl_ms

        def open(self, runtime_context) -> None:
            from pyflink.common import Time
            from pyflink.datastream.state import StateTtlConfig

            # Tên descriptor "seen" giữ nguyên cho các checkpoint sau mốc TTL.
            descriptor = ValueStateDescriptor("seen", Types.BOOLEAN())
            if self.ttl_ms > 0:
                ttl = (
                    StateTtlConfig.new_builder(Time.milliseconds(self.ttl_ms))
                    .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .set_ttl_time_characteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                    .build()
                )
                descriptor.enable_time_to_live(ttl)
            self.seen = runtime_context.get_state(descriptor)
            metrics = runtime_context.get_metrics_group()
            self.emitted = metrics.counter("dedupEmitted")
            self.dropped = metrics.counter("dedupDropped")

        def process_element(self, value, ctx):
            if self.seen.value():
                self.dropped.inc()
                return
            self.seen.update(True)
            self.emitted.inc()
            yield value

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

    class RecoveryGate(KeyedProcessFunction):
        """Chặn replay bằng watermark đầu vào đã checkpoint, không bằng max event time.

        Key hằng đặt cổng sau bước căn watermark, nên ``current_watermark`` là
        min các subtask nguồn, cùng watermark cửa sổ tumbling đang dùng.
        ``side_output_late_data`` xử lý đường đang chạy. Sau restore, watermark
        sống về ``Long.MIN_VALUE``. Cổng dùng bản đã lưu và không đưa event quá
        cleanup vào cửa sổ.

        Timer processing time một giây ghi nhận watermark khi không có event,
        kể cả khi subtask idle được nhả. Watermark chỉ tăng. Checkpoint lấy
        trước khi watermark vượt cleanup không chứa quyết định muộn hơn; replay
        của khoảng đó đi vào cửa sổ.
        """

        _SAMPLE_MS = 1000

        def __init__(self, spec: WindowSpec, recovery_tag):
            self.spec = spec
            self.recovery_tag = recovery_tag

        def open(self, runtime_context) -> None:
            self.dead_letters = runtime_context.get_metrics_group().counter("deadLetters")
            # UID operator giữ nguyên. Tên state đổi từ max_event_ts.
            # Không có savepoint production. Checkpoint cũ bỏ state tên cũ;
            # aligned_watermark bắt đầu rỗng nên không tái hiện max toàn cục.
            self.aligned_watermark = runtime_context.get_state(
                ValueStateDescriptor("aligned_watermark", Types.LONG())
            )
            self._seen_element = False
            self._restored_watermark = None
            self._timer_set = False

        def process_element(self, value, ctx):
            self._arm_timer(ctx)
            live = ctx.timer_service().current_watermark()
            stored = self.aligned_watermark.value()
            if not self._seen_element:
                self._seen_element = True
                self._restored_watermark = int(stored) if stored is not None else None
            self._store_watermark(stored, live)
            ahead = restored_watermark_ahead(live, self._restored_watermark)
            if ahead is not None and is_too_late(int(value[3]), ahead, self.spec):
                self.dead_letters.inc()
                payload = json.loads(value[7])
                record = dead_letter_record(payload, int(value[3]), ahead, self.spec.tumbling_ms)
                yield self.recovery_tag, dead_letter_value(record)
                return
            yield value

        def on_timer(self, timestamp, ctx):
            self._store_watermark(
                self.aligned_watermark.value(),
                ctx.timer_service().current_watermark(),
            )
            now = int(ctx.timer_service().current_processing_time())
            ctx.timer_service().register_processing_time_timer(max(int(timestamp), now) + self._SAMPLE_MS)
            if False:
                yield None

        def _arm_timer(self, ctx) -> None:
            if self._timer_set:
                return
            self._timer_set = True
            now = int(ctx.timer_service().current_processing_time())
            ctx.timer_service().register_processing_time_timer(now + self._SAMPLE_MS)

        def _store_watermark(self, stored, live) -> None:
            updated = advance_aligned_watermark(None if stored is None else int(stored), live)
            if updated is None:
                return
            if stored is None or int(stored) != updated:
                self.aligned_watermark.update(updated)

    class LateJson(MapFunction):
        def __init__(self, tumbling_ms: int):
            self.tumbling_ms = tumbling_ms

        def open(self, runtime_context) -> None:
            self.dead_letters = runtime_context.get_metrics_group().counter("deadLetters")

        def map(self, value):
            self.dead_letters.inc()
            payload = json.loads(value[7])
            record = dead_letter_record(payload, int(value[3]), None, self.tumbling_ms)
            return dead_letter_value(record)

    return Dedup, WindowAgg, RecoveryGate, LateJson




def main() -> None:
    args = build_parser().parse_args()
    config = config_from_args(args)
    if args.engine == "local":
        payloads = proof_events() if args.scenario == "proof" else [json.loads(line) for line in __import__("sys").stdin if line.strip()]
        run_local(config, payloads)
        return
    run_flink(config)


if __name__ == "__main__":
    main()
