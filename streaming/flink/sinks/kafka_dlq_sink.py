"""Dead letter của tumbling window ghi sang topic ``movie.playback.deadletter``.

Một heartbeat quá cleanup là một JSON: payload gốc, ``record_kind=dead_letter``,
``reason=window_past_cleanup``. Sink Kafka giữ exactly-once theo checkpoint.
"""

from __future__ import annotations

import json

from streaming.flink.config import StreamJobConfig


def dead_letter_value(payload: dict) -> str:
    return json.dumps(payload, separators=(",", ":"), default=str)


def kafka_dlq_table_ddl(config: StreamJobConfig, table_name: str = "dlq_sink") -> str:
    """Bảng tạm Kafka trong catalog mặc định.

    ``StatementSet.execute()`` chỉ kéo lineage của các INSERT. ``sink_to`` trên
    DataStream là nhánh anh em của Iceberg nên không vào graph. Đưa DLQ vào
    cùng statement set để một checkpoint commit cả Iceberg và Kafka.
    """
    return f"""
CREATE TEMPORARY TABLE {table_name} (
  payload STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '{config.dlq_topic}',
  'properties.bootstrap.servers' = '{config.bootstrap_servers}',
  'format' = 'raw',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = '{config.transactional_id_prefix}',
  'properties.transaction.timeout.ms' = '600000'
)
""".strip()


def build_kafka_dlq_sink(config: StreamJobConfig):
    """KafkaSink exactly-once. Transaction commit khi checkpoint hoàn tất."""
    from pyflink.common import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import (
        DeliveryGuarantee,
        KafkaRecordSerializationSchema,
        KafkaSink,
    )

    return (
        KafkaSink.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(config.dlq_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .set_transactional_id_prefix(config.transactional_id_prefix)
        .set_property("transaction.timeout.ms", "60000")
        .build()
    )
