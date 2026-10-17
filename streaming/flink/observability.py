"""Metric nhẹ trên job Flink. Không thêm Prometheus, Grafana hay exporter.

Đọc trên REST ``http://127.0.0.1:18081``:

- Kafka lag: metric source ``pendingRecords`` (KafkaSource 1.18). Offset đã
  checkpoint nằm trong state của source, không phải consumer group.
- Watermark: ``currentInputWatermark`` và ``currentOutputWatermark`` theo
  subtask. Source ``kafka-heartbeats`` dùng ``no_watermarks``; watermark thật
  nằm ở ``bounded-out-of-orderness``, sau parse JSON.
- Dead letter: counter ``deadLetters`` trên side output của cửa sổ và trên
  cổng recovery.
- Dedup: ``dedupEmitted`` và ``dedupDropped``.
- RocksDB: ``estimateNumKeys``, ``estimateLiveDataSize`` khi hai flag
  ``state.backend.rocksdb.metrics.estimate-*`` bật.
- Checkpoint: ``GET /jobs/<id>/checkpoints`` trường ``end_to_end_duration``,
  ``state_size``, ``counts.failed``, ``counts.restored``, ``latest.restored``.
- Iceberg: cột ``operation`` và ``committed_at`` của ``<bảng>.snapshots``.
  Committer chỉ chạy sau checkpoint thành công.
- Kafka transaction: log TaskManager chứa abort/commit của producer
  ``transactional-id-prefix``. Consumer ``isolation.level=read_committed`` không
  thấy transaction đã abort. ``read_uncommitted`` có thể thấy bản ghi của
  transaction abort còn nằm trong log.

Hai sink chia sẻ barrier nhưng không có giao dịch nguyên tử xuyên Kafka và Iceberg.
"""

from __future__ import annotations

from streaming.flink.config import StreamJobConfig, flink_checkpoint_configs

DEDUP_EMITTED = "dedupEmitted"
DEDUP_DROPPED = "dedupDropped"
DEAD_LETTERS = "deadLetters"


def job_configuration(config: StreamJobConfig):
    """Cấu hình đi theo job: checkpoint, restart, và metric RocksDB."""
    from pyflink.common import Configuration

    configuration = Configuration()
    for key, value in flink_checkpoint_configs(config.checkpoint).items():
        configuration.set_string(key, value)
    configuration.set_string("state.backend.rocksdb.metrics.estimate-num-keys", "true")
    configuration.set_string("state.backend.rocksdb.metrics.estimate-live-data-size", "true")
    configuration.set_string("restart-strategy", "fixed-delay")
    configuration.set_string("restart-strategy.fixed-delay.attempts", str(config.restart_attempts))
    configuration.set_string("restart-strategy.fixed-delay.delay", f"{config.restart_delay_ms} ms")
    return configuration
