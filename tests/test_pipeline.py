import copy
import json
import random
from pathlib import Path
import runpy
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch

import yaml


ROOT = Path(__file__).resolve().parents[1]


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.config = ModuleType("src.config")
        self.config.CONTRACT = yaml.safe_load((ROOT / "src/config/data_contract.yml").read_text())
        for name in ("RAW_TOPIC", "VALIDATED_TOPIC", "DLQ_TOPIC", "PARKING_LOT_TOPIC",
                     "SENDER_EMAIL", "RECEIVER_EMAIL", "SMTP_HOST", "SMTP_PASSWORD",
                     "DATABASE_SERVER", "DATABASE_NAME", "DATABASE_USER", "DATABASE_PASSWORD"):
            setattr(self.config, name, name)
        self.config.BOOTSTRAP_SERVERS = []
        self.config.RECOVERY_ATTEMPTS_THRESHOLD = 3
        self.config.SMTP_PORT = 465
        self.config.SCHEMA = "curly_store"
        self.config.EVENTS_TABLE = "processed_events"
        self.config_patch = patch.dict(sys.modules, {"src.config": self.config})
        self.config_patch.start()
        self.addCleanup(self.config_patch.stop)
        self.generator = runpy.run_path(str(ROOT / "src/producer/event_generator.py"))
        self.order = self.generator["_producer_sale"]()
        self.order.update(event_id="A94CBBA9-9C25-4E58-8453-2E83F92A19CD", event_version="1", event_type="order")
        self.refund = dict(refund_id="r", order_id="o", refund_amount=1.5,
                           refunded_at="2026-01-01", reason="Returned", event_type="refund",
                           event_id="b94cbba9-9c25-4e58-8453-2e83f92a19cd")

    def record(self, value, offset=0, headers=None):
        return SimpleNamespace(value=value, key=b"opaque-key", headers=headers or [],
                               topic=self.config.RAW_TOPIC, partition=0, offset=offset)

    def validate(self, values, producer=None):
        records = [self.record(copy.deepcopy(v), i) for i, v in enumerate(values)]
        consumer = Mock()
        producer = producer or Mock()
        with patch("kafka.KafkaConsumer") as consumer_class, patch("kafka.KafkaProducer", return_value=producer):
            def consume(*args, **kwargs):
                decoder = kwargs.get("value_deserializer", lambda v: v)
                for record in records:
                    if record.value is None or isinstance(record.value, bytes):
                        record.value = decoder(record.value)
                consumer.__iter__ = Mock(return_value=iter(records))
                return consumer
            consumer_class.side_effect = consume
            runpy.run_path(str(ROOT / "src/validation_consumer/validation.py"))
        return producer, consumer, records

    def recovery(self):
        return runpy.run_path(str(ROOT / "src/dlq_consumer/dlq_consumer.py"))

    def test_orders_and_refunds_pass_without_mutation(self):
        producer, consumer, records = self.validate([self.order, self.refund])
        self.assertEqual(consumer.commit.call_count, 2)
        self.assertEqual([c.args[0] for c in producer.send.call_args_list], [self.config.VALIDATED_TOPIC] * 2)
        self.assertEqual(producer.send.call_args_list[0].kwargs["value"], self.order)
        self.assertEqual(records[0].value, self.order)
        self.assertEqual(producer.send.call_args.kwargs["key"], b"opaque-key")

    def test_invalid_body_is_preserved_and_records_contract_version(self):
        invalid = {**self.order, "quantity": "wrong"}
        producer, _, records = self.validate([invalid])
        envelope = producer.send.call_args.kwargs["value"]
        self.assertEqual(producer.send.call_args.args[0], self.config.DLQ_TOPIC)
        self.assertEqual(envelope["original_message"], invalid)
        self.assertEqual(records[0].value, invalid)
        self.assertEqual(envelope["failure_metadata"]["contract_version"], 1)

    def test_legacy_ids_are_stable_on_replay(self):
        order = {k: v for k, v in self.order.items() if not k.startswith("event_")}
        first = self.validate([order])[0].send.call_args.kwargs["value"]
        second = self.validate([order])[0].send.call_args.kwargs["value"]
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertEqual(first["event_type"], "order")

    def test_malformed_records_do_not_block_following_order(self):
        values = [None, [], "text", b"{broken", b"\xff", self.order]
        producer, consumer, _ = self.validate(values)
        self.assertEqual(consumer.commit.call_count, len(values))
        self.assertEqual([c.args[0] for c in producer.send.call_args_list],
                         [self.config.DLQ_TOPIC] * 5 + [self.config.VALIDATED_TOPIC])
        import base64
        original = producer.send.call_args_list[4].kwargs["value"]["original_message"]
        self.assertEqual(base64.b64decode(original["data"]), b"\xff")

    def test_unknown_event_type_is_rejected(self):
        for event_type in ("unknown", None):
            producer, _, _ = self.validate([{**self.order, "event_type": event_type}])
            self.assertEqual(producer.send.call_args.args[0], self.config.DLQ_TOPIC)

    def test_valid_dlq_events_replay_with_metadata(self):
        recover = self.recovery()["_recover"]
        for event in (self.order, self.refund):
            with self.subTest(event=event["event_type"]):
                topic, payload, _, _ = recover(self.record({"original_message": event}))
                self.assertEqual(topic, self.config.RAW_TOPIC)
                self.assertEqual(payload, event)

    def test_repair_preserves_metadata_and_revalidates(self):
        recover = self.recovery()["_recover"]
        diagnosis = dict(valid=False, changes=[dict(column="quantity", issues=["type_changed"], expected="int")],
                         review=dict(status="completed", diagnosis=dict(category="representation_change", probability=.99)))
        actual_check = recover.__globals__["check_schema"]
        def check(contract, rows, **kwargs):
            return diagnosis if kwargs["ai_review"] else actual_check(contract, rows, **kwargs)
        invalid = {**self.order, "quantity": "2"}
        with patch.dict(recover.__globals__, check_schema=check):
            topic, payload, headers, _ = recover(self.record({"original_message": invalid}))
        self.assertEqual(topic, self.config.RAW_TOPIC)
        self.assertEqual(payload, {**self.order, "quantity": 2})
        self.assertIn(("recovery_attempts", b"1"), headers)
        self.assertEqual(invalid["quantity"], "2")

    def test_recovery_limit_and_invalid_envelopes_are_parked(self):
        recover = self.recovery()["_recover"]
        for value in (None, [], {}, {"original_message": {**self.order, "quantity": "bad"}}):
            with self.subTest(value=value):
                topic, _, _, _ = recover(self.record(value, headers=[("recovery_attempts", b"3")]))
                self.assertEqual(topic, self.config.PARKING_LOT_TOPIC)

    def test_failed_correction_is_not_published(self):
        recover = self.recovery()["_recover"]
        invalid = {**self.order, "quantity": "bad"}
        with patch.dict(recover.__globals__, _temporal_adaptation=lambda *args: {"quantity": 2}):
            with patch("src.SchemaDiagnoser.fn._review", return_value={"status": "failed"}):
                topic, payload, _, _ = recover(self.record({"original_message": invalid}))
        self.assertEqual(topic, self.config.PARKING_LOT_TOPIC)
        self.assertEqual(payload["original_message"], invalid)
        self.assertFalse(payload["failure_metadata"]["revalidation"]["valid"])

    def test_dlq_consumer_preserves_keys_and_parks_malformed_json(self):
        module = self.recovery()
        producer, consumer = Mock(), MagicMock()
        consume = module["dlq_consumer"]
        def create_consumer(*args, **kwargs):
            consumer.__iter__.return_value = iter([
                self.record(kwargs["value_deserializer"](b"{broken")),
                self.record(kwargs["value_deserializer"](json.dumps({"original_message": self.order}).encode())),
            ])
            return consumer
        with patch.dict(consume.__globals__, KafkaConsumer=create_consumer,
                        KafkaProducer=lambda **kw: producer, _send_alert=Mock()):
            consume()
        self.assertEqual([c.args[0] for c in producer.send.call_args_list],
                         [self.config.PARKING_LOT_TOPIC, self.config.RAW_TOPIC])
        self.assertEqual(consumer.commit.call_count, 2)
        self.assertEqual(producer.send.call_args.kwargs["key"], b"opaque-key")

    def test_generator_can_choose_both_event_types(self):
        generate = self.generator["produce_order"]
        with patch.dict(generate.__globals__, random=random.Random(67),
                        _producer_sale=lambda: "order", _producer_refund=lambda: "refund"):
            self.assertEqual({generate() for _ in range(100)}, {"order", "refund"})

    def test_refund_uses_selected_price_and_handles_empty_table(self):
        driver = ModuleType("mssql_python")
        driver.OperationalError = type("OperationalError", (Exception,), {})
        driver.ConnectionStringParseError = type("ConnectionStringParseError", (Exception,), {})
        driver.connect = MagicMock()
        cursor = driver.connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [("o-1", 10.0)]
        with patch.dict(sys.modules, mssql_python=driver):
            with patch("random.random", return_value=.5):
                refund = self.generator["_producer_refund"]()
            self.assertEqual(refund["order_id"], "o-1")
            self.assertEqual(refund["refund_amount"], 5.0)
            self.assertIn("refunded_at", refund)
            self.assertIn("total_price", cursor.execute.call_args.args[0])
            cursor.fetchall.return_value = []
            self.assertIn("order_id", self.generator["_producer_refund"]())

    def test_producer_surfaces_delivery_failure_and_closes(self):
        producer = Mock()
        producer.send.return_value.get.side_effect = RuntimeError("delivery failed")
        with patch("kafka.KafkaProducer", return_value=producer):
            get_orders = runpy.run_path(str(ROOT / "src/producer/kafka-producer.py"))["get_orders"]
        with patch.dict(get_orders.__globals__, _request_api=Mock(side_effect=[copy.deepcopy(self.order), {}])):
            with patch("time.sleep"), self.assertRaisesRegex(RuntimeError, "delivery failed"):
                get_orders()
        producer.close.assert_called_once()

    def test_validation_does_not_commit_failed_delivery(self):
        producer = Mock()
        producer.send.return_value.get.side_effect = RuntimeError("delivery failed")
        with self.assertRaisesRegex(RuntimeError, "delivery failed"):
            self.validate([self.order], producer)
        producer.close.assert_called_once()

    def test_invalid_event_ids_are_preserved_in_dlq_and_parked(self):
        recover = self.recovery()["_recover"]
        for event in (self.order, self.refund):
            for event_id in (None, "", 123, "not-a-uuid"):
                invalid = {**event, "event_id": event_id}
                producer, _, _ = self.validate([invalid])
                self.assertEqual(producer.send.call_args.args[0], self.config.DLQ_TOPIC)
                envelope = producer.send.call_args.kwargs["value"]
                topic, parked, _, _ = recover(self.record(envelope))
                self.assertEqual(topic, self.config.PARKING_LOT_TOPIC)
                self.assertEqual(parked["original_message"], invalid)

    def uploader(self):
        driver = ModuleType("mssql_python")
        driver.connect = MagicMock()
        driver.OperationalError = type("OperationalError", (Exception,), {})
        driver.ConnectionStringParseError = type("ConnectionStringParseError", (Exception,), {})
        with patch.dict(sys.modules, mssql_python=driver):
            upload = runpy.run_path(str(ROOT / "src/upload/uploader.py"))["upload_event"]
        conn = driver.connect.return_value.__enter__.return_value
        cursor = conn.cursor.return_value.__enter__.return_value
        return upload, driver, cursor

    def test_upload_preserves_ids_and_continues_after_duplicate(self):
        upload, driver, cursor = self.uploader()
        events = [self.order, self.order, self.refund]
        before = copy.deepcopy(events)
        cursor.fetchone.side_effect = [None, (self.order["event_id"],), None]
        consumer = MagicMock()
        consumer.__iter__.return_value = iter(self.record(e, i) for i, e in enumerate(events))
        with patch("kafka.KafkaConsumer", return_value=consumer):
            upload()
        inserts = [c.args for c in cursor.execute.call_args_list if c.args[0].startswith("INSERT")]
        self.assertEqual(len(inserts), 4)
        self.assertEqual([params["event_id"] for _, params in inserts],
                         [self.order["event_id"]] * 2 + [self.refund["event_id"]] * 2)
        self.assertEqual(inserts[0][1]["occurred_at"], self.order["order_date"])
        self.assertEqual(inserts[2][1]["occurred_at"], self.refund["refunded_at"])
        self.assertEqual(inserts[1][1]["total_price"], self.order["total_price"])
        self.assertEqual([sql.split()[2] for sql, _ in inserts],
                         ["[curly_store].[processed_events]", "[curly_store].orders",
                          "[curly_store].[processed_events]", "[curly_store].refunds"])
        self.assertEqual(consumer.commit.call_count, 3)
        self.assertEqual(events, before)
        consumer.close.assert_called_once()

    def test_upload_does_not_acknowledge_failed_transaction(self):
        for event in (self.order, self.refund):
            upload, driver, cursor = self.uploader()
            cursor.fetchone.return_value = None
            cursor.execute.side_effect = [None, None, driver.OperationalError("insert failed")]
            consumer = MagicMock()
            consumer.__iter__.return_value = iter([self.record(event)])
            with patch("kafka.KafkaConsumer", return_value=consumer):
                with self.assertRaisesRegex(driver.OperationalError, "insert failed"):
                    upload()
            consumer.commit.assert_not_called()
            self.assertIs(driver.connect.return_value.__exit__.call_args.args[0], driver.OperationalError)
            consumer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
