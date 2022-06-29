from vars import *
from redis import Redis
from kafka import KafkaConsumer, KafkaProducer
import json
import datetime

r = Redis(host=redis_conf["host"], port=redis_conf["port"], db=1)

consumer = KafkaConsumer(
    kafka["topic_2"],
    bootstrap_servers=kafka_config["server"] + ":" + kafka_config["port"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=kafka["topic"] + '__group033',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


def set_value(row_data):
    transaction_id = row_data["TRANSACTIONID"]
    payment_amount = row_data["PAYMENTAMOUNT"]
    created_datetime = row_data["CREATEDDATETIME"]
    created_datetime = datetime.datetime.fromtimestamp(created_datetime/1000.0)
    r.lpush(transaction_id, str(payment_amount), str(created_datetime))


def check_data_exist(transaction_id):
    temp_data = r.lrange(transaction_id, 0, -1)
    if temp_data is not None and temp_data is not "" and len(temp_data) != 0:
        return True
    else:
        return False


for msg in consumer:
    if msg is None:
        continue

    msg = msg.value
    check_if_exist = check_data_exist(msg["after"]["TRANSACTIONID"])

    if check_if_exist is False:
        set_value(msg["after"])



