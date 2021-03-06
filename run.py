from kafka import KafkaConsumer, KafkaProducer
from vars import *
import pyodbc
from redis import Redis
import time
from pathlib import Path
import json
from time import sleep


r = Redis(host=redis_conf["host"], port=redis_conf["port"], db=0)
r_header = Redis(host=redis_conf["host"], port=redis_conf["port"], db=1)
r_store = Redis(host=redis_conf["host"], port=redis_conf["port"], db=2)
r_custom = Redis(host=redis_conf["host"], port=redis_conf["port"], db=3)

consumer = KafkaConsumer(
    kafka["topic"],
    bootstrap_servers=kafka_config["server"] + ":" + kafka_config["port"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=kafka["topic"] + '__group_RTST',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config["server"] + ":" + kafka_config["port"],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

conn = pyodbc.connect(
    f'DRIVER={sql_conf["driver"]};SERVER=' + sql_conf["server"] + ';DATABASE=' + sql_conf["database"] + \
    ';UID=' + sql_conf["username"] + ';PWD=' + sql_conf["password"])
cursor = conn.cursor()


def fetch_needed_columns(nested_data):
    needed_columns = ["ITEMID", "RECID", "TRANSACTIONID", "PRICE", "DISCAMOUNT", "CUSTACCOUNT", "STORE"]
    return {k: v for (k, v) in nested_data.items() if k in [column for column in needed_columns]}


def fetch_name_alias(msg_without_name_alias):
    item_id = msg_without_name_alias["ITEMID"]
    name_alias = r.get(item_id)

    if name_alias is not '' and name_alias is not None:
        msg_without_name_alias["ITEMID"] = name_alias.decode('utf-8')

    elif name_alias is None:
        query = "select i.NAMEALIAS FROM INVENTTABLE i WITH(NOLOCK) " \
                "WHERE i.ITEMID = '%s'" % item_id

        # sleep(3.0)
        cursor.execute(query)
        value = cursor.fetchone()[0]
        msg_without_name_alias["ITEMID"] = value
        r.set(item_id, str(value))

    else:
        msg_without_name_alias["ITEMID"] = "Unknown"


def fetch_net_price(msg_without_net_price):
    price = msg_without_net_price["PRICE"]
    discount = msg_without_net_price["DISCAMOUNT"]
    net_price = price - discount
    msg_without_net_price["NETPRICE"] = float(net_price)


def fetch_store_name(msg_without_store_name):
    store_id = msg_without_store_name["STORE"]
    store_name = r_store.get(store_id)

    if store_name:
        msg_without_store_name["STORE"] = store_name.decode('utf-8')

    else:
        query = "select c.NAME " \
                f"from RETAILCHANNELTABLE a WITH(NOLOCK)" \
                f" inner join OMOPERATINGUNITVIEW b WITH(NOLOCK) " \
                f"on a.OMOPERATINGUNITID = b.RECID inner join DIRPARTYTABLE c WITH(NOLOCK)" \
                "ON c.PARTYNUMBER = b.PARTYNUMBER where a.STORENUMBER = '%s'" % store_id

        cursor.execute(query)
        value = cursor.fetchone()

        if value is [] or value is None:
            msg_without_store_name["STORE"] = "Unknown"
        else:
            r_store.set(store_id, str(value[0]))
            msg_without_store_name["STORE"] = value[0]


def fetch_custom_number(msg_without_custom_number):
    custom_account = str(msg_without_custom_number["CUSTACCOUNT"])

    if custom_account is '':
        msg_without_custom_number["CUSTACCOUNT"] = "Unknown"

    else:
        custom_phone_number = r_custom.get(custom_account)

        if custom_phone_number is not None and custom_phone_number is not '':
            msg_without_custom_number["CUSTACCOUNT"] = custom_phone_number.decode('utf-8')

        else:
            # print(f"{custom_account}")
            query = "SELECT d.NAMEALIAS FROM DIRPARTYTABLE d WITH(NOLOCK) " \
                    "INNER JOIN CUSTTABLE c WITH(NOLOCK) " \
                    "ON c.PARTY = d.RECID " \
                    "WHERE c.ACCOUNTNUM = '%s'" % custom_account
            cursor.execute(query)
            value = cursor.fetchone()

            if value is [] or value is None:
                msg_without_custom_number["CUSTACCOUNT"] = "Unknown"
            else:
                r_custom.set(custom_account, str(value[0]))
                msg_without_custom_number["CUSTACCOUNT"] = value[0]


def fetch_header(msg_without_header, transaction_id):
    get_header_items = r_header.lrange(transaction_id, 0, -1)

    if get_header_items is not None and get_header_items is not '' and len(get_header_items) != 0:
        msg_without_header["PAYMENTAMOUNT"] = float(get_header_items[1].decode('utf-8'))
        msg_without_header["CREATEDDATETIME"] = str(get_header_items[0].decode('utf-8'))
    else:
        query = "SELECT r.PAYMENTAMOUNT, r.CREATEDDATETIME FROM RETAILTRANSACTIONTABLE r WITH(NOLOCK) " \
                " WHERE r.TRANSACTIONID = '%s'" % transaction_id
        sleep(2)
        cursor.execute(query)
        temp = cursor.fetchone()
        header_items = [item for item in temp]
        for item in header_items:
            r.lpush(transaction_id, str(item))
        msg_without_header["PAYMENTAMOUNT"] = float(header_items[0])
        msg_without_header["CREATEDDATETIME"] = str(header_items[1])
        # msg_without_header["PAYMENTAMOUNT"] = "NO"
        # msg_without_header["CREATEDDATETIME"] = "NO"


def make_documents_from_msg(cleaned_msg):
    fetch_name_alias(cleaned_msg)
    fetch_net_price(cleaned_msg)
    fetch_store_name(cleaned_msg)
    fetch_custom_number(cleaned_msg)
    fetch_header(cleaned_msg, cleaned_msg["TRANSACTIONID"])


def send_producer(documented_msg):
    if producer:
        print(documented_msg)
        producer.send(kafka["producer"], documented_msg)


def write_to_json(msg_json, file_name):
    base = Path('data')
    path_to_save = base / file_name
    base.mkdir(exist_ok=True)

    with open(path_to_save, "w") as f:
        json.dump(msg_json, f)


for msg in consumer:
    if msg is None:
        continue

    msg = msg.value

    msg_with_needed_columns = fetch_needed_columns(msg["after"])
    make_documents_from_msg(msg_with_needed_columns)
    send_producer(msg_with_needed_columns)
    write_to_json(msg_with_needed_columns, f"data__{round(time.time() * 1000)}.json")
