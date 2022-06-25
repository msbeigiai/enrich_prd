import os
from dotenv import load_dotenv

load_dotenv()

kafka = {
    "topic": os.environ.get("KAFKA_TOPIC"),
    "producer": os.environ.get("KAFKA_PRODUCER")
}

sql_conf = {
    "driver": 'ODBC Driver 17 for SQL Server',
    "server": 'tcp:'+os.environ.get("SQL_SERVER_HOST")+','+os.environ.get("SQL_SERVER_PORT"),
    "database": os.environ.get("SQL_DATABASE"),
    "username": os.environ.get("DATABASE_USERNAME"),
    "password": os.environ.get("DATABASE_PASS"),
}

redis_conf = {"host": os.environ.get("REDIS_HOST"), "port": os.environ.get("REDIS_PORT")}

kafka_config = {"server": os.environ.get("KAFKA_BOOTSTRAP_SERVER"),
                "port": os.environ.get("KAFKA_BOOTSTRAP_PORT")}