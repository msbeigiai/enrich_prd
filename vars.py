import os
from dotenv import load_dotenv
import json

load_dotenv()

topics = {
    "rtst_topic": "tdb_server08.dbo.RETAILTRANSACTIONSALESTRANS",
}

sql_conf = {
    "driver": 'ODBC Driver 17 for SQL Server',
    "server": 'tcp:172.31.70.20,1433',
    "database": 'MicrosoftDynamicsAX',
    "username": os.environ.get("DATABASE_USERNAME"),
    "password": os.environ.get("DATABASE_PASS"),
}
