import mysql.connector

DB_CONFIG = {
    "host": "x4cgo0w4k88c044c0gss04gk",
    "user": "mysql",
    "password": "UJXZyMoIoIxydtGBzKtguBfkBJueK7MhV3H4tToEeW2kQJZ7xcYhWTrMcvy1W198",
    "database": "default",
    "port": 3306
}

def get_db():
    return mysql.connector.connect(**DB_CONFIG)