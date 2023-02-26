import os


PG_DSL = {
    "dbname": os.environ.get("DB_NAME", "movies_database"),
    "user": os.environ.get("DB_USER", "app"),
    "password": os.environ.get("DB_PASSWORD", "123qwe"),
    "host": os.environ.get("HOST", "127.0.0.1"),
    "port": os.environ.get("PORT", 5432),
}
