#!/bin/sh

if [ "$DATABASE" = "Postgres" ]
then
    echo "Waiting for postgres..."

    while ! nc -z $DB_HOST $DB_PORT; do
      sleep 0.1
    done

    echo "PostgreSQL started"

fi

python3 manage.py run

psql postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME -f schema_design/movies_database.ddl

exec "$@"
