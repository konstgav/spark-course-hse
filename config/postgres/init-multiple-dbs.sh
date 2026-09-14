#!/bin/bash
# Создаёт несколько баз данных в одном контейнере postgres при первом старте.
# Список баз передаётся через переменную окружения POSTGRES_MULTIPLE_DATABASES
# (см. .env). Используется официальным механизмом docker-entrypoint-initdb.d.
set -e
set -u

function create_database() {
	local database=$1
	echo "  -> создаю базу '$database' и выдаю права пользователю '$POSTGRES_USER'"
	psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
	    SELECT 'CREATE DATABASE "$database"'
	    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$database')\gexec
	    GRANT ALL PRIVILEGES ON DATABASE "$database" TO "$POSTGRES_USER";
EOSQL
}

if [ -n "${POSTGRES_MULTIPLE_DATABASES:-}" ]; then
	echo "Обнаружен список баз для создания: $POSTGRES_MULTIPLE_DATABASES"
	for db in $(echo "$POSTGRES_MULTIPLE_DATABASES" | tr ',' ' '); do
		create_database "$db"
	done
	echo "Все базы данных созданы."
fi
