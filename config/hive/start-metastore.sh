#!/bin/bash
# Запуск Hive Metastore: при первом старте создаёт схему в БД metastore (Postgres).
# hive-site.xml к этому моменту уже сгенерирован entrypoint'ом образа из HIVE_SITE_CONF_*.
# (Вынесено в файл: entrypoint bde2020 делает `exec $@` без кавычек и ломает `bash -c "..."`.)
set -e

if /opt/hive/bin/schematool -dbType postgres -info >/dev/null 2>&1; then
  echo "Hive metastore schema already initialized"
else
  echo "Initializing Hive metastore schema..."
  /opt/hive/bin/schematool -dbType postgres -initSchema
fi

exec /opt/hive/bin/hive --service metastore
