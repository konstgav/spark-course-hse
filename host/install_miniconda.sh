#!/usr/bin/env bash
# Установка Miniconda и окружения spark-course (Python 3.11 + Java 17 + host/requirements.txt).
# Linux (x86_64, aarch64) и macOS (Intel, Apple Silicon).
#
#   bash host/install_miniconda.sh
#
# Переменные окружения:
#   MINICONDA_PREFIX  куда ставить Miniconda (по умолчанию ~/miniconda3)
#   ENV_NAME          имя окружения (по умолчанию spark-course)
set -euo pipefail

PREFIX="${MINICONDA_PREFIX:-$HOME/miniconda3}"
ENV_NAME="${ENV_NAME:-spark-course}"
HOST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- 1. Miniconda ------------------------------------------------------------
if [ -n "${CONDA_EXE:-}" ] || command -v conda >/dev/null 2>&1; then
    CONDA="${CONDA_EXE:-$(command -v conda)}"
    echo "conda уже установлена: $CONDA"
elif [ -x "$PREFIX/bin/conda" ]; then
    CONDA="$PREFIX/bin/conda"
    echo "Miniconda уже установлена: $PREFIX"
else
    case "$(uname -s)-$(uname -m)" in
        Linux-x86_64)             INSTALLER=Miniconda3-latest-Linux-x86_64.sh ;;
        Linux-aarch64|Linux-arm64) INSTALLER=Miniconda3-latest-Linux-aarch64.sh ;;
        Darwin-x86_64)            INSTALLER=Miniconda3-latest-MacOSX-x86_64.sh ;;
        Darwin-arm64)             INSTALLER=Miniconda3-latest-MacOSX-arm64.sh ;;
        *) echo "Неподдерживаемая платформа: $(uname -s) $(uname -m)" >&2; exit 1 ;;
    esac

    TMP_DIR="$(mktemp -d)"
    trap 'rm -rf "$TMP_DIR"' EXIT
    echo "Скачиваю $INSTALLER ..."
    curl -fsSL -o "$TMP_DIR/miniconda.sh" "https://repo.anaconda.com/miniconda/$INSTALLER"

    echo "Устанавливаю Miniconda в $PREFIX ..."
    bash "$TMP_DIR/miniconda.sh" -b -p "$PREFIX"
    CONDA="$PREFIX/bin/conda"

    # Подключить conda к shell пользователя (добавляет блок в ~/.bashrc / ~/.zshrc).
    SHELL_NAME="$(basename "${SHELL:-bash}")"
    case "$SHELL_NAME" in
        bash|zsh|fish) "$CONDA" init "$SHELL_NAME" ;;
        *)             "$CONDA" init bash ;;
    esac
    # Не активировать base в каждом новом терминале.
    "$CONDA" config --set auto_activate_base false
fi

# --- 2. Окружение spark-course -----------------------------------------------
if "$CONDA" env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
    echo "Окружение $ENV_NAME уже существует, обновляю пакеты из requirements.txt"
else
    # --override-channels: только conda-forge, без канала defaults (не требует принятия ToS Anaconda).
    "$CONDA" create -y -n "$ENV_NAME" --override-channels -c conda-forge python=3.11 openjdk=17
fi
"$CONDA" run -n "$ENV_NAME" --no-capture-output python -m pip install -r "$HOST_DIR/requirements.txt"

# --- 3. Проверка -------------------------------------------------------------
"$CONDA" run -n "$ENV_NAME" --no-capture-output python -c \
    'import sys, pyspark; print("Python", sys.version.split()[0], "| pyspark", pyspark.__version__)'
"$CONDA" run -n "$ENV_NAME" --no-capture-output java -version

cat <<EOF

Готово. Откройте новый терминал и выполните:
    conda activate $ENV_NAME
    jupyter lab
EOF
