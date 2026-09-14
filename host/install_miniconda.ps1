# Установка Miniconda и окружения spark-course (Python 3.11 + Java 17 + host\requirements.txt).
# Windows 10/11 x64. Права администратора не нужны: установка только для текущего пользователя.
#
#   powershell -ExecutionPolicy Bypass -File host\install_miniconda.ps1
#
# Параметры:
#   -Prefix   куда ставить Miniconda (по умолчанию %USERPROFILE%\miniconda3)
#   -EnvName  имя окружения (по умолчанию spark-course)
param(
    [string]$Prefix = "$env:USERPROFILE\miniconda3",
    [string]$EnvName = "spark-course"
)
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"   # иначе Invoke-WebRequest качает в разы медленнее

$HostDir = $PSScriptRoot

function Invoke-CondaCmd {
    & $Conda @args
    if ($LASTEXITCODE -ne 0) { throw "conda $args завершилась с кодом $LASTEXITCODE" }
}

# --- 1. Miniconda ------------------------------------------------------------
$existing = if ($env:CONDA_EXE) { $env:CONDA_EXE } else {
    (Get-Command conda.exe -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1).Source
}
if ($existing -and (Test-Path $existing)) {
    $Conda = $existing
    Write-Host "conda уже установлена: $Conda"
} elseif (Test-Path "$Prefix\Scripts\conda.exe") {
    $Conda = "$Prefix\Scripts\conda.exe"
    Write-Host "Miniconda уже установлена: $Prefix"
} else {
    if (-not [Environment]::Is64BitOperatingSystem) { throw "Нужна 64-битная Windows" }
    if ($Prefix -match '\s') { throw "Путь установки не должен содержать пробелов: $Prefix (задайте -Prefix)" }

    $Installer = Join-Path $env:TEMP "Miniconda3-latest-Windows-x86_64.exe"
    Write-Host "Скачиваю Miniconda ..."
    Invoke-WebRequest -UseBasicParsing -OutFile $Installer `
        -Uri "https://repo.anaconda.com/miniconda/Miniconda3-latest-Windows-x86_64.exe"

    Write-Host "Устанавливаю Miniconda в $Prefix (1-3 минуты) ..."
    $p = Start-Process -FilePath $Installer -Wait -PassThru -ArgumentList @(
        "/InstallationType=JustMe", "/AddToPath=0", "/RegisterPython=0", "/S", "/D=$Prefix"
    )
    Remove-Item $Installer -ErrorAction SilentlyContinue
    if ($p.ExitCode -ne 0) { throw "Установщик Miniconda завершился с кодом $($p.ExitCode)" }

    $Conda = "$Prefix\Scripts\conda.exe"
    # Подключить conda к PowerShell и cmd, не активировать base в каждом терминале.
    Invoke-CondaCmd init powershell cmd.exe
    Invoke-CondaCmd config --set auto_activate_base false
}

# --- 2. Окружение spark-course -----------------------------------------------
$envs = & $Conda env list | ForEach-Object { ($_ -split '\s+')[0] }
if ($envs -contains $EnvName) {
    Write-Host "Окружение $EnvName уже существует, обновляю пакеты из requirements.txt"
} else {
    # --override-channels: только conda-forge, без канала defaults (не требует принятия ToS Anaconda).
    Invoke-CondaCmd create -y -n $EnvName --override-channels -c conda-forge python=3.11 openjdk=17
}
Invoke-CondaCmd run -n $EnvName --no-capture-output python -m pip install -r "$HostDir\requirements.txt"

# --- 3. Проверка -------------------------------------------------------------
Invoke-CondaCmd run -n $EnvName --no-capture-output python --version
Invoke-CondaCmd run -n $EnvName --no-capture-output python -m pip show pyspark
Invoke-CondaCmd run -n $EnvName --no-capture-output java -version

Write-Host ""
Write-Host "Готово. Откройте новое окно PowerShell (или Anaconda Prompt из меню Пуск) и выполните:"
Write-Host "    conda activate $EnvName"
Write-Host "    jupyter lab"
Write-Host ""
Write-Host "Если PowerShell пишет, что выполнение сценариев отключено, один раз выполните:"
Write-Host "    Set-ExecutionPolicy -Scope CurrentUser RemoteSigned"
