# Установка winutils.exe и hadoop.dll + переменные HADOOP_HOME и PATH.
# Windows 10/11 x64. Права администратора не нужны: переменные ставятся для текущего пользователя.
#
#   powershell -ExecutionPolicy Bypass -File host\install_winutils.ps1
#
# Параметры:
#   -Prefix         куда ставить (по умолчанию %USERPROFILE%\hadoop, файлы лягут в <Prefix>\bin)
#   -HadoopVersion  ветка сборки winutils (по умолчанию 3.3.6; в pyspark 3.5.8 клиент
#                   Hadoop 3.3.4, сборки 3.3.x взаимозаменяемы)
param(
    [string]$Prefix = "$env:USERPROFILE\hadoop",
    [string]$HadoopVersion = "3.3.6"
)
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"   # иначе Invoke-WebRequest качает в разы медленнее

if (-not [Environment]::Is64BitOperatingSystem) { throw "Нужна 64-битная Windows" }
if ($Prefix -match '\s') { throw "Путь установки не должен содержать пробелов: $Prefix (задайте -Prefix)" }

$BinDir  = Join-Path $Prefix "bin"
$BaseUri = "https://github.com/cdarlint/winutils/raw/master/hadoop-$HadoopVersion/bin"

# --- 1. winutils.exe + hadoop.dll --------------------------------------------
# Нужны оба файла: без hadoop.dll ошибка "Did not find winutils.exe" сменится на
# UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0
New-Item -ItemType Directory -Force $BinDir | Out-Null
foreach ($file in @("winutils.exe", "hadoop.dll")) {
    $target = Join-Path $BinDir $file
    if (Test-Path $target) {
        Write-Host "$file уже скачан: $target"
        continue
    }
    Write-Host "Скачиваю $file (hadoop-$HadoopVersion) ..."
    Invoke-WebRequest -UseBasicParsing -Uri "$BaseUri/$file" -OutFile $target
}

# --- 2. Переменные среды пользователя ----------------------------------------
[Environment]::SetEnvironmentVariable("HADOOP_HOME", $Prefix, "User")

# Правим именно пользовательский PATH: setx PATH "$env:PATH;..." склеил бы системный и
# пользовательский PATH в пользовательский и обрезал результат на 1024 символах.
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if (($userPath -split ';') -notcontains $BinDir) {
    $userPath = if ($userPath) { "$userPath;$BinDir" } else { $BinDir }
    [Environment]::SetEnvironmentVariable("Path", $userPath, "User")
}
# ... и в текущей сессии, чтобы проверка ниже работала без нового терминала
$env:HADOOP_HOME = $Prefix
$env:PATH = "$env:PATH;$BinDir"

# --- 3. Проверка -------------------------------------------------------------
# hadoop.dll ищется через java.library.path, куда на Windows входит PATH, —
# копировать её в C:\Windows\System32 не нужно, достаточно проверить, что файл на месте.
if ((Get-Item (Join-Path $BinDir "hadoop.dll")).Length -eq 0) {
    throw "hadoop.dll скачался пустым, удалите $BinDir и запустите скрипт заново"
}

# Именно ls/chmod вызывает Hadoop, когда выставляет права на локальных файлах.
# (winutils.exe systeminfo здесь не годится: на локализованной Windows он падает с
# PdhAddCounter ... 0xc0000bb8 — счётчики производительности названы не по-английски.)
& (Join-Path $BinDir "winutils.exe") ls $BinDir
if ($LASTEXITCODE -ne 0) { Write-Warning "winutils.exe ls завершилась с кодом $LASTEXITCODE" }

Write-Host ""
Write-Host "Готово: HADOOP_HOME = $Prefix"
