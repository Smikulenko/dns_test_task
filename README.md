# Test_task
Создает и заполняет таблицы и на их основе  оптимально распределяет остатки товаров
# **Как запустить проект:**
Клонировать репозиторий и перейти в него в командной строке:

поместить необходимые csv файлы в папку data

Cоздать и активировать виртуальное окружение:
```
python -m venv .venv
```
```
source .venv/Scripts/activate
```
Установить зависимости из файла requirements.txt:
```
python -m pip install --upgrade pip
```
```
pip install -r requirements.txt
```
Заполнить .env файл по примеру
```
DB_NAME=dbtest
DB_USER=user
DB_PASSWORD=secret
DB_HOST=localhost
DB_PORT=5432
```
скрипт по созданию и заполнению таблиц запускаеться командой
```
python create_and_fill.py
```
скрипт по запуску алгоритма который оптимально распределяет остатки товаров запускаеться командой
```
python  plan_distribution.py
```
В результате создаеться и заполняеться таблица planned
