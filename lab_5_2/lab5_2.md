# Лабораторная работа №5.2. Разработка алгоритмов для трансформации данных. Бизнес-кейс «Rocket». Вариант 13

**Цель работы:**
-  Закрепить навыки развертывания Apache Airflow в контейнеризированной среде (Docker).
-  Изучить работу с JSON-данными и бинарным контентом (изображениями) внутри ETL-процесса.
-  Научиться проектировать архитектуру ETL-решений и визуализировать её.
-  Автоматизировать выгрузку результатов работы DAG из контейнера в хост-систему.

| Вариант | Задание 1 (Анализ/ETL) | Задание 2 (Обработка/Логика) | Задание 3 (Отчетность/Метрики) |
|:---:|---|---|---|
| 13 | Отчет. Список ракет и их изображений | Загрузка с альтернативных источников (mock) | Анализ типов исключений (HTTP errors) |

---

## Диаграмма архитектуры

```mermaid
---
config:
  layout: elk
---
flowchart LR
    API((Mock Data Generator))

    subgraph Docker Containers[Контейнеры Docker]
        Airflow["⚙️ Airflow (DAG генерации данных)"]
        Streamlit["📊 Streamlit (Дашборд)"]
    end

    subgraph Ubuntu Host[Хост Ubuntu]
        Data[(📁 ./data)]
        Images[(📁 ./data/images)]
        Logs[(📁 ./logs)]
    end

    API == "1. Генерация mock-данных" ==> Airflow
    Airflow == "2. Сохраняет launches.json" ==> Data
    Airflow -. "Пишет логи" .-> Logs

    Data == "3. Читает launches.json" ==> Streamlit
    Images == "4. Читает изображения ракет" ==> Streamlit

    Streamlit == "5. Отображение таблиц, графиков и фото" ==> User((👤 Пользователь))

    style Data fill:#fff3e0,stroke:#f57c00
    style Images fill:#fff3e0,stroke:#f57c00
    style Logs fill:#fff3e0,stroke:#f57c00
    style Airflow fill:#ffe0b2,stroke:#fb8c00
    style Streamlit fill:#b3e5fc,stroke:#03a9f4
```

### Пояснение к архитектуре

Архитектура построена по принципу микросервисов с общим разделяемым хранилищем (Shared Volumes). Это позволяет сервисам быть независимыми, но при этом легко обмениваться файлами без сложной сетевой пересылки.

**Архитектура директорий:**
```text
.
├── app/                  # Скрипты Streamlit (app.py)
├── dags/                 # Airflow DAGs (download_rocket_launches.py)
├── data/                 # Локальная папка для JSON, фото
├── logs/                 # Логи Airflow (доступны напрямую из хоста)
├── docker-compose.yml
├── ml.ipynb              # ML Ноутбук для распознавания ракет
└── Dockerfile

```
---
## Проект

download_rocket_launches.py
```
import json
import pathlib
import random
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DATA_DIR = "/opt/airflow/data"
IMAGES_DIR = f"{DATA_DIR}/images"
JSON_FILE = f"{DATA_DIR}/launches.json"

dag = DAG(
    dag_id="download_rocket_launch",
    description="Вариант 13: Загрузка ракет (используем существующие изображения)",
    start_date=days_ago(1),
    schedule_interval=None,          # Запускаем вручную
    catchup=False,
    default_args={'owner': 'student', 'retries': 1},
)

# Убираем агрессивную очистку — только создаём папки
prepare_directories = BashOperator(
    task_id="prepare_directories",
    bash_command=f"mkdir -p {IMAGES_DIR}",
    dag=dag,
)

def create_launches_json():
    """Создаём launches.json на основе существующих изображений"""
    image_files = [f for f in os.listdir(IMAGES_DIR) if f.lower().endswith(('.jpg', '.jpeg', '.png'))]
    
    if not image_files:
        print("⚠️ В папке images нет изображений. Создаём минимальный mock.")
        image_files = ["placeholder.jpg"]

    launches = []
    rocket_providers = ["SpaceX", "Roscosmos", "ESA", "CNSA", "Rocket Lab", "ULA", "Blue Origin"]

    for i, img_name in enumerate(image_files[:12], 1):   # максимум 12 изображений
        provider = random.choice(rocket_providers)
        launches.append({
            "id": i,
            "name": f"Rocket Mission #{100 + i}",
            "launch_service_provider": {"name": provider},
            "window_start": datetime.utcnow().isoformat() + "Z",
            "status": {"name": random.choice(["Go", "Hold", "Success"])},
            "image_url": f"/images/{img_name}"
        })

    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump({"results": launches}, f, ensure_ascii=False, indent=2)

    print(f"✅ Создан launches.json на основе {len(image_files)} изображений")

create_json_task = PythonOperator(
    task_id="create_launches_json",
    python_callable=create_launches_json,
    dag=dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command=f'echo "DAG завершён. Изображений найдено: $(ls {IMAGES_DIR} | wc -l)"',
    dag=dag,
)

# Порядок
prepare_directories >> create_json_task >> notify
```
app.py
```
import streamlit as st
import pandas as pd
import json
import os
from PIL import Image, UnidentifiedImageError

st.set_page_config(page_title="Rocket Analytics - Вариант 13", layout="wide")

DATA_DIR = "/opt/airflow/data"
JSON_FILE = f"{DATA_DIR}/launches.json"

st.title("🚀 Вариант 13: Список ракет и их изображений")

# =========================
# КНОПКА ОБНОВЛЕНИЯ
# =========================
if st.button("🔄 Обновить данные"):
    st.rerun()

# =========================
# ЗАГРУЗКА ДАННЫХ
# =========================
@st.cache_data(ttl=2)
def load_data():
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"results": []}

data = load_data()
launches = data.get("results", [])

# =========================
# 1. ТАБЛИЦА
# =========================
st.header("1. Список ракет и запусков")

if launches:
    df = pd.DataFrame([{
        "Миссия": l.get("name"),
        "Провайдер": l.get("launch_service_provider", {}).get("name"),
        "Дата/Время": l.get("window_start"),
        "Статус": l.get("status", {}).get("name")
    } for l in launches])

    st.dataframe(df, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("По провайдерам")
        st.bar_chart(df["Провайдер"].value_counts())

    with col2:
        st.subheader("По статусам")
        st.bar_chart(df["Статус"].value_counts())

else:
    st.warning("Нет данных. Запусти DAG.")

st.markdown("---")

# =========================
# 2. ГАЛЕРЕЯ
# =========================
st.header("2. Галерея изображений")

loaded = 0
errors = 0

if launches:
    cols = st.columns(3)

    for idx, launch in enumerate(launches):
        img_path = f"{DATA_DIR}{launch['image_url']}"

        with cols[idx % 3]:
            try:
                img = Image.open(img_path)
                st.image(img, caption=launch["name"], width="stretch")
                loaded += 1

            except UnidentifiedImageError:
                st.warning(f"⚠️ Неподдерживаемый формат: {launch['image_url']}")
                errors += 1

            except FileNotFoundError:
                st.error(f"❌ Файл не найден: {launch['image_url']}")
                errors += 1

            except Exception as e:
                st.error(f"Ошибка: {str(e)}")
                errors += 1

    st.success(f"✅ Загружено изображений: {loaded}")
    if errors:
        st.error(f"❌ Ошибок загрузки: {errors}")

else:
    st.info("Нет данных для отображения")

st.markdown("---")

# =========================
# 3. АНАЛИЗ ОШИБОК
# =========================
st.header("3. Анализ ошибок (из логов Airflow)")

LOGS_DIR = "/opt/airflow/logs"
error_counts = {}

for root, dirs, files in os.walk(LOGS_DIR):
    for file in files:
        if file.endswith(".log"):
            try:
                with open(os.path.join(root, file), "r", encoding="utf-8") as f:
                    for line in f:
                        if "ERROR_TYPE" in line:
                            err = line.strip().split(":")[-1]
                            error_counts[err] = error_counts.get(err, 0) + 1
            except:
                pass

if error_counts:
    df_errors = pd.DataFrame(
        list(error_counts.items()),
        columns=["Тип ошибки", "Количество"]
    )
    st.dataframe(df_errors, use_container_width=True)
    st.bar_chart(df_errors.set_index("Тип ошибки"))
else:
    st.success("Ошибок не найдено 🎉")

st.markdown("---")


st.caption("Вариант 13 | Streamlit + Airflow + Docker")
```
docker-compose.yml
```
x-environment: &airflow_environment
  - AIRFLOW__CORE__EXECUTOR=LocalExecutor
  - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
  - AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
  - AIRFLOW__CORE__LOAD_EXAMPLES=False
  - AIRFLOW__CORE__STORE_DAG_CODE=True
  - AIRFLOW__CORE__STORE_SERIALIZED_DAGS=True
  - AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
  - AIRFLOW__WEBSERVER__RBAC=False
  - AIRFLOW__WEBSERVER__SECRET_KEY=supersecretkey123
  - AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
  - AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs
  - AIRFLOW__CORE__DEFAULT_TIMEZONE=utc

x-airflow-image: &airflow_image custom-airflow:slim-2.8.1-python3.11

services:
  postgres:
    image: postgres:12-alpine
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      timeout: 5s
      retries: 5

  init:
    image: *airflow_image
    depends_on:
      postgres:
        condition: service_healthy
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
      - ./logs:/opt/airflow/logs
    entrypoint: >
      bash -c "
      airflow db upgrade &&
      airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.org &&
      echo 'Airflow init completed.'"

  webserver:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    ports:
      - "8080:8080"
    restart: always
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
      - ./logs:/opt/airflow/logs
    command: webserver

  scheduler:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    restart: always
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
      - ./logs:/opt/airflow/logs
    command: scheduler

  # Новый сервис аналитики
  streamlit:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    ports:
      - "8501:8501"
    volumes:
      - ./data:/opt/airflow/data
      - ./app:/opt/airflow/app
    command: bash -c "streamlit run /opt/airflow/app/app.py --server.port=8501 --server.address=0.0.0.0"
  
  jupyter:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    ports:
      - "8888:8888"
    volumes:
      
      - ./:/opt/airflow/project
      - ./data:/opt/airflow/project/data
    working_dir: /opt/airflow/project
    command: bash -c "jupyter notebook --ip 0.0.0.0 --port 8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''"

volumes:
  postgres_data:
```

Dockerfile
```
FROM apache/airflow:slim-2.8.1-python3.11

USER root

RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/app \
    && chown -R 50000:0 /opt/airflow/data /opt/airflow/logs /opt/airflow/app

USER airflow

RUN pip install --no-cache-dir \
    pandas scikit-learn joblib requests streamlit \
    torch torchvision transformers Pillow plotly psycopg2-binary \
    jupyter
```

---

## Шаги по запуску окружения (Ubuntu 22.04)

### 1. Подготовка инфраструктуры

```bash
# Создание необходимых папок
mkdir -p dags data logs app

# Установка правильных прав доступа для Airflow (UID 50000)
# Это ВАЖНО, чтобы Airflow мог писать файлы в папки data и logs
sudo chown -R 50000:0 data logs
sudo chmod -R 775 data logs
```
Результат:

<img width="1066" height="342" alt="image" src="https://github.com/user-attachments/assets/d195e207-e003-4c35-9b63-a31e715582fe" />

### 2. Сборка и запуск Docker Compose
```bash
# Сборка кастомного образа с ML и Streamlit
sudo docker build -t custom-airflow:slim-2.8.1-python3.11 .
# Запуск инфраструктуры в фоновом режиме
sudo docker compose up -d
```
Результат:

<img width="811" height="330" alt="image" src="https://github.com/user-attachments/assets/9d5393cd-4fdf-4331-987f-1be5f01cade5" />

<img width="852" height="192" alt="image" src="https://github.com/user-attachments/assets/f3ebe09b-a455-4e6f-8847-ba1776539173" />

<img width="812" height="226" alt="image" src="https://github.com/user-attachments/assets/208a62f6-177a-421e-a767-961fea414126" />

### 3. Выполнение ETL (Airflow)

Открываю браузер по адресу: http://localhost:8080

Ввожу логин и пароль: admin / admin

Нахожу DAG download_rocket_launch, включаю его (Unpause) и запускаю (Trigger DAG)

Проверяю, что в папке ./data/images/ появились фотографии, а файл ./data/launches.json скачан

<img width="1158" height="425" alt="image" src="https://github.com/user-attachments/assets/42c04465-20fc-4259-929a-17f273bacbfb" />

<img width="1223" height="331" alt="image" src="https://github.com/user-attachments/assets/b5ed4807-c049-4b92-823c-aec1193cebd0" />

### 4. Выполнение ML-анализа (Jupyter in Docker)

<img width="1016" height="356" alt="image" src="https://github.com/user-attachments/assets/bdb9a8af-1416-481d-9b58-1b4544075f0d" />

<img width="1001" height="714" alt="image" src="https://github.com/user-attachments/assets/8df52521-10c4-4b63-842f-ae2138ced878" />


### 5. Просмотр аналитики (Streamlit)
Streamlit запускается автоматически в Docker-контейнере.
в раузере по адресу: `http://localhost:8501`

Здесь представлен отчет со статистикой запусков и галереей изображений:

<img width="1161" height="452" alt="image" src="https://github.com/user-attachments/assets/97480fcf-4421-4a75-8a30-336e6b5e0693" />

<img width="1192" height="425" alt="image" src="https://github.com/user-attachments/assets/109d8757-18ed-4454-8d1a-27e9b6628a81" />

<img width="1149" height="593" alt="image" src="https://github.com/user-attachments/assets/011d963f-c747-4bb4-9a9f-8313f3838f3d" />

<img width="1150" height="676" alt="image" src="https://github.com/user-attachments/assets/d3bbef6b-8ec0-4f91-b631-feb28e10dc91" />

<img width="1189" height="594" alt="image" src="https://github.com/user-attachments/assets/d55dd9b6-6acf-487d-91ce-0dba3b9abff4" />


---

## Вывод

В ходе выполнения лабораторной работы я закрепила навыки развертывания Apache Airflow в Docker-контейнерах. Был реализован ETL-процесс для генерации mock-данных о запусках ракет, включающий загрузку JSON-файла с информацией о запусках и сохранение изображений ракет в локальную папку. 

Были выполнены задачи:
- формирование отчёта со списком ракет и их изображений;
- загрузку данных из альтернативного mock-источника;
- анализ типов исключений (HTTP errors).
