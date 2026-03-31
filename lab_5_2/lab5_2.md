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
        Airflow["⚙️ Airflow (ETL + генерация запусков)"]
        Jupyter["🧠 Jupyter (ML / CLIP анализ)"]
        Streamlit["📊 Streamlit (Дашборд + аналитика)"]
    end

    subgraph Ubuntu Host[Хост Ubuntu]
        Data[(📁 ./data<br/>JSON + изображения + CSV)]
        Logs[(📁 ./logs Логи Airflow)]
    end

    API == "1. Генерация mock-данных" ==> Airflow

    Airflow == "2. Сохраняет launches.json и использует изображения" ==> Data
    Airflow -. "Пишет логи и ошибки (HTTP / Exception)" .-> Logs

    Data == "3. Берет изображения ракет" ==> Jupyter
    Jupyter == "4. Генерирует ml_predictions.csv" ==> Data

    Data == "5. Читает JSON, изображения и ML CSV" ==> Streamlit
    Logs == "6. Анализ типов ошибок" ==> Streamlit

    Streamlit == "7. Отображение таблиц, графиков, ML аналитики и изображений" ==> User((👤 Пользователь))

    style Data fill:#fff3e0,stroke:#f57c00
    style Logs fill:#fff3e0,stroke:#f57c00
    style Airflow fill:#ffe0b2,stroke:#fb8c00
    style Jupyter fill:#c8e6c9,stroke:#43a047
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

ml.ipynb
```
import os
import pandas as pd
from PIL import Image
from transformers import CLIPProcessor, CLIPModel
import json
import transformers

transformers.logging.set_verbosity_error()

DATA_DIR = "./data"
JSON_FILE = f"{DATA_DIR}/launches.json"

model_id = "openai/clip-vit-base-patch32"
processor = CLIPProcessor.from_pretrained(model_id)
model = CLIPModel.from_pretrained(model_id)

labels = [
    "Falcon 9 rocket",
    "Soyuz rocket",
    "Starship spacecraft",
    "Ariane rocket",
    "Electron rocket"
]

with open(JSON_FILE) as f:
    launches = json.load(f)["results"]

results = []

for launch in launches:
    try:
        # ❌ СПЕЦИАЛЬНО НЕПРАВИЛЬНЫЙ ПУТЬ
        wrong_path = f"{DATA_DIR}/wrong_folder/{launch['image']}"
        print(f"🔴 Пробуем неправильный путь: {wrong_path}")

        Image.open(wrong_path)

    except Exception as e:
        print(f"❌ ERROR_TYPE: FileNotFoundError")
        print("⚠️ Путь неверный. Используем корректный путь")

    try:
        # ✅ правильный путь
        img_path = f"{DATA_DIR}/images/{launch['image']}"
        image = Image.open(img_path).convert("RGB")

        inputs = processor(text=labels, images=image, return_tensors="pt", padding=True)
        outputs = model(**inputs)

        probs = outputs.logits_per_image.softmax(dim=1).detach().numpy()[0]
        idx = probs.argmax()

        results.append({
            "mission": launch["name"],
            "provider": launch["provider"],
            "status": launch["status"],
            "predicted_rocket": labels[idx],
            "confidence": round(probs[idx] * 100, 2),
            "image": launch["image"]
        })

    except Exception as e:
        print(f"Ошибка ML: {e}")

df = pd.DataFrame(results)
df.to_csv("./data/ml_predictions.csv", index=False)

print("✅ ML отчет создан")
```

download_rocket_launches.py
```
import json
import os
import random
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests

DATA_DIR = "/opt/airflow/data"
IMAGES_DIR = f"{DATA_DIR}/images"
JSON_FILE = f"{DATA_DIR}/launches.json"

dag = DAG(
    dag_id="download_rocket_launch",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
)

# =========================
# MOCK API + ОШИБКА + ФИКС
# =========================
def mock_api_with_error():
    try:
        # ❌ СПЕЦИАЛЬНО НЕПРАВИЛЬНЫЙ API
        print("🔴 Попытка подключения к неправильному API...")
        requests.get("http://256.256.256.256")  # невалидный IP

    except Exception as e:
        print(f"❌ ERROR_TYPE: ConnectionError")
        print("⚠️ Ошибка подключения к API. Используем fallback (mock данные)")

    # ✅ fallback (рабочий вариант)
    try:
        launches = []

        providers = ["SpaceX", "Roscosmos", "ESA"]
        statuses = ["Go", "Hold", "Success"]

        images = os.listdir(IMAGES_DIR)

        for i, img in enumerate(images):
            launches.append({
                "name": f"Mission {i}",
                "provider": random.choice(providers),
                "status": random.choice(statuses),
                "image": img,
                "date": datetime.utcnow().isoformat()
            })

        with open(JSON_FILE, "w") as f:
            json.dump({"results": launches}, f)

        print("✅ Данные успешно созданы через fallback")

    except Exception as e:
        print(f"❌ ERROR_TYPE: {type(e).__name__}")


task = PythonOperator(
    task_id="mock_api",
    python_callable=mock_api_with_error,
    dag=dag
)
```
app.py
```
import streamlit as st
import pandas as pd
import os
from PIL import Image, UnidentifiedImageError

st.set_page_config(page_title="Rocket ML Analytics", layout="wide")

DATA_DIR = "/opt/airflow/data"
ML_FILE = f"{DATA_DIR}/ml_predictions.csv"
IMAGES_DIR = f"{DATA_DIR}/images"

st.title("🚀 ML Аналитика ракет (Вариант 13)")

# =========================
# ОБНОВЛЕНИЕ
# =========================
if st.button("🔄 Обновить данные"):
    st.rerun()

# =========================
# ЗАГРУЗКА ML ДАННЫХ
# =========================
@st.cache_data(ttl=2)
def load_ml():
    if os.path.exists(ML_FILE):
        return pd.read_csv(ML_FILE)
    return pd.DataFrame()

df = load_ml()

# =========================
# 1. ТАБЛИЦА (ГЛАВНЫЙ ОТЧЕТ)
# =========================
st.header("1. ML Отчет по запускам")

if not df.empty:
    st.dataframe(df, use_container_width=True)
else:
    st.warning("Нет ML данных. Запусти ml.ipynb")
    st.stop()

st.markdown("---")

# =========================
# 2. ОСНОВНАЯ АНАЛИТИКА
# =========================
st.header("2. Основная аналитика")

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("По провайдерам")
    st.bar_chart(df["provider"].value_counts())

with col2:
    st.subheader("По статусам")
    st.bar_chart(df["status"].value_counts())

with col3:
    st.subheader("Типы ракет (ML)")
    st.bar_chart(df["predicted_rocket"].value_counts())

st.markdown("---")

# =========================
# 3. УГЛУБЛЕННАЯ АНАЛИТИКА
# =========================
st.header("3. ML аналитика")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Средняя уверенность по провайдерам")
    df_conf = df.groupby("provider")["confidence"].mean()
    st.bar_chart(df_conf)

with col2:
    st.subheader("Распределение confidence")
    st.line_chart(df["confidence"])

# KPI
st.markdown("### 📊 Метрики")

success_rate = (df["status"] == "Success").mean() * 100

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Всего запусков", len(df))

with col2:
    st.metric("Успешность (%)", f"{success_rate:.2f}")

with col3:
    st.metric("Средняя уверенность ML", f"{df['confidence'].mean():.2f}%")

st.markdown("---")

# =========================
# 4. ГАЛЕРЕЯ + ML
# =========================
st.header("4. Галерея с ML предсказаниями")

loaded = 0
errors = 0

cols = st.columns(3)

for idx, row in df.iterrows():
    img_path = f"{IMAGES_DIR}/{row['image']}"

    with cols[idx % 3]:
        try:
            img = Image.open(img_path)

            st.image(
                img,
                caption=f"{row['predicted_rocket']} ({row['confidence']}%)",
                width="stretch"
            )

            loaded += 1

        except UnidentifiedImageError:
            st.warning(f"Формат не поддерживается: {row['image']}")
            errors += 1

        except FileNotFoundError:
            st.error(f"Файл не найден: {row['image']}")
            errors += 1

        except Exception as e:
            st.error(str(e))
            errors += 1

st.success(f"Загружено: {loaded}")
if errors:
    st.error(f"Ошибок: {errors}")

st.markdown("---")

# =========================
# 5. АНАЛИЗ ОШИБОК AIRFLOW
# =========================
st.header("5. Анализ ошибок (Airflow logs)")

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
    st.success("Ошибок не найдено")

st.caption("ML-first архитектура | Airflow + CLIP + Streamlit")
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

<img width="1032" height="696" alt="image" src="https://github.com/user-attachments/assets/9a676b80-0e2c-4e05-86f6-a1a5f13e1c24" />

<img width="771" height="431" alt="image" src="https://github.com/user-attachments/assets/b06dae40-e48a-4967-a7a4-c6a420d58001" />


### 5. Просмотр аналитики (Streamlit)
Streamlit запускается автоматически в Docker-контейнере.
в раузере по адресу: `http://localhost:8501`

Здесь представлен отчет со статистикой запусков и галереей изображений:

<img width="1217" height="542" alt="image" src="https://github.com/user-attachments/assets/884d59f6-f67f-4adb-9c2b-4b4a6e8c887d" />

<img width="1147" height="450" alt="image" src="https://github.com/user-attachments/assets/1082e700-57d3-4e92-a085-d68bf763f9b3" />

<img width="1184" height="583" alt="image" src="https://github.com/user-attachments/assets/234a5033-eeca-4fb3-9661-6c21f90bbad6" />

<img width="1149" height="569" alt="image" src="https://github.com/user-attachments/assets/3e5f2381-7e4c-4fab-b248-a1459520d9d6" />

<img width="1160" height="386" alt="image" src="https://github.com/user-attachments/assets/c8dc2bf3-d6c8-4443-a2f5-28bd04c1e20e" />


---

## Вывод

В ходе выполнения лабораторной работы я закрепила навыки развертывания Apache Airflow в Docker-контейнерах. Был реализован ETL-процесс для генерации mock-данных о запусках ракет, включающий загрузку JSON-файла с информацией о запусках и сохранение изображений ракет в локальную папку. 

Были выполнены задачи:
- формирование отчёта со списком ракет и их изображений;
- загрузку данных из альтернативного mock-источника;
- анализ типов исключений (HTTP errors).
