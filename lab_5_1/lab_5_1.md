# Проектный практикум по разработке ETL-решений: Лабораторная работа №5. Вариант 13

## Постановка задачи (Вариант 20)
Разработать контейнеризированное ETL-решение на базе Apache Airflow для автоматизации пайплайна обработки данных со следующими требованиями:

| Вариант | Задание 1 (Сбор данных) | Задание 2 (Трансформация) | Задание 3 (Сохранение/Визуализация) |
|---|---|---|---|
| 13 | Прогноз: Прага, 3 дня | Столбец: дельта с пред. днём | Таблица изменений |

---

## Архитектура проекта

```mermaid
graph TD
    subgraph External Sources
        API[Open-Meteo API]
    end

    subgraph Docker Infrastructure
        PG[(PostgreSQL<br/>Метаданные Airflow)]
        
        subgraph Airflow Components
            SCH[Scheduler]
            WEB[Webserver :8080]
            INIT[Init-container]
        end
        
        subgraph Visualization
            STR[Streamlit App :8501]
        end
        
        VOL[Shared Volume<br/>/opt/airflow/data]
    end

    subgraph User
        BROWSER((Browser))
    end

    API -->|Получение прогноза| SCH
    SCH -->|Сохранение CSV| VOL
    SCH <-->|Чтение/Запись состояния| PG
    WEB <-->|Чтение состояния| PG
    STR -->|Чтение CSV| VOL
    
    BROWSER -->|Мониторинг DAG| WEB
    BROWSER -->|Просмотр дашборда| STR
```

## Технический стек
* **Оркестрация:** Apache Airflow 2.8.1
* **Контейнеризация:** Docker, Docker Compose
* **Язык программирования:** Python 3.11
* **Библиотеки (ETL & ML):** Pandas, Scikit-learn, Joblib, Requests
* **Визуализация:** Streamlit, Matplotlib
* **База данных:** PostgreSQL 12 (для метаданных Airflow)

## Описание DAG (`real_umbrella_prague`)
Пайплайн состоит из следующих задач (Task):
1. **`fetch_weather_forecast`**: Обращается к Open-Meteo API, получает прогноз для **Праги** на 3 дня, сохраняет в `weather_forecast.csv`.
2. **`clean_weather_data`**: Заполняет пропуски, вычисляет и **добавляет столбец "дельта с предыдущим днем"** (разница температур между текущим и предыдущим днем), сохраняет в `clean_weather.csv`.
3. **`fetch_sales_data`**: Читает даты из прогноза погоды и генерирует моковые данные о продажах на эти же даты (чтобы `join` отработал корректно), сохраняет в `sales_data.csv`.
4. **`clean_sales_data`**: Очищает данные продаж методом forward fill, сохраняет в `clean_sales.csv`.
5. **`join_datasets`**: Объединяет данные о погоде и продажах по дате (Inner Join), сохраняет в `joined_data.csv`.
6. **`create_changes_table`**: Создает **таблицу изменений** на основе объединенных данных, добавляет столбцы: тип изменения температуры (increase/decrease/no_change), абсолютное значение дельты, изменение продаж и его тип, сохраняет в `changes_table.csv`.
7. **`train_ml_model`**: Обучает модель линейной регрессии для предсказания продаж на основе двух признаков: температуры и дельты температуры, сохраняет модель в `ml_model.pkl`.
8. **`deploy_ml_model`**: Имитирует деплой (загружает сохраненную `.pkl` модель) и демонстрирует предсказания на основе таблицы изменений.

## Структура
```
project/
├── dags/
│   └── real_umbrella.py
├── app/
│   └── app.py
├── data/
│   ├── weather_forecast.csv
│   ├── clean_weather.csv
│   ├── sales_data.csv
│   ├── clean_sales.csv
│   ├── joined_data.csv
│   ├── changes_table.csv
│   └── ml_model.pkl
├── docker-compose.yml
└── Dockerfile
```
---

### 1. `Dockerfile`
Добавлен `streamlit` и `matplotlib` для графиков.
```dockerfile
FROM apache/airflow:slim-2.8.1-python3.11

USER airflow

# Устанавливаем необходимые Python-библиотеки
RUN pip install --no-cache-dir \
    pandas \
    scikit-learn \
    joblib \
    requests \
    azure-storage-blob==12.8.1 \
    psycopg2-binary \
    streamlit \
    matplotlib \
    "connexion[swagger-ui]"

USER root

# Создаём директории и назначаем владельца
RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/app \
    && chown -R airflow: /opt/airflow/data /opt/airflow/logs /opt/airflow/app

USER airflow
```

### 2. `docker-compose.yml`
Добавлен сервис `streamlit` для визуализации графиков.
```yaml
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
  - AIRFLOW__LOGGING__REMOTE_LOGGING=False
  - AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs

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
      - logs:/opt/airflow/logs
    entrypoint: >
      bash -c "
      airflow db upgrade &&
      airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.org &&
      echo 'Airflow init completed.'"
    healthcheck:
      test: ["CMD", "airflow", "db", "check"]
      interval: 10s
      retries: 5
      start_period: 10s

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
      - logs:/opt/airflow/logs
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
      - logs:/opt/airflow/logs
    command: scheduler

  streamlit:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    ports:
      - "8501:8501"
    restart: always
    volumes:
      - ./data:/opt/airflow/data
      - ./app:/opt/airflow/app
    command: bash -c "streamlit run /opt/airflow/app/app.py --server.port=8501 --server.address=0.0.0.0"

volumes:
  logs:
  postgres_data:
```

### 3. `dags/real_umbrella.py`
Скорректирован для Праги, добавления дельты дней и генерации корректных дат для join, сохранение таблицы измерений.
```
import os
import requests
import pandas as pd
import joblib
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sklearn.linear_model import LinearRegression

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id="real_umbrella_prague",
    default_args=default_args,
    description="Fetch Prague weather, clean, add delta, create changes table, train ML, deploy.",
    schedule_interval="@daily",
    catchup=False
)

def fetch_weather_forecast():
    # Открытый API Open-Meteo (Прага, прогноз на 3 дня, средняя температура)
    url = (
        "https://api.open-meteo.com/v1/forecast?"
        "latitude=50.0880&longitude=14.4208"
        "&daily=temperature_2m_mean"
        "&timezone=Europe%2FPrague"
        "&forecast_days=3"
    )
    
    response = requests.get(url)
    data = response.json()
    
    # Извлекаем списки дат и температур из JSON
    dates = data['daily']['time']
    temperatures = data['daily']['temperature_2m_mean']
    
    # Создаем DataFrame (имена колонок такие же, чтобы остальные функции работали)
    df = pd.DataFrame({
        'date': dates,
        'temperature': temperatures
    })
    
    data_dir = '/opt/airflow/data'
    os.makedirs(data_dir, exist_ok=True)
    df.to_csv(os.path.join(data_dir, 'weather_forecast.csv'), index=False)
    print("Weather forecast for Prague saved via Open-Meteo (3 days).")

def clean_weather_data():
    data_dir = '/opt/airflow/data'
    df = pd.read_csv(os.path.join(data_dir, 'weather_forecast.csv'))
    
    # Очистка
    df['temperature'] = df['temperature'].ffill()
    
    # Добавление столбца "дельта с предыдущим днем"
    df['delta_temp'] = df['temperature'].diff()
    
    # Заполняем первое значение дельты (NaN) нулем
    df['delta_temp'] = df['delta_temp'].fillna(0)
    
    df.to_csv(os.path.join(data_dir, 'clean_weather.csv'), index=False)
    print("Cleaned weather data with 'delta_temp' saved.")

def fetch_sales_data():
    data_dir = '/opt/airflow/data'
    # Чтобы join сработал, берём даты из прогноза
    weather_df = pd.read_csv(os.path.join(data_dir, 'weather_forecast.csv'))
    dates = weather_df['date'].tolist()
    
    # Моковые данные продаж
    sales = [10, 15, 20][:len(dates)]  # 3 дня
    
    df = pd.DataFrame({'date': dates, 'sales': sales})
    df.to_csv(os.path.join(data_dir, 'sales_data.csv'), index=False)
    print("Sales data saved.")

def clean_sales_data():
    data_dir = '/opt/airflow/data'
    df = pd.read_csv(os.path.join(data_dir, 'sales_data.csv'))
    df['sales'] = df['sales'].ffill()
    df.to_csv(os.path.join(data_dir, 'clean_sales.csv'), index=False)
    print("Cleaned sales data saved.")

def join_datasets():
    data_dir = '/opt/airflow/data'
    weather_df = pd.read_csv(os.path.join(data_dir, 'clean_weather.csv'))
    sales_df = pd.read_csv(os.path.join(data_dir, 'clean_sales.csv'))
    
    # Объединение
    joined_df = pd.merge(weather_df, sales_df, on='date', how='inner')
    joined_df.to_csv(os.path.join(data_dir, 'joined_data.csv'), index=False)
    print("Joined dataset saved.")

def create_changes_table():
    data_dir = '/opt/airflow/data'
    df = pd.read_csv(os.path.join(data_dir, 'joined_data.csv'))
    
    # Создаем таблицу изменений
    changes_df = df[['date', 'temperature', 'delta_temp', 'sales']].copy()
    
    # Добавляем столбец с типом изменения температуры
    changes_df['temp_change_type'] = changes_df['delta_temp'].apply(
        lambda x: 'increase' if x > 0 else ('decrease' if x < 0 else 'no_change')
    )
    
    # Добавляем столбец с абсолютным значением изменения температуры
    changes_df['abs_delta_temp'] = changes_df['delta_temp'].abs()
    
    # Добавляем столбец с изменением продаж относительно предыдущего дня
    changes_df['sales_change'] = changes_df['sales'].diff().fillna(0)
    changes_df['sales_change_type'] = changes_df['sales_change'].apply(
        lambda x: 'increase' if x > 0 else ('decrease' if x < 0 else 'no_change')
    )
    
    # Сохраняем таблицу изменений
    changes_df.to_csv(os.path.join(data_dir, 'changes_table.csv'), index=False)
    print("Changes table created and saved.")
    
    # Выводим таблицу изменений для визуализации в логах
    print("\n=== TABLE OF CHANGES ===")
    print(changes_df.to_string(index=False))
    print("========================")

def train_ml_model():
    data_dir = '/opt/airflow/data'
    df = pd.read_csv(os.path.join(data_dir, 'joined_data.csv'))
    
    X = df[['temperature', 'delta_temp']]  # Используем оба признака
    y = df['sales']
    
    model = LinearRegression()
    model.fit(X, y)
    
    joblib.dump(model, os.path.join(data_dir, 'ml_model.pkl'))
    print("ML model trained and saved.")
    
    # Выводим коэффициенты модели
    print(f"Model coefficients: temperature={model.coef_[0]:.2f}, delta_temp={model.coef_[1]:.2f}")
    print(f"Model intercept: {model.intercept_:.2f}")

def deploy_ml_model():
    data_dir = '/opt/airflow/data'
    model = joblib.load(os.path.join(data_dir, 'ml_model.pkl'))
    
    # Загружаем таблицу изменений для демонстрации
    changes_df = pd.read_csv(os.path.join(data_dir, 'changes_table.csv'))
    
    print("Model deployed successfully:", model)
    print("\n=== MODEL PREDICTIONS BASED ON CHANGES TABLE ===")
    
    # Делаем предсказания на основе данных из таблицы изменений
    X_new = changes_df[['temperature', 'delta_temp']]
    predictions = model.predict(X_new)
    
    for i, row in changes_df.iterrows():
        print(f"Date: {row['date']}, Temperature: {row['temperature']}°C, "
              f"Delta: {row['delta_temp']}°C, Predicted Sales: {predictions[i]:.2f}, "
              f"Actual Sales: {row['sales']}")

# Инициализация операторов
t1 = PythonOperator(task_id="fetch_weather_forecast", python_callable=fetch_weather_forecast, dag=dag)
t2 = PythonOperator(task_id="clean_weather_data", python_callable=clean_weather_data, dag=dag)
t3 = PythonOperator(task_id="fetch_sales_data", python_callable=fetch_sales_data, dag=dag)
t4 = PythonOperator(task_id="clean_sales_data", python_callable=clean_sales_data, dag=dag)
t5 = PythonOperator(task_id="join_datasets", python_callable=join_datasets, dag=dag)
t6 = PythonOperator(task_id="create_changes_table", python_callable=create_changes_table, dag=dag)
t7 = PythonOperator(task_id="train_ml_model", python_callable=train_ml_model, dag=dag)
t8 = PythonOperator(task_id="deploy_ml_model", python_callable=deploy_ml_model, dag=dag)

# Настройка зависимостей (граф)
t1 >> t2
t3 >> t4
[t2, t4] >> t5
t5 >> t6 >> t7 >> t8
```
### 4. `app/app.py` (Streamlit Дашборд)
```
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os

st.set_page_config(page_title="Прогноз погоды Прага", layout="wide")
st.title("Анализ погоды в Праге на 3 дня (Вариант 13)")

# Загружаем таблицу изменений
changes_path = '/opt/airflow/data/changes_table.csv'
weather_path = '/opt/airflow/data/clean_weather.csv'

if os.path.exists(changes_path) and os.path.exists(weather_path):
    changes_df = pd.read_csv(changes_path)
    weather_df = pd.read_csv(weather_path)
    
    st.write("### Таблица изменений")
    st.write("Данные с изменениями температуры и продаж по дням")
    
    # Переименовываем колонки для удобства отображения
    display_df = changes_df.rename(columns={
        'date': 'Дата',
        'temperature': 'Температура (°C)',
        'delta_temp': 'Дельта температуры (°C)',
        'sales': 'Продажи',
        'temp_change_type': 'Изменение температуры',
        'abs_delta_temp': 'Абсолютное изменение температуры',
        'sales_change': 'Изменение продаж',
        'sales_change_type': 'Изменение продаж (тип)'
    })
    
    st.dataframe(display_df, use_container_width=True)
    
    st.write("### Графики: Температура и изменения")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig1, ax1 = plt.subplots(figsize=(8, 5))
        ax1.plot(weather_df['date'], weather_df['temperature'], marker='o', color='orange', linewidth=2)
        ax1.set_xlabel('Дата')
        ax1.set_ylabel('Температура (°C)')
        ax1.set_title('Изменение температуры по дням')
        ax1.grid(True, linestyle='--', alpha=0.7)
        plt.xticks(rotation=45)
        st.pyplot(fig1)
    
    with col2:
        fig2, ax2 = plt.subplots(figsize=(8, 5))
        colors = ['red' if x < 0 else 'green' if x > 0 else 'blue' for x in changes_df['delta_temp']]
        bars = ax2.bar(changes_df['date'], changes_df['delta_temp'], color=colors, alpha=0.7)
        ax2.set_xlabel('Дата')
        ax2.set_ylabel('Дельта температуры (°C)')
        ax2.set_title('Изменение температуры относительно предыдущего дня')
        ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        ax2.grid(True, linestyle='--', alpha=0.3)
        plt.xticks(rotation=45)
        
        for bar, delta in zip(bars, changes_df['delta_temp']):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{delta:.1f}°C', ha='center', va='bottom' if height > 0 else 'top')
        
        st.pyplot(fig2)
    
    st.write("### Статистика изменений")
    
    col3, col4, col5 = st.columns(3)
    with col3:
        st.metric("Максимальное повышение температуры", f"{changes_df['delta_temp'].max():.1f}°C")
    with col4:
        st.metric("Максимальное понижение температуры", f"{changes_df['delta_temp'].min():.1f}°C")
    with col5:
        st.metric("Среднее абсолютное изменение", f"{changes_df['delta_temp'].abs().mean():.1f}°C")
    
    # Дополнительная статистика по продажам
    st.write("### Статистика продаж")
    col6, col7, col8 = st.columns(3)
    with col6:
        st.metric("Максимальные продажи", f"{changes_df['sales'].max()}")
    with col7:
        st.metric("Минимальные продажи", f"{changes_df['sales'].min()}")
    with col8:
        st.metric("Средние продажи", f"{changes_df['sales'].mean():.1f}")
    
    # Информация о модели ML
    model_path = '/opt/airflow/data/ml_model.pkl'
    if os.path.exists(model_path):
        st.write("### Информация о ML модели")
        st.info("✅ Модель машинного обучения обучена и готова к использованию")
        
        # Показываем последние предсказания если есть
        if 'sales' in changes_df.columns and 'temperature' in changes_df.columns:
            st.write("**Последние данные для предсказания:**")
            last_row = changes_df.iloc[-1]
            st.write(f"- Дата: {last_row['date']}")
            st.write(f"- Температура: {last_row['temperature']}°C")
            st.write(f"- Дельта температуры: {last_row['delta_temp']}°C")
            st.write(f"- Фактические продажи: {last_row['sales']}")
    
else:
    st.warning("Данные еще не сгенерированы. Пожалуйста, запустите DAG в Airflow.")
    st.write("Ожидаемые файлы:")
    st.write("- /opt/airflow/data/changes_table.csv")
    st.write("- /opt/airflow/data/clean_weather.csv")
```
---
# Запуск

Для того, чтобы у Airflow были права создать файл в папке data, меняем владельца этой папки и всех файлов внутри на пользователя с UID 50000 и группу root. Затем меняем владельца всей папки проекта и всех файлов внутри на пользователя dev:

<img width="849" height="126" alt="image" src="https://github.com/user-attachments/assets/72804e4e-21d4-4e99-951c-8bf3acb08e38" />

Создание образа:

<img width="826" height="490" alt="image" src="https://github.com/user-attachments/assets/08dfe288-483b-48b2-b59d-8766934ff142" />

<img width="826" height="160" alt="image" src="https://github.com/user-attachments/assets/7ad0fe50-5227-4fb0-849d-afc1e4e830ac" />

Запуск:

<img width="831" height="229" alt="image" src="https://github.com/user-attachments/assets/b731f52e-87bb-407e-8b41-fae3fd6de35f" />

Проверка:

<img width="815" height="257" alt="image" src="https://github.com/user-attachments/assets/42379b4e-1713-4246-8d98-c258fc190dd3" />

Логи:

<img width="826" height="512" alt="image" src="https://github.com/user-attachments/assets/b8175bf1-1ac0-434a-9df4-5ab5151c946c" />

<img width="834" height="590" alt="image" src="https://github.com/user-attachments/assets/651b3479-b254-435e-bb8a-7b665a54f189" />

Переход в Airflow:

<img width="984" height="500" alt="image" src="https://github.com/user-attachments/assets/9492cf14-6596-4534-8fc8-09026d76ef8d" />

<img width="1204" height="474" alt="image" src="https://github.com/user-attachments/assets/504d6dfd-1f0e-42a4-94b9-48d05f4cf4c5" />

Запуск ДАГ:

<img width="787" height="520" alt="image" src="https://github.com/user-attachments/assets/f3c47444-73aa-469e-9cec-1f78f031011c" />

<img width="849" height="147" alt="image" src="https://github.com/user-attachments/assets/e47f5081-108d-48d8-999b-75cc5eac83c1" />

<img width="988" height="331" alt="image" src="https://github.com/user-attachments/assets/3e045b61-82c1-4ebf-8cf2-2b77b6b25222" />

Дашборд:

<img width="1153" height="310" alt="image" src="https://github.com/user-attachments/assets/a29031a2-952a-4e48-9c7b-04b99ebd7e7a" />

<img width="1160" height="456" alt="image" src="https://github.com/user-attachments/assets/0b51c9c8-1f12-443e-b68e-7ed7cd32dd91" />

<img width="1135" height="546" alt="image" src="https://github.com/user-attachments/assets/8cdffe16-74fc-46b6-b03a-582f8b5d52a5" />

Предсказания:

<img width="1051" height="524" alt="image" src="https://github.com/user-attachments/assets/21f14d03-579b-4708-abb4-966963f6de83" />

---
# Выводы:
В ходе выполнения лабораторной работы были выполнены следующие задачи:

1. **Развернута среда Apache Airflow 2.8.1** с использованием Docker. Настроены все необходимые сервисы: PostgreSQL для хранения метаданных, веб-сервер для мониторинга, планировщик для выполнения задач, а также Streamlit для визуализации результатов.

2. **Изучена структура DAG** в Airflow. Реализован ETL-конвейер из 8 задач с зависимостями: сбор данных о погоде → очистка с добавлением дельты → генерация данных о продажах → очистка продаж → объединение датасетов → создание таблицы изменений → обучение ML-модели → развертывание модели в рабочую среду.

3. **Спроектирована архитектура** решения: сбор данных (Open-Meteo API для Праги на 3 дня), трансформация (очистка, добавление столбца "дельта с предыдущим днем"), ML-модель (линейная регрессия), таблица изменений (фиксация изменений температуры и продаж), визуализация (Streamlit), оркестрация (Airflow).

4. **Реализован ETL-процесс**: получение прогноза погоды для Праги через Open-Meteo API на 3 дня, очистка данных, вычисление дельты температуры, генерация моковых данных о продажах, объединение датасетов по дате, создание таблицы изменений с типами изменений и абсолютными значениями.

5. **Обучена ML-модель** для прогнозирования продаж на основе температуры. Модель линейной регрессии сохранена в формате `.pkl`.

6. **Создана таблица изменений** (`changes_table.csv`), которая содержит информацию о динамике температуры и продаж, включая типы изменений (повышение/понижение/без изменений) и абсолютные значения дельт.

7. **Разработано Streamlit-приложение** для визуализации результатов: отображение таблицы изменений, графики температуры и дельты, статистика изменений, информация об обученной ML-модели.

8. **Выявлены и устранены проблемы** с зависимостями между задачами в DAG, обеспечив корректный порядок выполнения: задача генерации продаж теперь выполняется только после получения прогноза погоды.


