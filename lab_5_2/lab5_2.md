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
└── Dockerfile            
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

### 4. Просмотр аналитики (Streamlit)
Streamlit запускается автоматически в Docker-контейнере.
в раузере по адресу: `http://localhost:8501`

Здесь представлен отчет со статистикой запусков и галереей изображений:

<img width="1161" height="452" alt="image" src="https://github.com/user-attachments/assets/97480fcf-4421-4a75-8a30-336e6b5e0693" />

<img width="1192" height="425" alt="image" src="https://github.com/user-attachments/assets/109d8757-18ed-4454-8d1a-27e9b6628a81" />

<img width="1149" height="593" alt="image" src="https://github.com/user-attachments/assets/011d963f-c747-4bb4-9a9f-8313f3838f3d" />

---

## Вывод

В ходе выполнения лабораторной работы я закрепила навыки развертывания Apache Airflow в Docker-контейнерах. Был реализован ETL-процесс для генерации mock-данных о запусках ракет, включающий загрузку JSON-файла с информацией о запусках и сохранение изображений ракет в локальную папку. 

Были выполнены задачи:
- формирование отчёта со списком ракет и их изображений;
- загрузку данных из альтернативного mock-источника;
- анализ типов исключений (HTTP errors).
