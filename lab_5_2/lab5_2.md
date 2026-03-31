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
