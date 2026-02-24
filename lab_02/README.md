# Лабораторная работа №2. Динамические соединения с базами данных. Вариант 13

**Цель работы.** Получить практические навыки создания сложного ETL-процесса, включающего динамическую загрузку файлов по HTTP, нормализацию базы данных, обработку дубликатов и настройку обработки ошибок с использованием Pentaho Data Integration (PDI).

---
## 1. Техническое обеспечение и окружение

Шаг был выполнен заранее
Используемые файлы:
- []()
- []()
- []()
---

## 2. Подготовка базы данных

Модернизирую представленный SQL-код и создаю таблицы с нужными мне значениями:

```sql
-- 1. Таблица заказов (фактов) - добавлены state, city, product_id
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    row_id INT PRIMARY KEY,
    order_date DATE,
    ship_date DATE,
    ship_mode VARCHAR(50),
    sales DECIMAL(10,2),
    quantity INT,
    discount DECIMAL(4,2),
    profit DECIMAL(10,2),
    returned TINYINT(1) DEFAULT 0, -- 1 = Yes, 0 = No
    state VARCHAR(100),
    city VARCHAR(100),
    product_id VARCHAR(100),
    INDEX idx_order_date (order_date),
    INDEX idx_ship_date (ship_date),
    INDEX idx_state (state),
    INDEX idx_city (city),
    INDEX idx_product_id (product_id)
);

-- 2. Таблица клиентов (измерение) - добавлен product_id
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    country VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    region VARCHAR(50),
    product_id VARCHAR(50), -- добавлен новый столбец
    INDEX idx_customer_id (customer_id),
    INDEX idx_region (region),
    INDEX idx_state_cust (state),
    INDEX idx_city_cust (city),
    INDEX idx_product_id_cust (product_id)
);

-- 3. Таблица продуктов (измерение) - добавлен state
DROP TABLE IF EXISTS products;
CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(20) NOT NULL,
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name VARCHAR(255),
    person VARCHAR(100),
    state VARCHAR(50), -- добавлен новый столбец
    INDEX idx_product_id (product_id),
    INDEX idx_category (category),
    INDEX idx_subcategory (sub_category),
    INDEX idx_state_prod (state)
);

-- 4. Настройка кодировки
ALTER DATABASE mgpu_ico_etl_13 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE customers CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE products CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

Результат:

<img width="1128" height="367" alt="image" src="https://github.com/user-attachments/assets/eada94ac-45ba-4df6-a4f2-530d10bb9a8d" />

---

## 3. Ход работы

### Шаг 1. Настройка Job (Главного задания)

Создаю Job, который управляет всем процессом.

1.  **Set Variables:** Создаю переменную пути к файлу.
    *   Variable: `CSV_FILE_PATH`
    *   Value: `/home/dba/Downloads/datain/samplestore-general.csv`.
2.  **Check File Exists:** Проверка наличия файла `${CSV_FILE_PATH}`.
3.  **HTTP (Download):** Загрузка файла, если его нет.
    *   **URL:** `https://raw.githubusercontent.com/BosenkoTM/workshop-on-ETL/main/data_for_lessons/samplestore-general.csv`
    *   **Target file:** `${CSV_FILE_PATH}`
4.  **Transformation.** Последовательный вызов трех трансформаций для загрузки данных.

HTTP:

<img width="1213" height="674" alt="image" src="https://github.com/user-attachments/assets/51a43f24-289c-4ce0-aec5-867e5d323ecf" />

Check File Exists:

<img width="446" height="178" alt="image" src="https://github.com/user-attachments/assets/fb2c94cb-c447-46b0-a32f-862663e3adeb" />

Готовая схема:

<img width="1021" height="471" alt="image" src="https://github.com/user-attachments/assets/6d00afdb-efbd-4d81-98c7-a9b69c6d8356" />

Можно увидеть, что Job успешно запустился.
### Шаг 2. Реализация Трансформаций (Transformations)

#### Трансформация 1. Load Orders
1.  **Select Values.** Установлены типы данных (Date format: `dd.MM.yyyy` для дат, Integer для ID).
2.  **Memory Group By.** Используется для дедупликации (группировка по `row_id`, взятие первых значений по остальным полям).
3.  **Filter Rows (Валидация):**
    *   Условие: `order_date IS NOT NULL` AND `ship_date IS NOT NULL` AND `state = 'Texas'`.
    *   TRUE -> **Table Output** (в таблицу `orders`).
    *   FALSE -> **Write to Log** (логирование ошибок).
4.  **Value Mapper.** Преобразование поля `Returned`: `Yes` -> `1`, `No` -> `0`, `Empty` -> `0`.

<img width="388" height="186" alt="image" src="https://github.com/user-attachments/assets/b2ea387d-020b-4435-b02f-0426861079ec" />


#### Трансформация 2. Load Customers
1.  **Select Values.** Оставьте только поля, относящиеся к клиенту (`customer_id`, `name`, `city` и т.д.).
2.  **Memory Group By.** Группировка по `customer_id` (устранение дублей клиентов).
3.   **Filter Rows (Валидация):**
    *   Условие:`state = 'Texas'`.
    *   TRUE -> **Table Output** (в таблицу `customers`).
    *   FALSE -> **Write to Log** (логирование ошибок).
4.  **Table Output.** Загрузка в таблицу `customers`.

<img width="726" height="289" alt="image" src="https://github.com/user-attachments/assets/67284b5d-a46d-486d-8190-bfcbd2c3641e" />

#### Трансформация 3. Load Products
1.  **Select Values.** Оставьте поля продукта (`product_id`, `category`, `name` и т.д.).
2.  **Memory Group By.** Группировка по `product_id`.
3.  **Filter Rows (Валидация):**
    *   Условие:`state = 'Texas'`.
    *   TRUE -> **Table Output** (в таблицу `products`).
    *   FALSE -> **Write to Log** (логирование ошибок).
4.  **Table Output.** Загрузка в таблицу `products`.

<img width="726" height="289" alt="image" src="https://github.com/user-attachments/assets/0d5b707b-c3b4-468e-a459-325a13f6c3a4" />

Результаты успешного выполнения трансформаций:

<img width="916" height="311" alt="image" src="https://github.com/user-attachments/assets/161398d0-75e3-4567-a8f4-230cc1ba1ae8" />

<img width="789" height="310" alt="image" src="https://github.com/user-attachments/assets/618b82d6-7feb-4405-961a-5e2c71c1815e" />

<img width="852" height="326" alt="image" src="https://github.com/user-attachments/assets/e4f703f7-800b-467d-9505-d3004553921d" />

Проверка загрузки данных в БД:

```
SELECT COUNT(*) as orders_count FROM orders;
SELECT COUNT(*) as customers_count FROM customers;
SELECT COUNT(*) as products_count FROM products;
```
Результаты:

orders:

<img width="392" height="177" alt="image" src="https://github.com/user-attachments/assets/05a39f99-3a05-49f8-9268-877a435f92d1" />

customers:

<img width="407" height="178" alt="image" src="https://github.com/user-attachments/assets/5e5eabdd-6a53-48b1-bbce-3383c7fdf950" />

products:

<img width="393" height="168" alt="image" src="https://github.com/user-attachments/assets/1be30b05-8f56-4c90-81e1-456d948cedc3" />

---

## 4. Варианты индивидуальных заданий. Вариант 13

| № | Основной фильтр для загрузки в БД | Доп. задание 1 (Аналитика) | Доп. задание 2 (Аналитика) |
|---|---|---|---|
| 13 | Штат: только Texas | Анализ возвратов | Отчет по доставке |

Выбор по штату уже был выполнен. Теперь необходимо провести аналитику

### 1. Анализа возвратов

- [Файл для анализа returns](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_02/returns_stats.ipynb)

#### Задача - вывести общую статистику по возвратам, возвраты по городам и по подкатегориям товаров

#### Результаты:

ОБЩАЯ СТАТИСТИКА ВОЗВРАТОВ (ПО ВСЕМ ДАННЫМ):
- Всего заказов: 1,129
- Всего возвратов: 48
- Процент возвратов: 4.25%
- Сумма возвращенных товаров: $11,721.25
- Количество возвращенных единиц: 194
- Общая прибыль: $-30,042.53
- Прибыль по возвратам: $-4,783.10

Статистика по городам:

<img width="1790" height="635" alt="image" src="https://github.com/user-attachments/assets/5e798463-229b-428c-bccf-0233e2906146" />

Статистика по подкатегориям товаров:

<img width="1189" height="590" alt="image" src="https://github.com/user-attachments/assets/3ae0bb28-64d3-41cd-83e2-cc50d01503ba" />

### 2. Отчет о доставке

- [Файл для анализа delievery](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_02/delivery__stats.ipynb)

#### Задача - вывести общий отчет по доставке, статистику по сегментам и по городам

#### Результаты:

ОБЩИЙ ОТЧЕТ ПО ДОСТАВКЕ
- Всего заказов с доставкой: 1129
- Среднее время доставки: 3.92 дней
- Минимальное время: 0 дней
- Максимальное время: 7 дней
- Среднее время: 4 дней
- Быстрая доставка (≤3 дня): 354 (31.4%)
- Нормальная доставка (4-7 дней): 775 (68.6%)
- Медленная доставка (>7 дней): 0 (0.0%)

Статистика по сегментам:

<img width="989" height="590" alt="image" src="https://github.com/user-attachments/assets/45ba4ab8-56c2-44ab-bee9-80405b24a7aa" />

Статистика по городам:

<img width="1189" height="1229" alt="image" src="https://github.com/user-attachments/assets/f1d4ad3e-bb8b-435b-86b8-f1e128bd4189" />

---

# Вывод

В ходе выполнения лабораторной работы был реализован полный цикл ETL-процесса и последующего анализа данных с использованием современных инструментов обработки данных: Pentaho Data Integration (Spoon) для загрузки и трансформации, MySQL для хранения данных и Python (Google Colab) для углубленного анализа и визуализации.
Были выявлены следующие факты:
- Всего заказов с доставкой: 1129
- Среднее время доставки: 3,92 дня
- Минимальное время: 0 дней, максимальное: 7 дней
- Медианное время: 4 дня
Распределение по скорости:
- Доставка ≤3 дней: 354 заказа (31,4%)
- Доставка 4-7 дней: 775 заказов (68,6%)
- Доставка >7 дней: 0 заказов (0%)

По сегментам клиентов:
- Consumer: 3,80 дня (403 заказа)
- Corporate: 4,01 дня (224 заказа)
- Home Office: 4,37 дня (117 заказов)

Города с самой долгой доставкой (топ-5):
- Port Arthur: 5,33 дня (6 заказов)
- Irving: 5,00 дня (9 заказов)
- Tyler: 5,00 дня (5 заказов)
- Grand Prairie: 4,93 дня (15 заказов)
- Round Rock: 4,71 дня (7 заказов)

Города с самой быстрой доставкой (топ-5):
- Bryan: 1,33 дня (9 заказов)
- Amarillo: 2,27 дня (15 заказов)
- Brownsville: 2,50 дня (8 заказов)
- Bedford: 2,60 дня (5 заказов)
- Beaumont: 2,83 дня (6 заказов)

Крупнейшие города по объему:
- Houston: 3,88 дня (438 заказов)
- Dallas: 4,08 дня (181 заказ)
- San Antonio: 4,22 дня (64 заказа)
