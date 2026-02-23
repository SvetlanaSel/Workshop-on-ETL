# Лабораторная работа №2. Динамические соединения с базами данных. Вариант 13

**Цель работы.** Получить практические навыки создания сложного ETL-процесса, включающего динамическую загрузку файлов по HTTP, нормализацию базы данных, обработку дубликатов и настройку обработки ошибок с использованием Pentaho Data Integration (PDI).

---
## 1. Техническое обеспечение и окружение

Шаг был выполнен заранее

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

<img width="392" height="177" alt="image" src="https://github.com/user-attachments/assets/05a39f99-3a05-49f8-9268-877a435f92d1" />

<img width="407" height="178" alt="image" src="https://github.com/user-attachments/assets/5e5eabdd-6a53-48b1-bbce-3383c7fdf950" />

<img width="393" height="168" alt="image" src="https://github.com/user-attachments/assets/1be30b05-8f56-4c90-81e1-456d948cedc3" />

---

## 4. Варианты индивидуальных заданий. Вариант 13

| № | Основной фильтр для загрузки в БД | Доп. задание 1 (Аналитика) | Доп. задание 2 (Аналитика) |
|---|---|---|---|
| 13 | Штат: только Texas | Анализ возвратов | Отчет по доставке |

Выбор по штату уже был выполнен. Теперь необходимо провести аналитику
