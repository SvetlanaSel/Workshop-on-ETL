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

-- 2. Таблица клиентов (измерение) - уже есть city и state, добавлен product_id
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

Шаг был выполнен заранее

### Шаг 2. Реализация Трансформаций (Transformations)

#### Трансформация 1. Load Orders
1.  **Select Values.** Установлены типы данных (Date format: `dd.MM.yyyy` для дат, Integer для ID).
2.  **Memory Group By.** Используется для дедупликации (группировка по `row_id`, взятие первых значений по остальным полям).
3.  **Filter Rows (Валидация):**
    *   Условие: `order_date IS NOT NULL` AND `ship_date IS NOT NULL` AND `state = 'Texas'`.
    *   TRUE -> **Table Output** (в таблицу `orders`).
    *   FALSE -> **Write to Log** (логирование ошибок).
4.  **Value Mapper.** Преобразование поля `Returned`: `Yes` -> `1`, `No` -> `0`, `Empty` -> `0`.

#### Трансформация 2. Load Customers
1.  **Select Values.** Оставьте только поля, относящиеся к клиенту (`customer_id`, `name`, `city` и т.д.).
2.  **Memory Group By.** Группировка по `customer_id` (устранение дублей клиентов).
3.  **Table Output.** Загрузка в таблицу `customers`.

#### Трансформация 3. Load Products
1.  **Select Values.** Оставьте поля продукта (`product_id`, `category`, `name` и т.д.).
2.  **Memory Group By.** Группировка по `product_id`.
3.  **Table Output.** Загрузка в таблицу `products`.

---

## 4. Варианты индивидуальных заданий
