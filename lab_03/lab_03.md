# Лабораторная работа №3. Интеграция данных из разнородных источников. Проектирование архитектуры. Вариант 13

**Цель работы.** Разработать комплексное ETL-решение для интеграции данных из локальной СУБД PostgreSQL и файловых источников (CSV/Excel) в целевое хранилище MySQL. Спроектировать верхнеуровневую архитектуру аналитического решения.

**Задание**
| № | Тема и источники | Задача (ETL + Аналитика) |
|---|---|---|
| 13 | **Лояльность.** <br>PostgreSQL: Бонусная программа (счета).<br>CSV: История покупок.<br>Excel: Акции и спецпредложения. | Проанализировать эффективность программы лояльности. Сколько бонусов начислено/списано по акционным товарам. |
---

Созданные файлы:
-[JOB](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_03/Job_CSV_to_MYsql_lab3.kjb)
-[csv transformation](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_03/lab_03_csv_to_purchase.ktr)
-[excel+pg transformation](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_03/lab_03_excel_pg.ktr)

---

## 1. Архитектура решения

Перед реализацией в Pentaho спроектирую схему потоков данных.

<img width="1092" height="442" alt="лаб3 drawio (1)" src="https://github.com/user-attachments/assets/4ac655e7-f673-4d13-9cda-eb83a126e199" />

---

## 2. Реализация ETL-процесса

### Шаг 1. Подготовка источника (PostgreSQL)

1. Создаю таблицу в postgre sql

Запускаю PostgreSQL

<img width="1159" height="778" alt="image" src="https://github.com/user-attachments/assets/2a8cf7c0-5e10-46c3-bffe-b4885400b0f9" />

Создаю таблицу

<img width="456" height="447" alt="image" src="https://github.com/user-attachments/assets/8dc5c615-2cf0-499d-bb28-3e843d0431fc" />

Добавляю данные

<img width="1126" height="260" alt="image" src="https://github.com/user-attachments/assets/0cbd5e4d-dd5a-4244-9765-3c66c9f1a4e1" />

Проверяю

<img width="1184" height="512" alt="image" src="https://github.com/user-attachments/assets/39723d09-d8fb-4e2b-975e-adae5a6eb8b4" />

---

### Шаг 2. Подготовка файлов

Были созданы 2 файла:
- [csv](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_03/purchase_history.csv)
- [excel](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_03/promotions_data.xlsx)

---

### Шаг 3. Разработка трансформации в Pentaho (Spoon)

#### 1. Трансформация csv

<img width="894" height="371" alt="image" src="https://github.com/user-attachments/assets/0778cf60-77ec-4926-9511-69fc8fa64fda" />

Настройка трансформации:

- Импорт файла:

<img width="882" height="735" alt="image" src="https://github.com/user-attachments/assets/e779137a-52e4-4fe2-b5cc-c756ba669d45" />

- Отбор значений:

<img width="861" height="466" alt="image" src="https://github.com/user-attachments/assets/db91f612-33a3-4705-b5bb-6ee850fa53fd" />

- Настройка группировки, убираю ненужные столбцы:

<img width="753" height="418" alt="image" src="https://github.com/user-attachments/assets/c7dd4389-e421-45ea-9d9b-7ae7b367b2c8" />

- Убираю данные, где нет информации о покупке:

<img width="757" height="399" alt="image" src="https://github.com/user-attachments/assets/1de4ea98-64e5-4464-ac1b-473319da56bc" />

- Сортировка по айди покупателя:

<img width="982" height="379" alt="image" src="https://github.com/user-attachments/assets/a8ea2dab-d568-49b4-b1ca-191e461b4d3e" />

- Экспорт в СУБД:

<img width="970" height="764" alt="image" src="https://github.com/user-attachments/assets/3f5262f5-b885-4b14-b8ed-e72f1e2ee336" />

#### 2. Трансформация excel + PG

<img width="920" height="462" alt="image" src="https://github.com/user-attachments/assets/289ec51d-d9be-4f70-8658-aff24f8ca3b9" />

- Импорт:
Постгри:

<img width="1032" height="706" alt="image" src="https://github.com/user-attachments/assets/734db2b8-053e-4deb-a6ad-6fcd376bee35" />

Эксель:

<img width="1241" height="597" alt="image" src="https://github.com/user-attachments/assets/427dfc10-8281-4b2c-8b03-98b32cc1465a" />

<img width="871" height="195" alt="image" src="https://github.com/user-attachments/assets/85f9e8ef-5d55-441f-92ec-e31dd0fd0d92" />

<img width="1186" height="647" alt="image" src="https://github.com/user-attachments/assets/6f388a32-a0b9-412a-b3be-f43b3a8f84af" />

- Сортировка по айди покупателя:

<img width="982" height="379" alt="image" src="https://github.com/user-attachments/assets/fdd36510-125e-49d2-9025-22b78b68ae11" />

- Объединение данных:

<img width="457" height="473" alt="image" src="https://github.com/user-attachments/assets/df525c32-760f-4bec-9797-c4734e647a63" />

- Корректировка значений:

<img width="920" height="785" alt="image" src="https://github.com/user-attachments/assets/4a29539d-452c-4885-b914-0c4eaddc7d8d" />

- Отбор столбцов:

<img width="792" height="803" alt="image" src="https://github.com/user-attachments/assets/c81cee0f-057b-46f5-a3b9-77a6439ce83b" />

- Фильтрация:

<img width="765" height="405" alt="image" src="https://github.com/user-attachments/assets/33437873-c6e4-4d01-9ebd-001b9f1bf990" />

- Расчет нового показателя:

bonus_usage_rate = (promo_written_off / promo_accrued) * 100 - насколько эффективно клиенты тратят бонусы, полученные по акциям

<img width="1245" height="344" alt="image" src="https://github.com/user-attachments/assets/280e3c14-1b0c-4df4-b99c-4e2f0ec9baab" />


#### 3. JOB

<img width="910" height="582" alt="image" src="https://github.com/user-attachments/assets/7854bd16-7418-40b0-b99b-69ea897cc86c" />


---

### Шаг 4. Создание таблицы и запуск JOB

Создаю таблицу для pg+excel:

<img width="464" height="549" alt="image" src="https://github.com/user-attachments/assets/231a57fa-2837-4cf2-845c-06c931a7ff6a" />

Проверяю после запуска JOB:

<img width="1429" height="514" alt="image" src="https://github.com/user-attachments/assets/47d26a96-3b97-4d8f-8be0-1c7fe89d75de" />

Данные успешно перенеслись

Аналогично с таблицей purchase_history

<img width="1355" height="353" alt="image" src="https://github.com/user-attachments/assets/95bf0435-7ff7-45ee-875c-b3949b5bdf07" />

---

## 5. Создание представлений

---

## 6. Вывод

В ходе выполнения лабораторной работы была спроектирована архитектура решения, было разработано комплексное ETL-решение для импорта данных из СУБД PostgreSQL и файловых источников CSV и Excel в хранилище MySQL.

Индивидуальное задание было выполнено в полном объеме.

