# Лабораторная работа №4. Анализ и обработка больших данных с Dask (ETL-пайплайн). Вариант 13

**Цель работы:** получить практические навыки работы с библиотекой Dask для построения базовых ETL-конвейеров (Extract, Transform, Load) при обработке больших массивов данных, не помещающихся в оперативную память. Изучить принципы «ленивых вычислений» (lazy evaluation), управление памятью и визуализацию ориентированных ациклических графов (DAG).

**Инструментарий:**
*   Язык программирования: Python 3.
*   Среда выполнения: Google Colab или Jupyter Notebook (локально).
*   Библиотеки: `dask`, `pandas`, `graphviz`.

[Файл lab_04_variant_13.ipynb]()

---

### Шаг 1. Extract (Извлечение данных)

Код:
```python
# Установка Dask с полным набором зависимостей и графов
!pip install "dask[complete]" graphviz

import dask.dataframe as dd
from dask.distributed import Client
from dask.diagnostics import ProgressBar

# Инициализация клиента Dask (Оптимизированные настройки без жесткого лимита памяти)
client = Client(n_workers=2, threads_per_worker=2, processes=True)
display(client)

# Чтение файла (Extract)
## read data using DataFrame API
dtypes = {
    'Issuer Command': 'object',
    'Issuer Squad': 'object',
    'House Number': 'object',
    'Time First Observed': 'object',
    'Violation Description': 'object',
    'Violation Legal Code': 'object',
    'Violation Post Code': 'object',
    'Unregistered Vehicle?': 'float64',
    'Violation Location': 'float64',
    
    'Date First Observed': 'float64',
    'Feet From Curb': 'float64',
    'Law Section': 'float64',
    'Vehicle Year': 'float64'
}

df = dd.read_csv('Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv',  dtype=dtypes)

# Просмотр структуры (метаданных) датасета
df
```
Результат:

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/1.jpg)

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/2.jpg)

![Image alt]([https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/3.jpg)

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/4.jpg)

### Шаг 2. Transform (Трансформация и очистка данных)
Проводится профилирование качества данных: вычисление процента пропущенных значений, метод `.compute()` * Затем удаляю разреженные столбцы, не прибегая к вычислениям в памяти.

```python
# Подсчет пропущенных значений (построение графа вычислений)
missing_values = df.isnull().sum()

# Вычисление процента пропусков
mysize = df.index.size
missing_count = ((missing_values / mysize) * 100)

# Запуск реальных вычислений только для агрегированной статистики
with ProgressBar():
    missing_count_percent = missing_count.compute()

print(missing_count_percent)

# Формирование списка столбцов, где пропусков > 60%
columns_to_drop = list(missing_count_percent[missing_count_percent > 60].index)
print("Удаляемые столбцы:", columns_to_drop)

# ОПТИМИЗАЦИЯ: Ленивое удаление столбцов без вызова .compute()!
# Метод .drop() лишь записывает правило "не читать эти столбцы в будущем".
df_dropped = df.drop(columns=columns_to_drop)

# Проверка результата (вычислит только первые 5 строк первого блока)
df_dropped.head()
```
Рузультат:

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/5.jpg)

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/6.jpg)

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/7.jpg)

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/5.jpg)

### Шаг 3. Load (Загрузка / Сохранение результатов)
Чтобы завершить ETL-цикл, сохраняю очищенный Dask DataFrame обратно на диск в формате `parquet`

```python
df_dropped.to_parquet('cleaned_parking_violations.parquet', 
                      engine='pyarrow')

print("Датасет успешно сохранен!")
```
Результат:

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/8.jpg)

Видим, что данные выгружаются:

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/9.jpg)


---

## Шаг 4. Визуализация направленных ациклических графов (DAG)


