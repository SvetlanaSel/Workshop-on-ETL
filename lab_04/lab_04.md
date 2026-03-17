# Лабораторная работа №4. Анализ и обработка больших данных с Dask (ETL-пайплайн). Вариант 13

**Цель работы:** получить практические навыки работы с библиотекой Dask для построения базовых ETL-конвейеров (Extract, Transform, Load) при обработке больших массивов данных, не помещающихся в оперативную память. Изучить принципы «ленивых вычислений» (lazy evaluation), управление памятью и визуализацию ориентированных ациклических графов (DAG).

**Инструментарий:**
*   Язык программирования: Python 3.
*   Среда выполнения: Google Colab или Jupyter Notebook (локально).
*   Библиотеки: `dask`, `pandas`, `graphviz`.

[Файл lab_04_variant_13.ipynb](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/lab_04_variant_13%20(6).ipynb)

---

### Шаг 1. Extract (Извлечение данных)


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

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/3.jpg)

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/4.jpg)

### Шаг 2. Transform (Трансформация и очистка данных)
Проводится профилирование качества данных: вычисление процента пропущенных значений, метод `.compute()` 
Затем удаляю разреженные столбцы, не прибегая к вычислениям в памяти. Также, удаляю непригодные для анализа столбцы.

Также, изменяю формат даты на подходящий мне
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

# Формирование списка столбцов, где пропусков > 55%
columns_to_drop = list(missing_count_percent[missing_count_percent > 55].index)
print("Удаляемые столбцы:", columns_to_drop)

# ОПТИМИЗАЦИЯ: Ленивое удаление столбцов без вызова .compute()!
# Метод .drop() лишь записывает правило "не читать эти столбцы в будущем".
df_dropped = df.drop(columns=columns_to_drop)

# Проверка результата (вычислит только первые 5 строк первого блока)
df_dropped.head()

# 2. Удаление лишних для анализа столбцов

additional_columns = [
    'Street Code1',           # технические коды улиц
    'Street Code2',
    'Street Code3',
    'Issuer Code',            # дублируется с Issuer Command/Squad
    'Feet From Curb',
    'Violation Post Code',    # почтовый индекс (20% пропусков)
]

# Удаляем столбцы из Dask DataFrame
df_final = df_dropped.drop(columns=additional_columns)

# Проверяем результат
df_final.head()
```
Рузультат:

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/5.jpg)

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/6.jpg)

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/55_prs.png)

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/7_2.png)


### Шаг 3. Load (Загрузка / Сохранение результатов)
Чтобы завершить ETL-цикл, сохраняю очищенный Dask DataFrame обратно на диск в формате `parquet`

```python
df_final.to_csv('55_new_cleaned_parking_violations.csv',
                  single_file=True,
                  index=False)

```
Результат:

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/8_load.png)

Видим, что данные выгружаются:

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/loadings.png)


---

## Шаг 4. Визуализация направленных ациклических графов (DAG)

## Простой граф
Сначала создаю простой граф, визуализирующий процесс подсчета общего количества нарушений, уникальных марок автомобилей и среднего количества нарушений на марку в датасете. Задаю 3 простых python-функции, отложенные объекты, и запускаю вычисления и получаю граф

```python
from dask import delayed
from IPython.display import Image

def get_total_violations():
    return len(df_final)

def get_unique_makes():
    return df_final['Vehicle Make'].nunique().compute()

def avg_violations_per_make(total, unique):
    return round(total / unique, 2)

x = delayed(get_total_violations)()
y = delayed(get_unique_makes)()
z = delayed(avg_violations_per_make)(x, y)

z.visualize(filename='simple_violation_analysis.png')
display(Image('simple_violation_analysis.png'))
```

Итоговый граф:

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/1%D0%BF%D1%80%D0%BE%D1%81%D1%82%D0%BE%D0%B9.png)

## Сложный граф

Теперь создаю сложный граф, который отражает процесс проведения расчетов по районам Нью-Йорка. Считаю данные отдельно для каждого района, вычисляю общее количество нарушений и процент нарушений, совершенных в час-пик (8-10 утра) для каждого района.

```python
from dask import delayed
from IPython.display import Image
import pandas as pd

# список районов для анализа
districts = ['NY', 'K', 'Q', 'BX', 'R']  # Манхэттен, Бруклин, Квинс, Бронкс, Статен-Айленд

# данные по каждому району
def load_district_data(district):
    return df_final[df_final['Violation County'] == district]

layer1 = [delayed(load_district_data)(d) for d in districts]

# количество нарушений в районе
def count_violations(district_data):
    if district_data is None or len(district_data) == 0:
        return 0
    return len(district_data)

layer2 = [delayed(count_violations)(d) for d in layer1]

# нарушения в час-пик
def count_peak_hours(district_data):
    if district_data is None or len(district_data) == 0:
        return 0
    district_data = district_data.copy()
    district_data['Hour'] = pd.to_datetime(district_data['Violation Time'],
                                          format='%H%M', errors='coerce').dt.hour
    peak = district_data[(district_data['Hour'] >= 8) & (district_data['Hour'] <= 10)]
    return len(peak)

layer3 = [delayed(count_peak_hours)(d) for d in layer1]

# процент нарушений в час-пик
def calculate_peak_percentage(total, peak):
    if total == 0:
        return 0
    return round((peak / total) * 100, 2)

layer4 = [delayed(calculate_peak_percentage)(t, p) for t, p in zip(layer2, layer3)]

results = delayed(list)(layer4)

results.visualize(filename='complex_district_analysis.png')
display(Image('complex_district_analysis.png'))
```
Итоговый граф:

![Image alt](https://github.com/SvetlanaSel/Workshop-on-ETL/blob/main/lab_04/img/2%D1%81%D0%BB%D0%BE%D0%B6%D0%BD.png)

