## Расчёт 4-недельных атрибутов взаимодействий для витрины данных

**Контекст**

В рамках построения витрины данных необходимо рассчитать атрибуты активности пользователей (`user_id`) с объектами (`object_id`).
Источники содержат данные о различных типах взаимодействий — встречах, лайках, звонках, департаментных активностях и т.д.

Витрина обновляется еженедельно, инкрементально, без удаления старых данных.
Каждая отчётная неделя заканчивается воскресеньем, а расчёт охватывает последние 4 календарные недели.

Так как между отчётными неделями могут быть пропуски, был добавлен календарь, который обеспечивает непрерывность дат для расчётов.

**Пример расчётного периода**

Если витрина обновляется `03.11.2025` (понедельник), то в расчет включаются 4 прошедшие недели:

| Неделя | Отчетная дата (воскресенье) | Период                  |
| :----: | :-------------------------- | :---------------------- |
|    1   | 12.10.2025                  | 06.10.2025 – 12.10.2025 |
|    2   | 19.10.2025                  | 13.10.2025 – 19.10.2025 |
|    3   | 26.10.2025                  | 20.10.2025 – 26.10.2025 |
|    4   | 02.11.2025                  | 27.10.2025 – 02.11.2025 |


**Проблема исходного подхода**

Ранее для каждой пары `user_id` - `object_id` формировался полный календарь начиная с сентября 2021 года, независимо от наличия взаимодействий. 
Тоесть к паре `user_id` - `object_id` делался CROSS JOIN отчетных недель начиная с сентября 2021 года и по текущую дату.

Пример избыточного календаря

| user_id | object_id | report_dt  |
| ------- | --------- | ---------- |
| 1       | 2         | 05.09.2021 |
| 1       | 2         | 12.09.2021 |
| 1       | 2         | 19.09.2021 |
| ...     | ...       | ...        |
| 1       | 2         | 02.11.2025 |


Даже если пользователь с объектом взаимодействовал всего 3 раза за весь период, создавались сотни "пустых" строк.
При джойне с фактами (`meetings`) таблица разрасталась до десятков миллионов строк, и в Impala появлялся spill.

**Оптимизированное решение**

Теперь календарь создается только в пределах активностей конкретной пары.
Это значительно снижает объём данных и ускоряет расчёты.

**Исходные данные**
В таблице `sandbox_hr.meetings` хранятся данные о встречах:

| user_id | object_id | report_dt  | num_meetings |
| ------- | --------- | ---------- | ------------ |
| 1       | 2         | 2025-09-07 | 3            |
| 1       | 2         | 2025-09-14 | 1            |
| 1       | 2         | 2025-09-28 | 2            |
| 1       | 2         | 2025-10-19 | 4            |
| 1       | 2         | 2025-11-02 | 3            |

Важно: между `2025-09-28` и `2025-10-19` есть пропуск 2 недели — это важно, чтобы показать, как работает оконная функция с “дырками” во времени.

**Шаг 1. Определение диапазонов по каждой паре**

```
-- Удаляем таблицу, если существует
DROP TABLE IF EXISTS sandbox_hr.tmp_pairs;
/
/* Создаём таблицу для хранения диапазонов активности по парам */
CREATE TABLE IF NOT EXISTS sandbox_hr.tmp_pairs (
                                                  user_id INT COMMENT 'Идентификатор пользователя',
                                                  object_id INT COMMENT 'Идентификатор объекта',
                                                  min_dt DATE COMMENT 'Минимальная дата взаимодействия',
                                                  max_dt DATE COMMENT 'Максимальная дата взаимодействия'
                                                )
COMMENT 'Диапазон активности по каждой паре user_id - object_id'
STORED AS PARQUET
/

/* Загружаем данные */
INSERT INTO sandbox_hr.tmp_pairs
SELECT
    user_id,
    object_id,
    MIN(report_dt) AS min_dt,
    MAX(report_dt) AS max_dt
FROM sandbox_hr.meetings
GROUP BY user_id, object_id
/
COMPUTE STATS sandbox_hr.tmp_pairs /* Обновляем статистику для оптимизации выполнения */
/
```

Пример:

| user_id | object_id | min_dt     | max_dt     |
| ------- | --------- | ---------- | ---------- |
| 1       | 2         | 2025-09-07 | 2025-11-02 |


**Шаг 2. Формирование календаря внутри диапазона**

```
DROP TABLE IF EXISTS sandbox_hr.tmp_calendar_per_pair;
/
CREATE TABLE IF NOT EXISTS sandbox_hr.tmp_calendar_per_pair (
                                                              user_id INT COMMENT 'Идентификатор пользователя',
                                                              object_id INT COMMENT 'Идентификатор объекта',
                                                              calend_dt DATE COMMENT 'Отчетная дата недели (воскресенье)'
                                                            )
COMMENT 'Календарь недель в пределах активности каждой пары'
STORED AS PARQUET
/

INSERT INTO sandbox_hr.tmp_calendar_per_pair
SELECT
    p.user_id,
    p.object_id,
    c.report_dt AS calend_dt
FROM sandbox_hr.tmp_pairs p
JOIN sandbox_hr.calendar_full c
ON c.report_dt BETWEEN p.min_dt AND p.max_dt
/
COMPUTE STATS sandbox_hr.tmp_calendar_per_pair
/
```

| user_id | object_id | calend_dt  |
| ------- | --------- | ---------- |
| 1       | 2         | 2025-09-07 |
| 1       | 2         | 2025-09-14 |
| 1       | 2         | 2025-09-21 |
| 1       | 2         | 2025-09-28 |
| 1       | 2         | 2025-10-05 |
| 1       | 2         | 2025-10-12 |
| 1       | 2         | 2025-10-19 |
| 1       | 2         | 2025-10-26 |
| 1       | 2         | 2025-11-02 |

Здесь календарь создается только в рамках активностей пары.

**Шаг 3. Джойн с фактами встреч**

```
DROP TABLE IF EXISTS sandbox_hr.tmp_meetings_with_calendar
/
CREATE TABLE IF NOT EXISTS sandbox_hr.tmp_meetings_with_calendar (
    user_id INT COMMENT 'Идентификатор пользователя',
    object_id INT COMMENT 'Идентификатор объекта',
    calend_dt DATE COMMENT 'Отчетная неделя',
    num_meetings INT COMMENT 'Количество встреч за неделю'
)
COMMENT 'Факты встреч в разрезе календаря по неделям'
STORED AS PARQUET
/

INSERT INTO sandbox_hr.tmp_meetings_with_calendar
SELECT
    c.user_id,
    c.object_id,
    c.calend_dt,
    m.num_meetings
FROM sandbox_hr.tmp_calendar_per_pair c
LEFT JOIN sandbox_hr.meetings m
  ON c.user_id = m.user_id
  AND c.object_id = m.object_id
  AND c.calend_dt = m.report_dt
/
COMPUTE STATS sandbox_hr.tmp_meetings_with_calendar
/
```


Пример объединения календаря и фактов:

| user_id | object_id | calend_dt  | num_meetings |
| ------- | --------- | ---------- | ------------ |
| 1       | 2         | 2025-09-07 | 3            |
| 1       | 2         | 2025-09-14 | 1            |
| 1       | 2         | 2025-09-21 | NULL         |
| 1       | 2         | 2025-09-28 | 2            |
| 1       | 2         | 2025-10-05 | NULL         |
| 1       | 2         | 2025-10-12 | NULL         |
| 1       | 2         | 2025-10-19 | 4            |
| 1       | 2         | 2025-10-26 | NULL         |
| 1       | 2         | 2025-11-02 | 3            |



**Шаг 4. Расчет суммы за 4 недели (скользящее окно)**

Используем оконную функцию `ROWS BETWEEN 3 PRECEDING AND CURRENT ROW` — это значит, берем текущую неделю и три предыдущие строки (независимо от пропусков по календарю).

```
DROP TABLE IF EXISTS sandbox_hr.tmp_meetings_sum_4w;
/


CREATE TABLE IF NOT EXISTS sandbox_hr.tmp_meetings_sum_4w (
    user_id            INT COMMENT 'Идентификатор пользователя',
    object_id          INT COMMENT 'Идентификатор объекта',
    calend_dt          DATE COMMENT 'Отчетная неделя (воскресенье)',
    num_meetings       BIGINT COMMENT 'Количество встреч за неделю',
    sum_num_meetings   BIGINT COMMENT 'Сумма встреч за последние 4 недели'
)
COMMENT 'Результат расчёта количества встреч за 4 отчетные недели'
STORED AS PARQUET
/


INSERT INTO sandbox_hr.tmp_meetings_sum_4w
SELECT
    user_id,
    object_id,
    calend_dt,
    num_meetings,
    SUM(num_meetings) OVER (
        PARTITION BY user_id, object_id
        ORDER BY calend_dt
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS sum_num_meetings
FROM sandbox_hr.tmp_meetings_with_calendar
ORDER BY user_id, object_id, calend_dt
/
COMPUTE STATS sandbox_hr.tmp_meetings_sum_4w
/
```

Итоговый результат:

| user_id | object_id | calend_dt  | num_meetings | sum_num_meetings  |
| ------- | --------- | ---------- | ------------ | ----------------- |
| 1       | 2         | 2025-09-07 | 3            | 3                 |
| 1       | 2         | 2025-09-14 | 1            | 4                 |
| 1       | 2         | 2025-09-21 | NULL         | 4                 |
| 1       | 2         | 2025-09-28 | 2            | 6                 |
| 1       | 2         | 2025-10-05 | NULL         | 3 ← пересчет окна |
| 1       | 2         | 2025-10-12 | NULL         | 3                 |
| 1       | 2         | 2025-10-19 | 4            | 6                 |
| 1       | 2         | 2025-10-26 | NULL         | 4                 |
| 1       | 2         | 2025-11-02 | 3            | 7                 |

Как работает окно

-`ROWS BETWEEN 3 PRECEDING AND CURRENT ROW` берёт ровно 4 строки, не 4 календарные недели.

-Даже если в этих строках были NULL (нет встреч), окно всё равно двигается построчно.

-Для учёта именно календарных недель без данных — мы заранее построили календарь.
