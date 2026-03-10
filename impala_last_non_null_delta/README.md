# Пример 1

Ищу дельту между отчетной датой report_dt и максимальной датой звонка max_call_dttm. Но бывает такое, что в max_call_dttm есть NULL.
Поэтому делаю следующее.
Там где в max_call_dttm стоит NULL, надо брать последнюю не NULL запись и протиягивать ниже, после чего искать делту между report_dt и last_max_call_dttm

 
Пример расчета 

```
SELECT user_id,
       object_id,
       report_dt,
       max_call_dttm,
       MAX(max_call_dttm) OVER(PARTITION BY user_id, object_id ORDER BY report_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_max_call_dttm,
       DATEDIFF(report_dt, MAX(max_call_dttm) OVER(PARTITION BY user_id, object_id ORDER BY report_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS days_after_last_call
FROM  schema_test.ees_table
```

Результат

<img width="785" height="753" alt="image001" src="https://github.com/user-attachments/assets/2d845130-1f90-40fa-989d-b382d12c6a96" />


# Пример 2

```
WITH t1 AS (SELECT user_id,
                   object_id,
                   report_dt,
                   calend_dt,
                   user_total_meet_ctc,
                   SUM(user_total_meet_ctc) OVER(PARTITION BY user_id, object_id ORDER BY calend_dt ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS user_total_meet_ctc_lm,
                   SUM(CASE WHEN report_dt IS NOT NULL THEN 1 ELSE 0 END)
                   OVER(PARTITION BY user_id, object_id ORDER BY calend_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS test                               
             FROM test_schema.test_table
             WHERE user_id = 1  AND object_id = 3079
             ),
t2 AS (
       SELECT user_id,
              object_id,
              report_dt,
              calend_dt,
              user_total_meet_ctc,
              user_total_meet_ctc_lm,
              test,
              ROW_NUMBER() OVER(PARTITION BY user_id, object_id, test ORDER BY calend_dt) AS rn, /* Нумерую строки внутри группы */
              MAX(user_total_meet_ctc_lm) OVER(PARTITION BY user_id, object_id, test) AS test_2, /* Беру последнее не нулл значение*/
              MIN(CASE WHEN report_dt IS NOT NULL THEN calend_dt END) OVER(PARTITION BY user_id, object_id, test) AS test_3 /* Протягивую последний report_dt*/
         FROM t1
         ),
t3 AS (
       SELECT user_id,
              object_id,
              report_dt,
              calend_dt,
              user_total_meet_ctc,
              user_total_meet_ctc_lm,
              test,
              rn, /* Нумерую строки внутри группы */
              test_2,
              CASE WHEN rn <= 5 THEN  test_2 ELSE NULL END AS test_4,
              test_3,
              CASE WHEN rn <= 5 THEN  test_3 ELSE NULL END AS test_6,
              CASE WHEN rn <= 5 THEN  calend_dt ELSE NULL END AS test_7               
          FROM t2
          )
SELECT user_id,
       object_id,
       report_dt,
       calend_dt,
       user_total_meet_ctc,
       user_total_meet_ctc_lm,
       test,
       rn,
       test_2,
       test_4 AS user_total_meet_ctc_lm_new,
       test_3,
       test_6,
       test_7 AS new_report_dt
FROM t3
```
<img width="1567" height="413" alt="unnamed" src="https://github.com/user-attachments/assets/abd2dbcb-6c0e-4aa9-8714-503851f838af" />

Если историческая запись в витрине для пары пользователь объект только одна, то необходимо с помощью календаря расширить диапазон.
После чего можно протягивать необходимый атрибут и делать на нем расчеты.

 
```
WITH t1 AS (
SELECT a.user_id,
       a.object_id,
       b.report_dt AS calend_dt
FROM test_schema.ees_user_object_calend_tmp a /* минимальная и максимальная отчетные недели для пар в витрине когда они взаимодействовали. Интервал между которым добавляем календарь */
JOIN test_schema.ees_calendar_0_tmp b /* календарь без пропуска дат за все время, с начала формирования витрины по текущий день */
ON b.report_dt BETWEEN a.min_report_dt AND DATE_ADD(a.max_report_dt, INTERVAL 4 WEEK
WHERE b.report_dt <= CURRENT_DATE())

SELECT a.user_id,
       a.object_id,
       a.calend_dt,
       b.report_dt,
       b.user_total_meet_ctc_lm
FROM t1 a
LEFT JOIN test_schema.ees_table_data_mart b 
ON a.user_id = b.user_id AND a.object_id = b.object_id AND a.calend_dt = b.report_dt
```

<img width="607" height="125" alt="unnamed (1)" src="https://github.com/user-attachments/assets/30e38f74-7b8f-445f-9c12-b7dfcb92e7c3" />
