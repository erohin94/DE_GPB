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


