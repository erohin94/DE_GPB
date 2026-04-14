## **Пример оптимизации**

Есть таблица `sbxm_hr.ees_input_contact_interact_agg` которая собирается на основе джоина 4-х других таблиц.
Таблица широкая, порядка 100 атрибутов. 
Данные с 2021 года, отчетная дата это последний день недели - воскресенье.

**При джоине возникает spill:**

В CLOUDERA manager получили следующую инфу по запросу

`Memory Spilled: 236.6 GiB` - Cпил, кол-во перелитой памяти из оперативной в память хранилища данных.

`Per Node Peak Memory Usage: 4.2 GiB` - Пиковая нагрузка на ноду (оперативной памяти). Пиковая нагрузка на ноду (по умолчанию 5 ГБ, но при достижении 80% от лимита могут появляться спилы даже если в результате текущего запроса их нет).

`Aggregate Peak Memory Usage: 163.4 GiB` - Пиковая память на агрегации.

`Memory Accrual: 3.9 GiB hours` - Накопление памяти.

`HDFS Bytes Read:` 	- Кол-во считанной памяти. Кол-во считанной памяти. Можно увидеть объем информации которую вы отдаете на выполнение запроса импале. Иногда информации может быть слишком много для выполнения в одном запросе, этот фактор относительный и не имеет четких исследованных границ для рекомендаций, носит информативный характер.

`HDFS Bytes Written:` - Объем записанной памяти. Кол-во записанной памяти. Можно увидеть сколько информации получилось в результате запроса, иногда кол-во записанной памяти может превышать объем считанной в разы, что может подсказать о наличии дублей или использовании не оптимальных форматах данных. Не имеет четких исследованных границ для рекомендаций, носит информативный характер. 

`Rows Produced:` - Кол-во строк по результатам запроса. Информативный параметр, для оптимизации не несет критических значений, однако стоит его хранить для отслеживания изменений. Если запускать скрипт в разное время на обновляемых источниках информации, можно получить разные результаты и стоит учитывать кол-во полученных строк если параметры скрипта изменились.

`Duration:` - Длительность выполнения запроса, запрос считается не оптимальным если время его выполнения более 1 часа. (1 час выполнения не является критичным если есть механика nested loop соединения и нет возможности изменить её. У ДРПД не прописаны четкие временные ограничения). 


**Пример таблицы в которой возникал spill:**

```
/*Витрина агрегатов - все приоритеты*/
DROP TABLE IF EXISTS sbxm_hr.ees_input_contact_interact_agg 
/
CREATE TABLE IF NOT EXISTS sbxm_hr.ees_input_contact_interact_agg(
									            user_id INT COMMENT 'Пользователь. Идентификатор сотрудника №1',
									            object_id INT COMMENT 'Объект рекомендации. Идентификатор сотрудника №2',
									            report_dt TIMESTAMP COMMENT 'Дата недельного временного среза (дата последнего дня недели - воскресенья)', 
									            .............
												)
COMMENT 'Витрина агрегатов'
STORED AS PARQUET
/

/*Витрина агрегатов - все приоритеты*/
INSERT INTO sbxm_hr.ees_input_contact_interact_agg
SELECT a.user_id,
	   a.object_id,
	   a.report_dt,
	   ................	   
FROM sbxm_hr.ees_interaction_agg_tmp a --Атрибуты 1-го, 2-го приоритета
LEFT JOIN sbxm_hr.ees_interaction_agg_pr_thr_tmp b ON a.user_id = b.user_id AND a.object_id = b.object_id AND a.report_dt = b.report_dt --Атрибуты 3-го приоритета
LEFT JOIN sbxm_hr.ees_interaction_agg_pr_fo_tmp c ON a.user_id = c.user_id AND a.object_id = c.object_id AND a.report_dt = c.report_dt --Атрибуты 4-го приоритета
LEFT JOIN sbxm_hr.ees_interaction_agg_pr_five_tmp d ON a.user_id = d.user_id AND a.object_id = d.object_id AND a.report_dt = d.report_dt --Атрибуты 5-го приоритета
/
COMPUTE STATS sbxm_hr.ees_input_contact_interact_agg
/
```

Размеры таблиц, которые учавствуют в джоине:

```
SHOW TABLE STATS sbxm_hr.ees_interaction_agg_tmp -- Size 14.46GB
SHOW TABLE STATS sbxm_hr.ees_interaction_agg_pr_thr_tmp -- Size 15.79GB
SHOW TABLE STATS sbxm_hr.ees_interaction_agg_pr_fo_tmp -- Size 6.12GBGB
SHOW TABLE STATS sbxm_hr.ees_interaction_agg_pr_five_tmp -- Size 14.25GB
```

В каждой таблице, которые учавствуют в джоине `471 203 233` строк. Дублей нет.

**Решение как избавится от SPILL:**

Разбить джоин на несколько таблиц, тоесть сделать джоин сначала 1,2 приоритета с 3-им приоритетом. 
Потом джоин 4-го приоритета с 5-м приоритетом.
И добавить партиции по отчетной дате.
Получится порядка 230 партиций по ~70 MB в каждой TMP.

Пример полученных партиций на основе таблицы: `show table stats sbxm_hr.ees_input_contact_interact_1_agg`

<img width="1367" height="215" alt="Снимок" src="https://github.com/user-attachments/assets/abb332be-9828-48e9-bbd2-ef7e9b8ce165" />

После чего обьединяю все приоритеты 1,2,3,4,5 вместе, при этом делаю фильтрацию по годам.
Тоесть сначала за 2021 год, потом за 2022 и тд. Тоесть применение правил `PARTITION_PRUNING`.
Чтение движком только нужных партиций, а не всей таблицы. Тоесть сейчас у нас таблицы `sbxm_hr.ees_input_contact_interact_1_agg` и `sbxm_hr.ees_input_contact_interact_2_agg` разбиты (партицированы) по дате.
В `WHERE` я указываю фильтр по годам. Движок отбрасывает лишние партиции (PRUN - обрезать). Читает только те файлы где есть нужные данные.
И далее делаю UNION ALL всех годов, см тмп `sbxm_hr.ees_input_contact_interact_agg_test`.


После чего получаю итоговую витрину sbxm_hr.ees_input_contact_interact_agg_test_final, в которой уже нет spill.

```
/*Джоин таблиц витрин агрегатов - 1-й и 2-й приоритет с 3-м приоритетом*/

DROP TABLE IF EXISTS sbxm_hr.ees_input_contact_interact_1_agg
/
CREATE TABLE IF NOT EXISTS sbxm_hr.ees_input_contact_interact_1_agg(
									            user_id INT COMMENT 'Пользователь. Идентификатор сотрудника №1',
									            object_id INT COMMENT 'Объект рекомендации. Идентификатор сотрудника №2',									             
									            week_num INT COMMENT 'Порядковый номер недели от начала даты формирования витрины',
									            ...............
												)
PARTITIONED BY (report_dt DATE COMMENT 'отчетная дата')
COMMENT 'Джоин таблиц витрин агрегатов - 1-й и 2-й приоритет с 3-м приоритетом'
STORED AS PARQUET
/


/*Джоин таблиц витрин агрегатов - 4-й приоритет с 5-м приоритетом*/
DROP TABLE IF EXISTS sbxm_hr.ees_input_contact_interact_2_agg
/
CREATE TABLE IF NOT EXISTS sbxm_hr.ees_input_contact_interact_2_agg(
									            user_id INT COMMENT 'Пользователь. Идентификатор сотрудника №1',
									            object_id INT COMMENT 'Объект рекомендации. Идентификатор сотрудника №2',
									            ..............
												)
PARTITIONED BY (report_dt DATE COMMENT 'отчетная дата')												
COMMENT 'Джоин таблиц витрин агрегатов - 4-й приоритет с 5-м приоритетом'
STORED AS PARQUET
/


/*Витрина агрегатов - все приоритеты*/
DROP TABLE IF EXISTS sbxm_hr.ees_input_contact_interact_agg_test
/
CREATE TABLE IF NOT EXISTS sbxm_hr.ees_input_contact_interact_agg_test(
									            user_id INT COMMENT 'Пользователь. Идентификатор сотрудника №1',
									            object_id INT COMMENT 'Объект рекомендации. Идентификатор сотрудника №2',
									            week_num INT COMMENT 'Порядковый номер недели от начала даты формирования витрины',
									            .........
                                                report_dt DATE COMMENT 'Дата недельного временного среза (дата последнего дня недели - воскресенья)'
												)											
COMMENT 'Витрина агрегатов - все приоритеты'
STORED AS PARQUET
/


/*Витрина агрегатов - все приоритеты - итог*/
DROP TABLE IF EXISTS sbxm_hr.ees_input_contact_interact_agg_test_final
/
CREATE TABLE IF NOT EXISTS sbxm_hr.ees_input_contact_interact_agg_test_final(
									            user_id INT COMMENT 'Пользователь. Идентификатор сотрудника №1',
									            object_id INT COMMENT 'Объект рекомендации. Идентификатор сотрудника №2', 
									            week_num INT COMMENT 'Порядковый номер недели от начала даты формирования витрины',
									            ..........
                                                t_changed_dttm TIMESTAMP COMMENT 'Дата и время появления или изменения записи в витрине'
												)
PARTITIONED BY (report_dt DATE COMMENT 'отчетная дата')
COMMENT 'Витрина агрегатов - все приоритеты - итог'
STORED AS PARQUET
/


/*****************************************************************************************************/
/******************************Джоин всех приориетов витрины агрегатов********************************/
/*****************************************************************************************************/


/*Джоин таблиц витрин агрегатов - 1-й и 2-й приоритет с 3-м приоритетом*/
INSERT INTO sbxm_hr.ees_input_contact_interact_1_agg
PARTITION (report_dt)
SELECT a.user_id,
	   a.object_id,	   
	   a.week_num,
	   .......
       CAST(a.report_dt AS DATE) AS report_dt
FROM sbxm_hr.ees_interaction_agg_tmp a 
LEFT JOIN sbxm_hr.ees_interaction_agg_pr_thr_tmp b ON a.user_id = b.user_id AND a.object_id = b.object_id AND a.report_dt = b.report_dt
/
COMPUTE STATS sbxm_hr.ees_input_contact_interact_1_agg
/


/*Джоин таблиц витрин агрегатов - 4-й приоритет с 5-м приоритетом*/
INSERT INTO sbxm_hr.ees_input_contact_interact_2_agg
PARTITION (report_dt)
SELECT a.user_id,
	   a.object_id,	   
	   ......
       CAST(a.report_dt AS DATE) AS report_dt
FROM sbxm_hr.ees_interaction_agg_pr_fo_tmp a
LEFT JOIN sbxm_hr.ees_interaction_agg_pr_five_tmp b ON a.user_id = b.user_id AND a.object_id = b.object_id AND a.report_dt = b.report_dt 
/
COMPUTE STATS sbxm_hr.ees_input_contact_interact_2_agg
/



/*Витрина агрегатов - все приоритеты*/
INSERT INTO sbxm_hr.ees_input_contact_interact_agg_test
SELECT a.user_id,
	   a.object_id,
	   .......
       a.report_dt
FROM sbxm_hr.ees_input_contact_interact_1_agg a 
LEFT JOIN sbxm_hr.ees_input_contact_interact_2_agg b ON a.user_id = b.user_id AND a.object_id = b.object_id AND a.report_dt = b.report_dt  
WHERE a.report_dt >= '2021-01-01' AND a.report_dt < '2022-01-01'

UNION ALL 

SELECT a.user_id,
	   a.object_id,
	   .......
       a.report_dt
FROM sbxm_hr.ees_input_contact_interact_1_agg a 
LEFT JOIN sbxm_hr.ees_input_contact_interact_2_agg b ON a.user_id = b.user_id AND a.object_id = b.object_id AND a.report_dt = b.report_dt  
WHERE a.report_dt >= '2022-01-01' AND a.report_dt < '2023-01-01'

UNION ALL 

SELECT a.user_id,
	   a.object_id,
	   .........
       a.report_dt
FROM sbxm_hr.ees_input_contact_interact_1_agg a 
LEFT JOIN sbxm_hr.ees_input_contact_interact_2_agg b ON a.user_id = b.user_id AND a.object_id = b.object_id AND a.report_dt = b.report_dt  
WHERE a.report_dt >= '2023-01-01' AND a.report_dt < '2024-01-01'

UNION ALL 

SELECT a.user_id,
	   a.object_id,
	   .........
       a.report_dt
FROM sbxm_hr.ees_input_contact_interact_1_agg a 
LEFT JOIN sbxm_hr.ees_input_contact_interact_2_agg b ON a.user_id = b.user_id AND a.object_id = b.object_id AND a.report_dt = b.report_dt  
WHERE a.report_dt >= '2024-01-01' AND a.report_dt < '2025-01-01'

UNION ALL 

SELECT a.user_id,
	   a.object_id,
	   .........
       a.report_dt
FROM sbxm_hr.ees_input_contact_interact_1_agg a 
LEFT JOIN sbxm_hr.ees_input_contact_interact_2_agg b ON a.user_id = b.user_id AND a.object_id = b.object_id AND a.report_dt = b.report_dt  
WHERE a.report_dt >= '2025-01-01' AND a.report_dt < '2026-01-01'

UNION ALL 

SELECT a.user_id,
	   a.object_id,
	   ...........
       a.report_dt
FROM sbxm_hr.ees_input_contact_interact_1_agg a 
LEFT JOIN sbxm_hr.ees_input_contact_interact_2_agg b ON a.user_id = b.user_id AND a.object_id = b.object_id AND a.report_dt = b.report_dt  
WHERE a.report_dt >= '2026-01-01' AND a.report_dt < '2027-01-01'
/
COMPUTE STATS sbxm_hr.ees_input_contact_interact_agg_test
/



/*Итоговая витрина*/
INSERT INTO sbxm_hr.ees_input_contact_interact_agg_test_final
PARTITION (report_dt)
SELECT a.user_id,
	   a.object_id,
	   ...........
       CURRENT_TIMESTAMP() AS t_changed_dttm,
       a.report_dt
FROM sbxm_hr.ees_input_contact_interact_agg_test a
/
COMPUTE STATS sbxm_hr.ees_input_contact_interact_agg_test_final
/
```

Получаем:

```
Memory Spilled: 0 B
Per Node Peak Memory Usage: 2.8 GiB
Aggregate Peak Memory Usage: 40.1 GiB
Memory Accrual: 3.4 GiB hours
Duration: 8.6m
HDFS Bytes Read: 35.3 GiB (18.79GB + 13.37GB)
HDFS Bytes Written: 30.5 GiB
```
