/*Автор скрипта Ерохин Е.С.*/
/*Выполнено в рамках задачи OPALAB-14848*/


INSERT INTO sbxm_hr.course_review_filtered_responses_tmp
/*Номеруем отзывы по порядку от самого свежего до старого. Нумеруем только содержательные отзывы. Подсчитываем общее количество отзывов у курса.
 *Задаем флаг, содержательный отзыв (1) или нет (0). Определяем длину отзыва*/
SELECT 
    response_id, 
    response_date,
    object_id,
    event_form,
    event_id,
    event_name,
    event_start_date,
    event_finish_date,
    person_fullname,
    person_id,
    `comment`,
    general_impression,
    practical_applicability,
    negative_tags,
    positive_tags,
    recomend,
    likes,
    dislikes,
    ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY response_date DESC) AS rn, /*Нумерация отзывов по порядку*/
    CASE /*Нумеруем только содержательные отзывы (длина >= 50 символов)*/
    WHEN LENGTH(REGEXP_REPLACE(`comment`, '.', 'X')) >= 50 THEN 
    ROW_NUMBER() OVER (PARTITION BY object_id, CASE WHEN LENGTH(REGEXP_REPLACE(`comment`, '.', 'X')) >= 50 THEN 1 ELSE 0 END ORDER BY response_date DESC) ELSE 0 END AS rn_2,
    COUNT(*) OVER (PARTITION BY object_id) AS total_responses, /*Общее количество отзывов у курса*/
    CASE WHEN LENGTH(REGEXP_REPLACE(`comment`, '.', 'X')) >= 50 THEN 1 ELSE 0 END AS is_meaningful_review, /*Флаг содержательности отзыва*/
    LENGTH(REGEXP_REPLACE(`comment`, '.', 'X')) AS comment_length /*Длина отзыва*/
FROM dm_webtutor_hdp.HR_edu_response 
WHERE 1 = 1
    AND object_id IS NOT NULL
    AND `comment` IS NOT NULL
    AND LENGTH(REGEXP_REPLACE(`comment`, '.', 'X')) > 6
/
COMPUTE STATS sbxm_hr.course_review_filtered_responses_tmp						    
/


INSERT INTO sbxm_hr.course_review_recent_responses_tmp
/*Отсеиваем курсы с менее чем 10 отзывов. Задаем флаг новизны отзыва*/
SELECT 
	  response_id,
	  response_date,
	  object_id,
	  event_form,
	  event_id,
      event_name,
      event_start_date,
      event_finish_date,
      person_fullname,
      person_id,
      `comment`,
      general_impression,
      practical_applicability,
      negative_tags,
      positive_tags,
      recomend,
      likes,
      dislikes,
      rn,
      rn_2,
      total_responses,
      is_meaningful_review,
      comment_length,
      CASE /*Задаем флаг новизны отзыва*/
        WHEN rn_2 <= 500 AND response_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 150 DAY) THEN 1
       ELSE 0
      END AS include_flag
FROM sbxm_hr.course_review_filtered_responses_tmp
WHERE total_responses >= 10
/
COMPUTE STATS sbxm_hr.course_review_recent_responses_tmp
/


INSERT INTO sbxm_hr.course_review_final_responses_tmp
/*Задаем финальный флаг: Если в последние 150 дней меньше 10 содержательных отзывов, снимаем ограничение по периоду*/
SELECT
      response_id, 
	  response_date,
	  object_id, 
	  event_form,
	  event_id,
      event_name,
      event_start_date,
      event_finish_date,
      person_fullname,
      person_id,
      `comment`,
      general_impression,
      practical_applicability,
      negative_tags,
      positive_tags,
      recomend,
      likes,
      dislikes,
      rn,
      rn_2,
      total_responses,
      is_meaningful_review,
      comment_length,
      include_flag,
      /*Если отзыв содержательный то получается единица и происходит подсчет, так как ELSE не задан то будет NULL, который COUNT() не учтет. Аналог суммы*/
      COUNT(CASE WHEN is_meaningful_review = 1 THEN 1 END) OVER(PARTITION BY object_id) AS total_meaningful_review, --Подсчет содержательных отзывов
      /*Если у курса мало содержательных отзывов за последние 150 дней, снимаем ограничение по свежести и подтягиваем старые отзывы*/
      CASE 
      WHEN include_flag = 0 
      /*Возвращаем 1 для содержательных отзывов в последние 150 дней, NULL для остальных. И считаем сколько содержательных отзывов за последние 150 дней у курса.
        Если < 10 то ставим флаг 1
        Если оба условия выполняются include_flag = 0 и свежих содержательных отзывов меньше 10, то final_include_flag = 1, включаем отзыв в витрину даже если он старый*/
      AND COUNT(CASE WHEN response_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 150 DAY) AND is_meaningful_review = 1 THEN 1 END) OVER(PARTITION BY object_id) < 10 THEN 1
      ELSE include_flag
      END AS final_include_flag
FROM sbxm_hr.course_review_recent_responses_tmp
/
COMPUTE STATS sbxm_hr.course_review_final_responses_tmp
/


INSERT INTO sbxm_hr.ees_course_review_input
/*Итоговая таблица*/
PARTITION (review_year)				    				    
SELECT
    r.response_id AS review_id,
    r.response_date AS review_dttm,
    r.object_id AS course_id,
    c.catalog AS course_type,
    c.learning_status AS course_status,
    c.object_name AS course_nm,
    c.category AS course_category_nm,
    c.competence AS course_competence_info,
    c.description_block AS course_desc,
    c.plus_tag AS course_sum_positive_tags_info,
    c.minus_tag AS course_sum_negative_tags_info,
    CAST(c.rating AS DECIMAL(28,6)) AS course_rating_nval,
    r.event_form AS event_form_nm,
    r.event_id,
    r.event_name AS event_nm,
    r.event_start_date AS event_start_dttm,
    r.event_finish_date event_finish_dttm,
    r.person_fullname AS employee_full_name,
    r.person_id AS employee_id,
    r.`comment` AS course_review_info,
    r.general_impression AS course_impression_nval,
    r.practical_applicability AS course_applicability_nval,
    r.negative_tags AS review_negative_tags_info,
    r.positive_tags AS review_positive_tags_info,
    r.recomend AS recommend_info,
    r.likes AS review_like_cnt,
    r.dislikes AS review_dislike_cnt,
    CURRENT_TIMESTAMP() AS t_changed_dttm,
    YEAR(r.response_date) AS review_year
FROM dm_webtutor_hdp.HR_edu_catalog c 
JOIN sbxm_hr.course_review_final_responses_tmp r ON c.object_id = r.object_id
WHERE r.final_include_flag = 1
    AND r.rn_2 <= 500
    AND r.total_meaningful_review >= 10
/
COMPUTE STATS sbxm_hr.ees_course_review_input
/
