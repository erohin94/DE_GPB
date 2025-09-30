/*Автор скрипта Ерохин Е.С.*/
/*Выполнено в рамках задачи OPALAB-14848*/


DROP TABLE IF EXISTS sbxm_hr.course_review_filtered_responses_tmp
/
CREATE TABLE IF NOT EXISTS sbxm_hr.course_review_filtered_responses_tmp (
    response_id BIGINT COMMENT 'Уникальный идентификатор отзыва',
    response_date STRING COMMENT 'Дата и время отзыва',
    object_id BIGINT COMMENT 'Уникальный идентификатор курса',
    event_form STRING COMMENT 'Формат мероприятия в рамках курса',
    event_id BIGINT COMMENT 'Идентификатор мероприятия в рамках курса',
    event_name STRING COMMENT 'Наименование мероприятия в рамках курса',
    event_start_date STRING COMMENT 'Дата и время начала мероприятия',
    event_finish_date STRING COMMENT 'Дата и время окончания мероприятия',
    person_fullname STRING COMMENT 'ФИО сотрудника, оставившего отзыв',
    person_id BIGINT COMMENT 'Идентификатор сотрудника WebTutor',
    `comment` STRING COMMENT 'Комментарий по курсу от сотрудника',
    general_impression INT COMMENT 'Оценка общего впечатления от курса сотрудником',
    practical_applicability INT COMMENT 'Оценка практической применимости курса сотрудником',
    negative_tags STRING COMMENT 'Негативные тэги, отмеченные сотрудником',
    positive_tags STRING COMMENT 'Позитивные тэги, отмеченные сотрудником',
    recomend STRING COMMENT 'Оценка рекомендации + Комментарий пользователя, если он его оставил',
    likes INT COMMENT 'Количество лайков по отзыву',
    dislikes INT COMMENT 'Количество дизлайков по отзыву',
    rn BIGINT COMMENT 'Порядковый номер отзыва',
    rn_2 BIGINT COMMENT 'Порядковый номер содержательного отзыва',
    total_responses BIGINT COMMENT 'Общее количество отзывов у курса',
    is_meaningful_review TINYINT COMMENT 'Содержательность отзыва',
    comment_length INT COMMENT 'Длина отзыва'
)
COMMENT 'Подсчет общего количества отзывов и нумерация отзывов по порядку от самого свежего до старого'
STORED AS PARQUET
/


DROP TABLE IF EXISTS sbxm_hr.course_review_recent_responses_tmp
/
CREATE TABLE IF NOT EXISTS sbxm_hr.course_review_recent_responses_tmp (
    response_id BIGINT COMMENT 'Уникальный идентификатор отзыва',
    response_date STRING COMMENT 'Дата и время отзыва',
    object_id BIGINT COMMENT 'Уникальный идентификатор курса',
    event_form STRING COMMENT 'Формат мероприятия в рамках курса',
    event_id BIGINT COMMENT 'Идентификатор мероприятия в рамках курса',
    event_name STRING COMMENT 'Наименование мероприятия в рамках курса',
    event_start_date STRING COMMENT 'Дата и время начала мероприятия',
    event_finish_date STRING COMMENT 'Дата и время окончания мероприятия',
    person_fullname STRING COMMENT 'ФИО сотрудника, оставившего отзыв',
    person_id BIGINT COMMENT 'Идентификатор сотрудника WebTutor',
    `comment` STRING COMMENT 'Комментарий по курсу от сотрудника',
    general_impression INT COMMENT 'Оценка общего впечатления от курса сотрудником',
    practical_applicability INT COMMENT 'Оценка практической применимости курса сотрудником',
    negative_tags STRING COMMENT 'Негативные тэги, отмеченные сотрудником',
    positive_tags STRING COMMENT 'Позитивные тэги, отмеченные сотрудником',
    recomend STRING COMMENT 'Оценка рекомендации + Комментарий пользователя, если он его оставил',
    likes INT COMMENT 'Количество лайков по отзыву',
    dislikes INT COMMENT 'Количество дизлайков по отзыву',
    rn BIGINT COMMENT 'Порядковый номер отзыва',
    rn_2 BIGINT COMMENT 'Порядковый номер содержательного отзыва',
    total_responses BIGINT COMMENT 'Общее количество отзывов у курса',
    is_meaningful_review TINYINT COMMENT 'Содержательность отзыва',
    comment_length INT COMMENT 'Длина отзыва',
    include_flag TINYINT COMMENT 'Флаг отбора отзывов'
)
COMMENT 'Отсеиваем курсы с менее чем 10 отзывов. Задаем флаг: Пропускаем если меньше 10 отзывов'
STORED AS PARQUET
/


DROP TABLE IF EXISTS sbxm_hr.course_review_final_responses_tmp
/
CREATE TABLE IF NOT EXISTS sbxm_hr.course_review_final_responses_tmp (
    response_id BIGINT COMMENT 'Уникальный идентификатор отзыва',
    response_date STRING COMMENT 'Дата и время отзыва',
    object_id BIGINT COMMENT 'Уникальный идентификатор курса',
    event_form STRING COMMENT 'Формат мероприятия в рамках курса',
    event_id BIGINT COMMENT 'Идентификатор мероприятия в рамках курса',
    event_name STRING COMMENT 'Наименование мероприятия в рамках курса',
    event_start_date STRING COMMENT 'Дата и время начала мероприятия',
    event_finish_date STRING COMMENT 'Дата и время окончания мероприятия',
    person_fullname STRING COMMENT 'ФИО сотрудника, оставившего отзыв',
    person_id BIGINT COMMENT 'Идентификатор сотрудника WebTutor',
    `comment` STRING COMMENT 'Комментарий по курсу от сотрудника',
    general_impression INT COMMENT 'Оценка общего впечатления от курса сотрудником',
    practical_applicability INT COMMENT 'Оценка практической применимости курса сотрудником',
    negative_tags STRING COMMENT 'Негативные тэги, отмеченные сотрудником',
    positive_tags STRING COMMENT 'Позитивные тэги, отмеченные сотрудником',
    recomend STRING COMMENT 'Оценка рекомендации + Комментарий пользователя, если он его оставил',
    likes INT COMMENT 'Количество лайков по отзыву',
    dislikes INT COMMENT 'Количество дизлайков по отзыву',
    rn BIGINT COMMENT 'Порядковый номер отзыва',
    rn_2 BIGINT COMMENT 'Порядковый номер содержательного отзыва',
    total_responses BIGINT COMMENT 'Общее количество отзывов у курса',
    is_meaningful_review TINYINT COMMENT 'Содержательность отзыва',
    comment_length INT COMMENT 'Длина отзыва',
    include_flag TINYINT COMMENT 'Флаг отбора отзывов',
    total_meaningful_review BIGINT COMMENT 'Подсчет содержательных отзывов',
    final_include_flag TINYINT COMMENT 'Итоговый флаг отбора отзывов'
)
COMMENT 'Задаем флаг: Если в последние 150 дней меньше 10 отзывов, снимаем ограничение по периоду'
STORED AS PARQUET
/


DROP TABLE IF EXISTS sbxm_hr.ees_course_review_input
/
CREATE TABLE IF NOT EXISTS sbxm_hr.ees_course_review_input (
    review_id BIGINT COMMENT 'Уникальный идентификатор отзыва',
    review_dttm TIMESTAMP COMMENT 'Дата и время отзыва',
    course_id BIGINT COMMENT 'Уникальный идентификатор курса',
    course_type STRING COMMENT 'Тип объекта (курс, учебная программа, тест)',
    course_status STRING COMMENT 'Статус материала Архив или актуальное',
    course_nm STRING COMMENT 'Наименование курса',
    course_category_nm STRING COMMENT 'Категория курса',
    course_competence_info STRING COMMENT 'Основные компетенции по курсу',
    course_desc STRING COMMENT 'Описание курса',
    course_sum_positive_tags_info STRING COMMENT 'Положительные теги по курсу с указанием суммарного количества по каждому тегу',
    course_sum_negative_tags_info STRING COMMENT 'Отрицательные теги по курсу с указанием суммарного количества по каждому тегу',
    course_rating_nval DECIMAL(28,6) COMMENT 'Рейтинг курса',
    event_form_nm STRING COMMENT 'Формат мероприятия в рамках курса',
    event_id BIGINT COMMENT 'Идентификатор мероприятия в рамках курса',
    event_nm STRING COMMENT 'Наименование мероприятия в рамках курса',
    event_start_dttm TIMESTAMP COMMENT 'Дата и время начала мероприятия',
    event_finish_dttm TIMESTAMP COMMENT 'Дата и время окончания мероприятия',
    employee_full_name STRING COMMENT 'ФИО сотрудника, оставившего отзыв',
    employee_id BIGINT COMMENT 'Идентификатор сотрудника WebTutor',
    course_review_info STRING COMMENT 'Комментарий по курсу от сотрудника',
    course_impression_nval INT COMMENT 'Оценка общего впечатления от курса сотрудником',
    course_applicability_nval INT COMMENT 'Оценка практической применимости курса сотрудником',
    review_negative_tags_info STRING COMMENT 'Негативные теги, отмеченные сотрудником',
    review_positive_tags_info STRING COMMENT 'Позитивные теги, отмеченные сотрудником',
    recommend_info STRING COMMENT 'Оценка рекомендации + Комментарий пользователя, если он его оставил',
    review_like_cnt INT COMMENT 'Количество лайков по отзыву',
    review_dislike_cnt INT COMMENT 'Количество дизлайков по отзыву',
    c_changed_dttm TIMESTAMP COMMENT 'Дата и время появления или изменения записи в витрине'
)
PARTITIONED BY (review_year INT COMMENT 'Год отзыва')
COMMENT 'Отзывы на курсы Импульс'
STORED AS PARQUET
