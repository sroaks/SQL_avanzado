-- 1 - TABLA DETAIL
CREATE OR REPLACE TABLE `bd-ml-12-grs.keepcoding.ivr_detail` AS
SELECT
  llamadas.ivr_id AS calls_ivr_id,
  llamadas.phone_number AS calls_phone_number,
  llamadas.ivr_result AS calls_ivr_result,
  llamadas.vdn_label AS calls_vdn_label,
  llamadas.start_date AS calls_start_date,
  FORMAT_DATE('%Y%m%d', llamadas.start_date) AS calls_start_date_id,
  llamadas.end_date AS calls_end_date,
  FORMAT_DATE('%Y%m%d', llamadas.end_date) AS calls_end_date_id,
  TIMESTAMP_DIFF(llamadas.end_date, llamadas.start_date, SECOND) AS calls_total_duration,
  llamadas.customer_segment AS calls_customer_segment,
  llamadas.ivr_language AS calls_ivr_language,
  pasos.step_sequence AS step_sequence,
  pasos.step_name AS step_name,
  pasos.step_result AS step_result,
  pasos.step_description_error AS step_description_error,
  llamadas.steps_module AS calls_steps_module,
  llamadas.module_aggregation AS calls_module_aggregation,
  modulos.module_sequece AS module_sequece,
  modulos.module_name AS module_name,
  modulos.module_duration AS module_duration,
  modulos.module_result AS module_result,
  pasos.document_type AS document_type,
  pasos.document_identification AS document_identification
FROM
  `bd-ml-12-grs.keepcoding.ivr_calls` AS llamadas

LEFT JOIN
  `bd-ml-12-grs.keepcoding.ivr_steps` AS pasos
ON
  llamadas.ivr_id = pasos.ivr_id

LEFT JOIN
  `bd-ml-12-grs.keepcoding.ivr_modules` AS modulos
ON
  llamadas.ivr_id = modulos.ivr_id;


-- 2 TABLA SUMARIO

CREATE OR REPLACE TABLE `bd-ml-12-grs.keepcoding.ivr_summary` AS
SELECT
  calls_ivr_id AS ivr_id,
  calls_phone_number AS phone_number,
  calls_ivr_result AS ivr_result,
  CASE 
    WHEN calls_vdn_label LIKE 'ATC%' THEN 'FRONT'
    WHEN calls_vdn_label LIKE 'TECH%' THEN 'TECH'
    WHEN calls_vdn_label = 'ABSORPTION' THEN 'ABSORPTION'
    ELSE 'RESTO'
  END AS vdn_aggregation,
  calls_start_date AS start_date,
  calls_end_date AS end_date,
  calls_total_duration AS total_duration,
  calls_customer_segment AS customer_segment,
  calls_ivr_language AS ivr_language,
  calls_steps_module AS steps_module,
  calls_module_aggregation AS module_aggregation,
  document_type,
  document_identification,
  calls_phone_number AS customer_phone,
  IF(calls_steps_module > 0 AND calls_steps_module IN (SELECT DISTINCT calls_steps_module FROM `bd-ml-12-grs.keepcoding.ivr_detail` WHERE module_name = 'AVERIA_MASIVA'), 1, 0) AS masiva_lg,
  IF((SELECT COUNT(*) FROM `bd-ml-12-grs.keepcoding.ivr_detail` WHERE calls_ivr_id = calls_ivr_id AND step_name = 'CUSTOMERINFOBYPHONE.TX' AND step_description_error IS NULL) > 0, 1, 0) AS info_by_phone_lg,
  IF((SELECT COUNT(*) FROM `bd-ml-12-grs.keepcoding.ivr_detail` WHERE calls_ivr_id = calls_ivr_id AND step_name = 'CUSTOMERINFOBYDNI.TX' AND step_description_error IS NULL) > 0, 1, 0) AS info_by_dni_lg,
  IF((SELECT COUNT(*) FROM `bd-ml-12-grs.keepcoding.ivr_detail` WHERE calls_phone_number = calls_phone_number AND TIMESTAMP_DIFF(calls_start_date, calls_start_date, HOUR) < 24) > 1, 1, 0) AS repeated_phone_24H,
  IF((SELECT COUNT(*) FROM `bd-ml-12-grs.keepcoding.ivr_detail` WHERE calls_phone_number = calls_phone_number AND TIMESTAMP_DIFF(calls_start_date, calls_end_date, HOUR) < 24) > 1, 1, 0) AS cause_recall_phone_24H
FROM `bd-ml-12-grs.keepcoding.ivr_detail`;


-- 3 CREAR FUNCION LIMPIAR ENTEROS

CREATE OR REPLACE FUNCTION `bd-ml-12-grs.keepcoding.limpiar_enteros`(valor1 INT64, valor2 INT64) AS (
  STRUCT(IFNULL(valor1, -999999) AS valor1, IFNULL(valor2, -999999) AS valor2)
)