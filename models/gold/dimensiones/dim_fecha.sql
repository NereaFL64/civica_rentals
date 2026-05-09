{{
  config(
    materialized='table'
  )
}}

WITH fechas AS (

    SELECT
        DATEADD(day, seq4(), '2020-01-01') AS fecha
    FROM TABLE(GENERATOR(ROWCOUNT => 2557))

),

final AS (

    SELECT
          fecha AS id_fecha
        , fecha
        , YEAR(fecha) AS anio
        , MONTH(fecha) AS mes
        , DAY(fecha) AS dia
        , DAYOFWEEK(fecha) AS dia_semana
        , MONTHNAME(fecha) AS nombre_mes
        , CASE
            WHEN DAYOFWEEK(fecha) IN (6, 7) THEN 'FIN_DE_SEMANA'
            ELSE 'LABORAL'
          END AS tipo_dia

    FROM fechas
    WHERE fecha <= '2026-12-31'

)

SELECT *
FROM final