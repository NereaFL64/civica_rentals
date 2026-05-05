{{
  config(
    materialized='table'
  )
}}

WITH alquileres AS (

    SELECT *
    FROM {{ ref('stg_alquileres') }}

),

usuarios AS (

    SELECT id_usuario
    FROM {{ ref('int_usuario') }}

),

productos AS (

    SELECT id_producto
    FROM {{ ref('int_producto') }}

),

validated AS (

    SELECT
          a.id_alquiler
        , a.id_usuario
        , a.id_producto
        , a.fecha_inicio
        , a.fecha_fin
        , a.estado_alquiler
        , a.canal_reserva
        , a.codigo_promocional

    FROM alquileres a

    INNER JOIN usuarios u
        ON a.id_usuario = u.id_usuario

    INNER JOIN productos p
        ON a.id_producto = p.id_producto

    WHERE a.id_alquiler IS NOT NULL
      AND a.id_usuario IS NOT NULL
      AND a.id_producto IS NOT NULL
      AND a.fecha_inicio IS NOT NULL
      AND a.fecha_fin IS NOT NULL
      AND a.fecha_fin >= a.fecha_inicio

),

final AS (

    SELECT
          id_alquiler
        , id_usuario
        , id_producto
        , fecha_inicio
        , fecha_fin
        , estado_alquiler
        , canal_reserva
        , codigo_promocional
        , CASE
            WHEN DATEDIFF(day, fecha_inicio, fecha_fin) <= 0 THEN 1
            ELSE DATEDIFF(day, fecha_inicio, fecha_fin)
          END AS duracion_dias

    FROM validated

)

SELECT *
FROM final