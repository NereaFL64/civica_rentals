{{
  config(
    materialized='incremental',
    unique_key='id_alquiler',
    incremental_strategy='merge'
  )
}}

WITH alquileres AS (

    SELECT *
    FROM {{ ref('int_alquiler') }}

),

usuarios AS (

    SELECT
          sk_usuario
        , id_usuario
    FROM {{ ref('dim_usuario') }}
    WHERE is_current = TRUE

),

productos AS (

    SELECT
          sk_producto
        , id_producto
    FROM {{ ref('dim_producto') }}
    WHERE is_current = TRUE

),

final AS (

    SELECT
          a.id_alquiler
        , u.sk_usuario
        , p.sk_producto
        , a.fecha_inicio AS id_fecha_inicio
        , a.fecha_fin AS id_fecha_fin
        , a.duracion_dias
        , a.estado_alquiler
        , a.canal_reserva
        , a.codigo_promocional
        , 1 AS num_alquileres

    FROM alquileres a

    INNER JOIN usuarios u
        ON a.id_usuario = u.id_usuario

    INNER JOIN productos p
        ON a.id_producto = p.id_producto

    WHERE a.fecha_inicio BETWEEN '2020-01-01' AND '2026-12-31'

    {% if is_incremental() %}
      AND a.id_alquiler NOT IN (
          SELECT id_alquiler
          FROM {{ this }}
      )
    {% endif %}

)

SELECT *
FROM final