{{
  config(
    materialized='incremental',
    unique_key='id_resena',
    incremental_strategy='merge'
  )
}}

WITH resenas AS (

    SELECT *
    FROM {{ ref('int_resena_sentimiento') }}

),

alquileres AS (

    SELECT
          id_alquiler
        , sk_usuario
        , sk_producto
    FROM {{ ref('fact_alquileres') }}

),

final AS (

    SELECT
          r.id_resena
        , r.id_alquiler
        , a.sk_usuario
        , a.sk_producto
        , r.id_producto
        , r.fecha_resena AS id_fecha_resena
        , r.rating
        , r.sentimiento
        , 1 AS num_resenas

    FROM resenas r

    INNER JOIN alquileres a
        ON r.id_alquiler = a.id_alquiler

    WHERE r.fecha_resena BETWEEN '2020-01-01' AND '2026-12-31'

    {% if is_incremental() %}
      AND r.id_resena NOT IN (
          SELECT id_resena
          FROM {{ this }}
      )
    {% endif %}

)

SELECT *
FROM final