{{
  config(
    materialized='incremental',
    unique_key='id_resena',
    incremental_strategy='merge'
  )
}}

WITH resenas AS (

    SELECT *
    FROM {{ ref('int_resena') }}

),

alquileres AS (

    SELECT *
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

    LEFT JOIN alquileres a
        ON r.id_alquiler = a.id_alquiler

    {% if is_incremental() %}
    WHERE r.fecha_resena > (
        SELECT COALESCE(MAX(id_fecha_resena), '1900-01-01')
        FROM {{ this }}
    )
    {% endif %}

)

SELECT *
FROM final