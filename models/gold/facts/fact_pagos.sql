{{
  config(
    materialized='incremental',
    unique_key='id_pago',
    incremental_strategy='merge'
  )
}}

WITH pagos AS (

    SELECT *
    FROM {{ ref('int_pago') }}

),

alquileres AS (

    SELECT
          id_alquiler
        , sk_usuario
        , sk_producto
    FROM {{ ref('fact_alquileres') }}

),

pagos_nuevos AS (

    SELECT *
    FROM pagos

    {% if is_incremental() %}
    WHERE id_pago NOT IN (
        SELECT id_pago
        FROM {{ this }}
    )
    {% endif %}

),

final AS (

    SELECT
          p.id_pago
        , p.id_alquiler
        , a.sk_usuario
        , a.sk_producto
        , p.id_metodo_pago
        , p.fecha_pago AS id_fecha_pago
        , p.importe
        , p.estado_pago
        , 1 AS num_pagos

    FROM pagos_nuevos p

    INNER JOIN alquileres a
        ON p.id_alquiler = a.id_alquiler

)

SELECT *
FROM final