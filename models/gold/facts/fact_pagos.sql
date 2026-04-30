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

    SELECT *
    FROM {{ ref('fact_alquileres') }}

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

    FROM pagos p

    LEFT JOIN alquileres a
        ON p.id_alquiler = a.id_alquiler

    {% if is_incremental() %}
    WHERE p.fecha_pago > (
        SELECT COALESCE(MAX(id_fecha_pago), '1900-01-01')
        FROM {{ this }}
    )
    {% endif %}

)

SELECT *
FROM final