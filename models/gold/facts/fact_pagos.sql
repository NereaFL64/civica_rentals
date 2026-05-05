{{
  config(
    materialized='table'
  )
}}

WITH pagos AS (

    SELECT
          id_pago
        , id_alquiler
        , fecha_pago
        , importe
        , id_metodo_pago
        , estado_pago
    FROM {{ ref('int_pago') }}

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

    INNER JOIN alquileres a
        ON p.id_alquiler = a.id_alquiler

)

SELECT *
FROM final