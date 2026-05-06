WITH pagos AS (

    SELECT *
    FROM {{ ref('stg_pagos') }}

),

metodos AS (

    SELECT *
    FROM {{ ref('int_metodo_pago') }}

),

alquileres AS (

    SELECT id_alquiler
    FROM {{ ref('int_alquiler') }}

),

validated AS (

    SELECT
          p.id_pago
        , p.id_alquiler
        , p.fecha_pago
        , p.importe
        , m.id_metodo_pago
        , p.estado_pago

    FROM pagos p

    INNER JOIN alquileres a
        ON p.id_alquiler = a.id_alquiler

    INNER JOIN metodos m
        ON TRIM(UPPER(p.metodo_pago)) = m.nombre

    WHERE p.id_pago IS NOT NULL
      AND p.id_alquiler IS NOT NULL
      AND p.importe IS NOT NULL
      AND p.importe >= 0
      AND p.fecha_pago IS NOT NULL

),

deduplicated AS (

    SELECT *
    FROM validated
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_pago
        ORDER BY fecha_pago DESC NULLS LAST
    ) = 1

)

SELECT *
FROM deduplicated