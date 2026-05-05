-- models/silver/intermediate/int_resena.sql

WITH resenas AS (

    SELECT *
    FROM {{ ref('stg_resenas') }}

),

alquileres AS (

    SELECT id_alquiler
    FROM {{ ref('int_alquiler') }}

),

productos AS (

    SELECT id_producto
    FROM {{ ref('int_producto') }}

),

validated AS (

    SELECT
          r.id_resena
        , r.id_alquiler
        , r.id_producto
        , r.rating
        , r.comentario
        , r.fecha_resena

    FROM resenas r

    INNER JOIN alquileres a
        ON r.id_alquiler = a.id_alquiler

    INNER JOIN productos p
        ON r.id_producto = p.id_producto

    WHERE r.id_resena IS NOT NULL
      AND r.id_alquiler IS NOT NULL
      AND r.id_producto IS NOT NULL
      AND r.rating BETWEEN 1 AND 5
      AND r.comentario IS NOT NULL

),

deduplicated AS (

    SELECT *
    FROM validated
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_resena
        ORDER BY fecha_resena DESC NULLS LAST
    ) = 1

)

SELECT *
FROM deduplicated