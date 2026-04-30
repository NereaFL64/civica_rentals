WITH producto_scd2 AS (

    SELECT *
    FROM {{ ref('snap_producto') }}

),

categorias AS (

    SELECT *
    FROM {{ ref('int_categoria') }}

),

final AS (

    SELECT
          p.dbt_scd_id AS sk_producto
        , p.id_producto
        , p.nombre AS nombre_producto
        , p.descripcion
        , p.precio_dia
        , p.id_usuario AS id_usuario_propietario
        , p.id_categoria
        , c.nombre AS categoria
        , p.estado_producto
        , p.dbt_valid_from AS valid_from
        , p.dbt_valid_to AS valid_to
        , CASE
            WHEN p.dbt_valid_to IS NULL THEN TRUE
            ELSE FALSE
          END AS is_current

    FROM producto_scd2 p

    LEFT JOIN categorias c
        ON p.id_categoria = c.id_categoria

)

SELECT *
FROM final