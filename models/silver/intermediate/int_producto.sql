WITH productos AS (

    SELECT *
    FROM {{ ref('stg_productos') }}

),

usuarios AS (

    SELECT id_usuario
    FROM {{ ref('int_usuario') }}

),

categorias AS (

    SELECT *
    FROM {{ ref('int_categoria') }}

),

joined AS (

    SELECT
          p.id_producto
        , p.nombre_producto AS nombre
        , p.descripcion
        , p.precio_dia
        , p.id_usuario_propietario AS id_usuario
        , c.id_categoria
        , p.estado_producto

    FROM productos p

    INNER JOIN usuarios u
        ON p.id_usuario_propietario = u.id_usuario

    LEFT JOIN categorias c
        ON COALESCE(NULLIF(TRIM(p.categoria), ''), 'Sin categoría') = c.nombre

    WHERE p.id_producto IS NOT NULL
      AND c.id_categoria IS NOT NULL

),

final AS (

    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_producto
        ORDER BY id_producto
    ) = 1

)

SELECT *
FROM final