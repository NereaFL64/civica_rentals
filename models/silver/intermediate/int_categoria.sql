WITH categorias_base AS (

    SELECT DISTINCT
          COALESCE(NULLIF(TRIM(categoria), ''), 'Sin categoría') AS categoria
        , COALESCE(NULLIF(TRIM(categoria_padre), ''), 'Sin categoría') AS categoria_padre
    FROM {{ ref('stg_productos') }}

),

categorias_padre AS (

    SELECT DISTINCT
          categoria_padre AS nombre
        , NULL AS nombre_categoria_padre
    FROM categorias_base

),

categorias_hijo AS (

    SELECT DISTINCT
          categoria AS nombre
        , categoria_padre AS nombre_categoria_padre
    FROM categorias_base

),

categorias_union AS (

    SELECT * FROM categorias_padre
    UNION
    SELECT * FROM categorias_hijo

),

ids AS (

    SELECT
          ROW_NUMBER() OVER (ORDER BY nombre) AS id_categoria
        , nombre
        , nombre_categoria_padre
    FROM categorias_union

),

final AS (

    SELECT
          c.id_categoria
        , c.nombre
        , p.id_categoria AS id_categoria_padre
    FROM ids c
    LEFT JOIN ids p
        ON c.nombre_categoria_padre = p.nombre

)

SELECT *
FROM final