WITH productos AS (

    SELECT *
    FROM {{ ref('stg_productos') }}

),

categorias_origen AS (

    SELECT DISTINCT
          COALESCE(NULLIF(TRIM(categoria), ''), 'Sin categoría') AS nombre
        , COALESCE(NULLIF(TRIM(categoria_padre), ''), 'Sin categoría') AS nombre_categoria_padre
    FROM productos

),

categorias_padre AS (

    SELECT DISTINCT
          nombre_categoria_padre AS nombre
        , NULL AS nombre_categoria_padre
    FROM categorias_origen
    WHERE nombre_categoria_padre IS NOT NULL

),

categorias_hijo AS (

    SELECT DISTINCT
          nombre
        , nombre_categoria_padre
    FROM categorias_origen

),

categorias_union AS (

    SELECT * FROM categorias_padre
    UNION ALL
    SELECT * FROM categorias_hijo

),

categorias_deduplicadas AS (

    SELECT
          nombre
        , MAX(nombre_categoria_padre) AS nombre_categoria_padre
    FROM categorias_union
    GROUP BY nombre

),

categorias_con_id AS (

    SELECT
          ROW_NUMBER() OVER (ORDER BY nombre) AS id_categoria
        , nombre
        , nombre_categoria_padre
    FROM categorias_deduplicadas

),

final AS (

    SELECT
          c.id_categoria
        , c.nombre
        , p.id_categoria AS id_categoria_padre

    FROM categorias_con_id c

    LEFT JOIN categorias_con_id p
        ON c.nombre_categoria_padre = p.nombre
       AND c.nombre != p.nombre

)

SELECT *
FROM final