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

categorias_deduplicadas AS (

    SELECT
          nombre
        , MAX(nombre_categoria_padre) AS nombre_categoria_padre
    FROM categorias_origen
    GROUP BY nombre

),

final AS (

    SELECT
          {{ dbt_utils.generate_surrogate_key(['nombre']) }} AS id_categoria
        , nombre
        , CASE
            WHEN nombre_categoria_padre IS NOT NULL
             AND nombre_categoria_padre != nombre
            THEN {{ dbt_utils.generate_surrogate_key(['nombre_categoria_padre']) }}
            ELSE NULL
          END AS id_categoria_padre

    FROM categorias_deduplicadas

)

SELECT *
FROM final