WITH src_productos AS (

    SELECT *
    FROM {{ source('bronze', 'PRODUCTOS') }}

),

renamed_casted AS (

    SELECT
          TRY_TO_NUMBER(ID_PRODUCTO) AS id_producto
        , COALESCE(NULLIF(TRIM(NOMBRE_PRODUCTO), ''), 'Producto desconocido') AS nombre_producto
        , NULLIF(TRIM(DESCRIPCION), '') AS descripcion
        , COALESCE(
              TRY_TO_DECIMAL(
                  REPLACE(REPLACE(NULLIF(TRIM(PRECIO_DIA), ''), '€', ''), ',', '.'),
                  10, 2
              ),
              0
          ) AS precio_dia
        , COALESCE(NULLIF(TRIM(CATEGORIA), ''), 'Sin categoría') AS categoria
        , COALESCE(NULLIF(TRIM(CATEGORIA_PADRE), ''), 'Sin categoría') AS categoria_padre
        , TRY_TO_NUMBER(ID_USUARIO_PROPIETARIO) AS id_usuario_propietario
        , COALESCE(UPPER(NULLIF(TRIM(ESTADO_PRODUCTO), '')), 'DESCONOCIDO') AS estado_producto

    FROM src_productos

),

deduplicated AS (

    SELECT *
    FROM renamed_casted
    WHERE id_producto IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_producto
        ORDER BY id_producto
    ) = 1

)

SELECT *
FROM deduplicated