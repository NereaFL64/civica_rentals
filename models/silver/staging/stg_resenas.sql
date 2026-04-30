WITH src_resenas AS (

    SELECT *
    FROM {{ source('bronze', 'RESENAS') }}

),

renamed_casted AS (

    SELECT
          TRY_TO_NUMBER(ID_RESENA) AS id_resena
        , TRY_TO_NUMBER(ID_ALQUILER) AS id_alquiler
        , TRY_TO_NUMBER(ID_PRODUCTO) AS id_producto
        , CASE
            WHEN TRY_TO_NUMBER(RATING) BETWEEN 1 AND 5
                THEN TRY_TO_NUMBER(RATING)
            ELSE 3
          END AS rating
        , COALESCE(NULLIF(TRIM(COMENTARIO), ''), 'Sin comentario') AS comentario
        , COALESCE(
              TRY_TO_DATE(NULLIF(TRIM(FECHA_RESENA), ''), 'YYYY-MM-DD'),
              TRY_TO_DATE(NULLIF(TRIM(FECHA_RESENA), ''), 'YYYY/MM/DD'),
              TRY_TO_DATE(NULLIF(TRIM(FECHA_RESENA), ''), 'DD-MM-YYYY'),
              TO_DATE('1900-01-01')
          ) AS fecha_resena

    FROM src_resenas

),

deduplicated AS (

    SELECT *
    FROM renamed_casted
    WHERE id_resena IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_resena
        ORDER BY fecha_resena DESC
    ) = 1

)

SELECT *
FROM deduplicated