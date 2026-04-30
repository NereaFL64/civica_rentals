WITH src_alquileres AS (

    SELECT *
    FROM {{ source('bronze', 'ALQUILERES') }}

),

renamed_casted AS (

    SELECT
          TRY_TO_NUMBER(ID_ALQUILER) AS id_alquiler
        , TRY_TO_NUMBER(ID_USUARIO) AS id_usuario
        , TRY_TO_NUMBER(ID_PRODUCTO) AS id_producto
        , COALESCE(
              TRY_TO_DATE(NULLIF(TRIM(FECHA_INICIO), ''), 'YYYY-MM-DD'),
              TRY_TO_DATE(NULLIF(TRIM(FECHA_INICIO), ''), 'YYYY/MM/DD'),
              TRY_TO_DATE(NULLIF(TRIM(FECHA_INICIO), ''), 'DD-MM-YYYY')
          ) AS fecha_inicio
        , COALESCE(
              TRY_TO_DATE(NULLIF(TRIM(FECHA_FIN), ''), 'YYYY-MM-DD'),
              TRY_TO_DATE(NULLIF(TRIM(FECHA_FIN), ''), 'YYYY/MM/DD'),
              TRY_TO_DATE(NULLIF(TRIM(FECHA_FIN), ''), 'DD-MM-YYYY')
          ) AS fecha_fin
        , COALESCE(UPPER(NULLIF(TRIM(ESTADO_ALQUILER), '')), 'DESCONOCIDO') AS estado_alquiler
        , COALESCE(UPPER(NULLIF(TRIM(CANAL_RESERVA), '')), 'DESCONOCIDO') AS canal_reserva
        , NULLIF(TRIM(CODIGO_PROMOCIONAL), '') AS codigo_promocional

    FROM src_alquileres

),

deduplicated AS (

    SELECT *
    FROM renamed_casted
    WHERE id_alquiler IS NOT NULL
      AND id_usuario IS NOT NULL
      AND id_producto IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_alquiler
        ORDER BY fecha_inicio DESC NULLS LAST
    ) = 1

)

SELECT *
FROM deduplicated