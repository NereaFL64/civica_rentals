WITH src_usuarios AS (

    SELECT *
    FROM {{ source('bronze', 'USUARIOS') }}

),

renamed_casted AS (

    SELECT
          TRY_TO_NUMBER(ID_USUARIO) AS id_usuario
        , COALESCE(NULLIF(TRIM(NOMBRE), ''), 'Desconocido') AS nombre
        , COALESCE(LOWER(NULLIF(TRIM(EMAIL), '')), 'sin_email_' || ID_USUARIO || '@desconocido.com') AS email
        , COALESCE(
              TRY_TO_DATE(NULLIF(TRIM(FECHA_REGISTRO), ''), 'YYYY-MM-DD'),
              TRY_TO_DATE(NULLIF(TRIM(FECHA_REGISTRO), ''), 'YYYY/MM/DD'),
              TRY_TO_DATE(NULLIF(TRIM(FECHA_REGISTRO), ''), 'DD-MM-YYYY'),
              TO_DATE('1900-01-01')
          ) AS fecha_registro
        , NULLIF(TRIM(DIRECCION), '') AS direccion
        , NULLIF(TRIM(CIUDAD), '') AS ciudad
        , NULLIF(TRIM(PAIS), '') AS pais
        , NULLIF(TRIM(TELEFONO), '') AS telefono
        , CASE
            WHEN UPPER(TRIM(ORIGEN_ALTA)) IN ('WEB', 'APP', 'PARTNER', 'REFERIDO')
                THEN UPPER(TRIM(ORIGEN_ALTA))
            ELSE 'WEB'
          END AS origen_alta

    FROM src_usuarios

),

deduplicated AS (

    SELECT *
    FROM renamed_casted
    WHERE id_usuario IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_usuario
        ORDER BY fecha_registro DESC
    ) = 1

)

SELECT *
FROM deduplicated