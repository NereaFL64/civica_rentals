WITH src_usuarios AS (

    SELECT *
    FROM {{ source('bronze', 'USUARIOS') }}

),

renamed_casted AS (

    SELECT
          TRY_TO_NUMBER(ID_USUARIO)      AS id_usuario
        , TRIM(NOMBRE)                   AS nombre
        , LOWER(TRIM(EMAIL))             AS email
        , TRY_TO_DATE(FECHA_REGISTRO)    AS fecha_registro
        , TRIM(DIRECCION)                AS direccion
        , TRIM(CIUDAD)                   AS ciudad
        , TRIM(PAIS)                     AS pais
        , TRIM(TELEFONO)                 AS telefono
        , TRIM(ORIGEN_ALTA)              AS origen_alta

    FROM src_usuarios

)

SELECT *
FROM renamed_casted