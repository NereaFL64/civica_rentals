WITH usuario_scd2 AS (

    SELECT *
    FROM {{ ref('snap_usuario') }}

),

final AS (

    SELECT
          dbt_scd_id AS sk_usuario
        , id_usuario
        , nombre
        , email
        , fecha_registro
        , id_ubicacion
        , telefono
        , origen_alta
        , dbt_valid_from AS valid_from
        , dbt_valid_to AS valid_to
        , CASE
            WHEN dbt_valid_to IS NULL THEN TRUE
            ELSE FALSE
          END AS is_current

    FROM usuario_scd2

)

SELECT *
FROM final