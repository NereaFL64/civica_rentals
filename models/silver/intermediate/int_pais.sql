WITH paises AS (

    SELECT DISTINCT
          UPPER(TRIM(pais)) AS nombre_pais
    FROM {{ ref('stg_usuarios') }}
    WHERE pais IS NOT NULL

),

final AS (

    SELECT
          ROW_NUMBER() OVER (ORDER BY nombre_pais) AS id_pais
        , nombre_pais AS nombre
    FROM paises

)

SELECT *
FROM final