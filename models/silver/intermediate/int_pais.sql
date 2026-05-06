WITH usuarios AS (

    SELECT *
    FROM {{ ref('stg_usuarios') }}

),

paises AS (

    SELECT DISTINCT
        COALESCE(NULLIF(TRIM(pais), ''), 'Sin país') AS nombre_pais
    FROM usuarios

),

final AS (

    SELECT
          {{ dbt_utils.generate_surrogate_key(['nombre_pais']) }} AS id_pais
        , nombre_pais AS nombre

    FROM paises

)

SELECT *
FROM final