WITH ciudades AS (

    SELECT DISTINCT
          UPPER(TRIM(ciudad)) AS nombre_ciudad
        , UPPER(TRIM(pais)) AS nombre_pais
    FROM {{ ref('stg_usuarios') }}
    WHERE ciudad IS NOT NULL
      AND pais IS NOT NULL

),

paises AS (

    SELECT *
    FROM {{ ref('int_pais') }}

),

final AS (

    SELECT
          ROW_NUMBER() OVER (ORDER BY c.nombre_pais, c.nombre_ciudad) AS id_ciudad
        , c.nombre_ciudad AS nombre
        , p.id_pais
    FROM ciudades c
    LEFT JOIN paises p
        ON c.nombre_pais = p.nombre

)

SELECT *
FROM final