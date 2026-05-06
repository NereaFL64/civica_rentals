WITH usuarios AS (

    SELECT *
    FROM {{ ref('stg_usuarios') }}

),

paises AS (

    SELECT *
    FROM {{ ref('int_pais') }}

),

ciudades AS (

    SELECT DISTINCT
          COALESCE(NULLIF(TRIM(ciudad), ''), 'Sin ciudad') AS nombre_ciudad
        , COALESCE(NULLIF(TRIM(pais), ''), 'Sin país') AS nombre_pais
    FROM usuarios

),

final AS (

    SELECT
          {{ dbt_utils.generate_surrogate_key(['c.nombre_ciudad', 'c.nombre_pais']) }} AS id_ciudad
        , c.nombre_ciudad AS nombre
        , p.id_pais

    FROM ciudades c

    INNER JOIN paises p
        ON c.nombre_pais = p.nombre

)

SELECT *
FROM final