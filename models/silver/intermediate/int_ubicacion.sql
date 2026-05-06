WITH ubicaciones AS (

    SELECT DISTINCT
          COALESCE(NULLIF(TRIM(direccion), ''), 'Dirección desconocida') AS calle
        , UPPER(TRIM(ciudad)) AS nombre_ciudad
        , UPPER(TRIM(pais)) AS nombre_pais
    FROM {{ ref('stg_usuarios') }}
    WHERE ciudad IS NOT NULL
      AND pais IS NOT NULL

),

ciudades AS (

    SELECT
          c.id_ciudad
        , c.nombre AS nombre_ciudad
        , p.nombre AS nombre_pais
    FROM {{ ref('int_ciudad') }} c

    LEFT JOIN {{ ref('int_pais') }} p
        ON c.id_pais = p.id_pais

),

joined AS (

    SELECT
          {{ dbt_utils.generate_surrogate_key(['u.calle', 'u.nombre_ciudad', 'u.nombre_pais']) }} AS id_ubicacion
        , u.calle
        , c.id_ciudad

    FROM ubicaciones u

    INNER JOIN ciudades c
        ON u.nombre_ciudad = c.nombre_ciudad
       AND u.nombre_pais = c.nombre_pais

),

final AS (

    SELECT *
    FROM joined

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_ubicacion
        ORDER BY calle
    ) = 1

)

SELECT *
FROM final