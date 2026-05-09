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
        , UPPER(TRIM(c.nombre)) AS nombre_ciudad
        , UPPER(TRIM(p.nombre)) AS nombre_pais

    FROM {{ ref('int_ciudad') }} c

    LEFT JOIN {{ ref('int_pais') }} p
        ON c.id_pais = p.id_pais

),

final AS (

    SELECT
          ROW_NUMBER() OVER (
              ORDER BY u.nombre_pais, u.nombre_ciudad, u.calle
          ) AS id_ubicacion

        , u.calle
        , c.id_ciudad

    FROM ubicaciones u

    LEFT JOIN ciudades c
        ON u.nombre_ciudad = c.nombre_ciudad
       AND u.nombre_pais = c.nombre_pais

)

SELECT *
FROM final
WHERE id_ciudad IS NOT NULL