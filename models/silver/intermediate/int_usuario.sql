{{
  config(
    materialized='table'
  )
}}

WITH usuarios AS (

    SELECT *
    FROM {{ ref('stg_usuarios') }}

),

ubicaciones AS (

    SELECT
          ub.id_ubicacion
        , ub.calle
        , c.nombre AS nombre_ciudad
        , p.nombre AS nombre_pais

    FROM {{ ref('int_ubicacion') }} ub

    INNER JOIN {{ ref('int_ciudad') }} c
        ON ub.id_ciudad = c.id_ciudad

    INNER JOIN {{ ref('int_pais') }} p
        ON c.id_pais = p.id_pais

),

joined AS (

    SELECT
          u.id_usuario
        , u.nombre
        , u.email
        , u.fecha_registro
        , ub.id_ubicacion
        , u.telefono
        , u.origen_alta

    FROM usuarios u

    INNER JOIN ubicaciones ub
        ON UPPER(TRIM(COALESCE(u.direccion, 'Dirección desconocida'))) = UPPER(TRIM(ub.calle))
       AND UPPER(TRIM(COALESCE(u.ciudad, 'Sin ciudad'))) = UPPER(TRIM(ub.nombre_ciudad))
       AND UPPER(TRIM(COALESCE(u.pais, 'Sin país'))) = UPPER(TRIM(ub.nombre_pais))

    WHERE u.id_usuario IS NOT NULL

),

final AS (

    SELECT *
    FROM joined

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_usuario
        ORDER BY fecha_registro DESC NULLS LAST
    ) = 1

)

SELECT *
FROM final