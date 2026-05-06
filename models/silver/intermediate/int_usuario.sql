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
          u.id_ubicacion
        , u.calle
        , c.nombre AS nombre_ciudad
        , p.nombre AS nombre_pais

    FROM {{ ref('int_ubicacion') }} u

    INNER JOIN {{ ref('int_ciudad') }} c
        ON u.id_ciudad = c.id_ciudad

    INNER JOIN {{ ref('int_pais') }} p
        ON c.id_pais = p.id_pais

),

joined AS (

    SELECT
          us.id_usuario
        , us.nombre
        , us.email
        , us.fecha_registro
        , ub.id_ubicacion
        , us.telefono
        , us.origen_alta

    FROM usuarios us

    INNER JOIN ubicaciones ub
        ON COALESCE(NULLIF(TRIM(us.direccion), ''), 'Dirección desconocida') = ub.calle
       AND UPPER(TRIM(us.ciudad)) = ub.nombre_ciudad
       AND UPPER(TRIM(us.pais)) = ub.nombre_pais

    WHERE us.id_usuario IS NOT NULL

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