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
        , c.nombre AS ciudad
        , p.nombre AS pais
    FROM {{ ref('int_ubicacion') }} u
    LEFT JOIN {{ ref('int_ciudad') }} c
        ON u.id_ciudad = c.id_ciudad
    LEFT JOIN {{ ref('int_pais') }} p
        ON c.id_pais = p.id_pais

),

final AS (

    SELECT
          us.id_usuario
        , us.nombre
        , us.email
        , us.fecha_registro
        , ub.id_ubicacion
        , us.telefono
        , us.origen_alta
    FROM usuarios us
    LEFT JOIN ubicaciones ub
        ON COALESCE(us.direccion, 'Dirección desconocida') = ub.calle
       AND UPPER(us.ciudad) = ub.ciudad
       AND UPPER(us.pais) = ub.pais

)

SELECT *
FROM final