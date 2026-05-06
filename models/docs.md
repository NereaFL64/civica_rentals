{% docs arquitectura_capas %}

El proyecto sigue una arquitectura medallion:

- Bronze: datos crudos ingestados desde S3 mediante Snowpipe.
- Silver: limpieza, normalización, deduplicación y modelo relacional.
- Gold: modelo dimensional orientado a analítica y Power BI.

También se aplican tests de calidad, snapshots SCD2, modelos incrementales y automatización mediante dbt Cloud Jobs.

{% enddocs %}