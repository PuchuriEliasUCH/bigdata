# Operación y configuración

Use las variables de entorno para configurar rutas, credenciales y selección de fuentes. Compose carga `.env`, monta `./data` en `/data` y expone Jupyter en el puerto 8888.

## Configuración principal

| Variable | Uso / valor predeterminado conocido |
|---|---|
| `OPENALEX_TOKEN` | Credencial de OpenAlex |
| `OPENALEX_FILTER` | Opcional. Si no se define, OpenAlex usa la definición de IA de `src/search_criteria.py`; `none` desactiva el filtro |
| `SCOPUS_API` | API key de Scopus |
| `SCOPUS_QUERY` | Consulta que habilita Scopus; vacía significa omitirla |
| `INGEST_FIRST_YEAR`, `INGEST_LAST_YEAR` | Rango de años de ambas fuentes (predeterminado 2011 hasta el año actual) |
| `INGEST_INITIAL_TARGET` | Registros de la carga inicial por fuente, repartidos por año (predeterminado 100 000) |
| `COMPACT_MIN_BATCHES` | Lotes sin compactar de una fuente y año a partir de los cuales se compactan (predeterminado 10) |
| `INGEST_TIME_BUDGET_SECONDS` | Tiempo total de ingesta por corrida (predeterminado 80 s de los 120 s del ciclo) |
| `INGEST_SCOPUS_SHARE` | Parte de ese tiempo para Scopus, la fuente más lenta (predeterminado 0,6); el resto es de OpenAlex |
| `SCOPUS_REQUESTS_PER_SECOND` | Solicitudes por segundo a Scopus (predeterminado 5; Elsevier permite 9 por key) |
| `INCREMENTAL_LIMIT` | Tope de seguridad de registros por corrida en la fase incremental (predeterminado 6 000) |
| `BRONZE_PATH` | `/data/bronze` en Compose |
| `SILVER_PATH` | `/data/silver` en Compose |
| `SPARK_MASTER` | `local[*]` si no se configura |
| `TZ` | `UTC` en Compose |

Compose monta `./data:/data` y el notebook como solo lectura. Los datos persistentes del contenedor quedan así en el directorio local `data/`.

## Ejecución y resultado

`src/run_pipeline.py` coordina OpenAlex, Scopus condicional y Spark local. Al final emite un evento JSON con conteos/estados. La ejecución puede terminar como parcial si OpenAlex falla o algún lote Scopus no se completa; consultar ese estado en lugar de asumir éxito por la existencia de archivos.

La tarea de `scheduler/pipeline.cron` se ejecuta cada dos minutos (`*/2 * * * *`): unos 80 s de ingesta más Spark y compactación (alrededor de 40 s). El reporte de cada corrida incluye `ingestion_seconds` y `silver_seconds` para vigilar que el ciclo siga cabiendo en esos dos minutos.

## Rutas de datos

```text
data/bronze/openalex/<batch-id>/       páginas Parquet OpenAlex
data/bronze/scopus/<batch-id>/         páginas Parquet Scopus
data/bronze/batch_metadata/            estado compartido de lotes
data/silver/rejected/<batch-id>/       registros rechazados
data/silver/works/<batch-id>/          Parquet aceptado (Snappy)
data/silver/_processing/               estado de procesamiento
```

Las rutas dentro del contenedor usan `/data/bronze` y `/data/silver`. No inspeccionar ni publicar páginas crudas sin autorización: pueden contener datos de gran tamaño o sensibles.

## Comprobaciones útiles

1. Revisar que las credenciales requeridas estén configuradas sin imprimir sus valores.
2. Confirmar si `SCOPUS_QUERY` está presente y si se definieron ambos años para particionar.
3. Comparar estado/conteos JSON de la ejecución con los lotes completos en `batch_metadata`.
4. Verificar salidas Silver y rechazos por lote; lotes ya procesados se omiten mediante `_processing`.
5. Tras compactar, validar manifiesto y rutas activas antes de considerar cualquier poda.

El [índice](README.md) enlaza el recorrido general; ver también [ingesta](ingesta.md) y [filtrado/Silver](bronze-filtrado-silver.md).


## Carga inicial continua (una sola vez)

El cron limita cada corrida a `INGEST_TIME_BUDGET_SECONDS` (80 s) para no solaparse, así que la carga inicial de 100 000 registros por fuente se reparte en muchas corridas. Para hacerla de una sola vez, detené el servicio programado y lanzá una corrida con un presupuesto grande; después volvé al cron:

```bash
docker compose -f compose.yaml stop spark-pipeline
docker compose -f compose.yaml run --rm -e INGEST_TIME_BUDGET_SECONDS=86400 spark-pipeline python -m src.run_pipeline
docker compose -f compose.yaml up -d
```

El presupuesto se reparte entre las fuentes activas y cada una usa el suyo completo. Si la corrida se interrumpe, los lotes quedan guardados con su cursor y se retoman en la siguiente.
