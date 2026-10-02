# Operación y configuración

Use las variables de entorno para configurar rutas, credenciales y selección de fuentes. Compose carga `.env`, monta `./data` en `/data` y expone Jupyter en el puerto 8888.

## Configuración principal

| Variable | Uso / valor predeterminado conocido |
|---|---|
| `OPENALEX_TOKEN` | Credencial de OpenAlex |
| `GOLD_PATH` | Opcional. Carpeta de la capa Gold (predeterminado `/data/gold`) |
| `OPENALEX_FILTER` | Opcional. Si no se define, OpenAlex usa la definición de IA de `src/search_criteria.py`; `none` desactiva el filtro |
| `SCOPUS_API` | API key de Scopus |
| `SCOPUS_QUERY` | Consulta que habilita Scopus; vacía significa omitirla |
| `SCOPUS_FIRST_YEAR`, `SCOPUS_LAST_YEAR` | Ambos habilitan partición anual de Scopus |
| `BRONZE_PATH` | `/data/bronze` en Compose |
| `SILVER_PATH` | `/data/silver` en Compose |
| `SPARK_MASTER` | `local[*]` si no se configura |
| `TZ` | `UTC` en Compose |

Compose monta `./data:/data` y el notebook como solo lectura. Los datos persistentes del contenedor quedan así en el directorio local `data/`.

## Ejecución y resultado

`src/run_pipeline.py` coordina OpenAlex, Scopus condicional y Spark local. Al final emite un evento JSON con conteos/estados. La ejecución puede terminar como parcial si OpenAlex falla o algún lote Scopus no se completa; consultar ese estado en lugar de asumir éxito por la existencia de archivos.

La tarea de `scheduler/pipeline.cron` está configurada para ejecutarse cada minuto (`* * * * *`). El README existente indica cada dos minutos y describe solo OpenAlex; hay una discrepancia con la configuración vigente y con el comportamiento actual del orquestador. Para operar, tomar el cron y el código como fuente de verdad, no esa descripción desactualizada.

## Rutas de datos

```text
data/bronze/openalex/<batch-id>/       páginas Parquet OpenAlex
data/bronze/scopus/<batch-id>/         páginas Parquet Scopus
data/bronze/batch_metadata/            estado compartido de lotes
data/bronze/rejected/<batch-id>/       registros rechazados
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
