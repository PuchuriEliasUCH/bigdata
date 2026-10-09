# Filtrado de Bronze y conversión a Silver

Spark convierte los lotes Bronze completados en Parquet Silver, conserva aparte un subconjunto auditable de rechazos y registra el procesamiento para omitir lotes ya completados. La entrada cruda no se elimina en este flujo.

## Elegibilidad y validación

[Spark](../src/spark_pipeline.py) carga las páginas de Bronze, extrae las [columnas de cada fuente](../src/silver_columns.py) y aplica las [reglas de rechazo](../src/silver_rules.py). Solo considera lotes completos con al menos una página. Antes de deduplicar, un registro se **rechaza** si no se puede identificar, titular o fechar:

| Regla de rechazo | Motivo |
|---|---|
| Identificador vacío | `missing_source_id` |
| Título vacío | `missing_title` |
| Sin año de publicación ni fecha | `missing_publication_year_or_date` |

La ausencia de tópicos, resumen, autores, idioma o tipo de publicación **no causa rechazo**. `has_abstract`, `has_topics` y `topic_count` permiten evaluar la cobertura sin reprocesar Bronze. Los rechazados conservan el registro original y su motivo en Parquet Silver bajo `silver/rejected/<batch-id>`, no en JSON Bronze.

## Duplicados

El primer registro válido según `record_order` se conserva de forma determinista. Se rechazan duplicados dentro del lote y también IDs repetidos entre lotes de la misma fuente. Esta comparación no afirma que un registro OpenAlex y uno Scopus sean la misma entidad.

## Salidas

- `silver/works/<batch-id>`: registros aceptados en Parquet con compresión Snappy.
- `silver/_processing`: estado de procesamiento por lote; los lotes ya completados se omiten en ejecuciones posteriores.
- `silver/rejected/<batch-id>`: registros rechazados, con `record_json` (el registro original) y `rejection_reason`.

Los originales permanecen en Bronze; la compactación automática sí puede podar las salidas Silver por lote.

### Reconstrucción por versión

`silver/_schema_version.json` registra la versión vigente (3). Si cambia o el marcador no indica la versión actual, se reconstruyen las salidas derivadas `works`, `rejected`, `_processing` y `_compact` desde Bronze. Antes de eliminarlas se comprueba que existan los metadatos Bronze de todos los lotes implicados, incluidos los del manifiesto. **No se valida previamente la integridad de todas las páginas crudas**: metadatos presentes no garantizan que la reconstrucción pueda leer todos los datos. El procesamiento puede continuar en varias ejecuciones según el presupuesto Silver.

## Columnas de Silver (versión 3 del esquema)

| Grupo | Columnas | OpenAlex | Scopus (vista estándar) |
|---|---|---|---|
| Identidad y trazabilidad | `source`, `source_id`, `batch_id`, `doi`, `normalized_doi`, `ingested_at`, `schema_version`, `processed_at` | Sí | Sí |
| Bibliográficas | `title`, `publication_year`, `publication_date`, `work_type`, `language`, `venue` | Sí | Sin `language` |
| Texto | `abstract` (reconstruido del índice invertido), `keywords`, `has_abstract` | Sí | No hay resumen ni palabras clave |
| Tópicos | `topics` (id, nombre, puntaje, subcampo, campo, dominio), `primary_topic`, `topic_count`, `has_topics` | Sí | Vacíos |
| Autoría | `author_count`, `authors` (id, nombre, ORCID, posición, correspondiente) | Todos los autores | Solo el primer autor; `author_count` nulo |
| Instituciones y países | `institutions` (id, nombre, ROR, código de país, nombre de país, ciudad, tipo), `country_codes`, `country_names` | Códigos ISO en `country_codes` | Nombres en `country_names` (ej. `Ukraine`) |
| Citas | `cited_by_count`, `counts_by_year` | Sí | Solo el total |
| Acceso y estado | `is_oa`, `is_retracted`, `is_paratext` | Sí | `is_oa` sí; los otros nulos |

Notas:
- `cited_by_count` es una **instantánea** al momento de `ingested_at`: incluye citas posteriores a cualquier año base `t`. Para variables sin fuga temporal en OpenAlex se usa `counts_by_year`; en Scopus no se puede reconstruir.
- Los países se guardan como los entrega cada fuente (códigos ISO en OpenAlex, nombres en Scopus). No se inventó una equivalencia entre ambos.
- `title` y `abstract` se limpian (etiquetas, espacios) y se normalizan a Unicode NFC.
- `source_id` solo identifica un trabajo dentro de su fuente: no es una clave de enlace entre bases. Los motivos de rechazo son `missing_source_id`, `missing_title`, `missing_publication_year_or_date`, `duplicate_source_id` y `duplicate_source_id_previous_batch`.

## Compactación automática y lectura activa

El orquestador llama a [compact_pending](../src/compaction.py) después de Spark. Agrupa lotes completados aún no compactados por fuente y año; con al menos `COMPACT_MIN_BATCHES` (10 por defecto) publica generaciones Parquet bajo `silver/_compact/` y un manifiesto atómico `manifest.json`. Valida esquema y conteos aceptados, y conteos de rechazos cuando existen; después revalida la publicación y poda automáticamente los directorios originales `works/<batch-id>` y `rejected/<batch-id>`. Bronze se conserva.

Los lectores deben usar **`active_silver_paths` para aceptados y `active_rejected_paths` para rechazos**: combinan generaciones publicadas con lotes aún no compactados. Leer solo `works/*` o `rejected/*` omite datos ya podados; sumar originales y generaciones puede duplicarlos si la poda quedó pendiente. `resolve_completed_paths` resuelve las generaciones para el procesamiento que conoce los lotes completados. `_processing` conserva la trazabilidad por lote.

Para métricas opcionales por lote, `python -m src.metrics --bronze ... --silver ...` informa conteos, número/tamaño de archivos Parquet y tiempo de escritura.

Continuar con [operación](operacion.md), revisar la [ingesta](ingesta.md) o volver al [índice](README.md).
