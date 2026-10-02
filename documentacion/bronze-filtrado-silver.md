# Filtrado de Bronze y conversión a Silver

Spark convierte los lotes Bronze completados en Parquet Silver, conserva aparte un subconjunto auditable de rechazos y registra el procesamiento para omitir lotes ya completados. La entrada cruda no se elimina en este flujo.

## Elegibilidad y validación

`src/spark_pipeline.py` carga las páginas de Bronze, extrae las columnas de cada fuente (`src/silver_columns.py`) y aplica las reglas de rechazo (`src/silver_rules.py`, un solo módulo porque el filtrado va a cambiar). Solo considera lotes completos con al menos una página. Un registro se **rechaza** únicamente si no se puede identificar, titular o fechar:

| Regla de rechazo | Motivo |
|---|---|
| Identificador vacío | `missing_source_id` |
| Título vacío | `missing_title` |
| Sin año de publicación ni fecha | `missing_publication_year_or_date` |

Todo lo demás (sin tópicos, sin resumen, sin autores) **se conserva** y queda marcado en columnas de calidad (`has_abstract`, `has_topics`, etc.), para decidir el filtro final sin reprocesar Bronze. Los rechazados conservan el registro original y su motivo en `silver/rejected/<batch-id>`.

## Duplicados

El primer registro válido según `record_order` se conserva de forma determinista. Se rechazan duplicados dentro del lote y también IDs repetidos entre lotes de la misma fuente. Esta comparación no afirma que un registro OpenAlex y uno Scopus sean la misma entidad.

## Salidas

- `silver/works/<batch-id>`: registros aceptados en Parquet con compresión Snappy.
- `silver/_processing`: estado de procesamiento por lote; los lotes ya completados se omiten en ejecuciones posteriores.
- `silver/rejected/<batch-id>`: registros rechazados, con `record_json` (el registro original) y `rejection_reason`.

Los originales permanecen en Bronze. No se debe confundir el flujo de filtrado con una política de limpieza o eliminación.

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

## Compactación opcional

`src/compaction.py` ofrece compactación opcional de lotes compatibles de una misma fuente. Publica Parquet versionado y un manifiesto atómico; los lectores deben resolver las rutas activas mediante `active_silver_paths` / `resolve_completed_paths`. La poda de archivos anteriores es una acción separada y destructiva: solo corresponde después de validar la compactación.

Para métricas opcionales por lote, `python -m src.metrics --bronze ... --silver ...` informa conteos, número/tamaño de archivos Parquet y tiempo de escritura.
