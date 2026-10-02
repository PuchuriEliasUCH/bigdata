# Filtrado de Bronze y conversión a Silver

Spark convierte los lotes Bronze completados en Parquet Silver, conserva aparte un subconjunto auditable de rechazos y registra el procesamiento para omitir lotes ya completados. La entrada cruda no se elimina en este flujo.

## Elegibilidad y validación

`src/spark_pipeline.py` carga páginas según el esquema de la fuente y normaliza sus registros. Solo considera lotes con estado completado y al menos una página. Para aceptar un registro se requiere:

| Regla | OpenAlex | Scopus |
|---|---:|---:|
| Identificador no vacío | Sí | Sí |
| Título no vacío | Sí | Sí |
| Año de publicación o fecha | Sí | Sí |
| Al menos un ID o nombre de tema | Sí | No |

Scopus no aporta temas mapeados en este pipeline, por lo que la regla de temas no se le aplica. Los registros rechazados conservan motivos auditables y el subconjunto crudo correspondiente se escribe en `silver/rejected/<batch-id>`.

## Duplicados

El primer registro válido según `record_order` se conserva de forma determinista. Se rechazan duplicados dentro del lote y también IDs repetidos entre lotes de la misma fuente. Esta comparación no afirma que un registro OpenAlex y uno Scopus sean la misma entidad.

## Salidas

- `silver/works/<batch-id>`: registros aceptados en Parquet con compresión Snappy.
- `silver/_processing`: estado de procesamiento por lote; los lotes ya completados se omiten en ejecuciones posteriores.
- `silver/rejected/<batch-id>`: registros rechazados, con `record_json` (el registro original) y `rejection_reason`.

Los originales permanecen en Bronze. No se debe confundir el flujo de filtrado con una política de limpieza o eliminación.

## Campos comunes de Silver

| Campo | Significado |
|---|---|
| `batch_id` | Lote de origen |
| `openalex_id` | Identificador de trabajo: para Scopus se llena con el `eid` de Scopus, no con un ID OpenAlex nativo |
| `source` | `openalex` o `scopus` |
| `source_id` | Actualmente copia `openalex_id` |
| `title` | Título |
| `publication_year`, `publication_date` | Año y fecha; si falta el año, puede derivarse de los primeros cuatro caracteres de la fecha |
| `topic_ids`, `topic_names`, `topic_count` | Temas disponibles; Scopus queda sin temas mapeados |
| `work_type`, `doi`, `normalized_doi` | Tipo y DOI; el normalizado elimina prefijo URL/`doi` y convierte a minúsculas |
| `cited_by_count`, `is_oa`, `source_name` | Citaciones, acceso abierto y nombre de fuente; `is_oa` es nulo para Scopus |
| `processed_at` | Momento de procesamiento |

Los nombres `openalex_id` y `source_id` son comunes por compatibilidad del esquema y pueden inducir a error en filas Scopus. No constituyen una clave de enlace entre bases ni realizan resolución de entidades.

## Compactación opcional

`src/compaction.py` ofrece compactación opcional de lotes compatibles de una misma fuente. Publica Parquet versionado y un manifiesto atómico; los lectores deben resolver las rutas activas mediante `active_silver_paths` / `resolve_completed_paths`. La poda de archivos anteriores es una acción separada y destructiva: solo corresponde después de validar la compactación.

Para métricas opcionales por lote, `python -m src.metrics --bronze ... --silver ...` informa conteos, número/tamaño de archivos Parquet y tiempo de escritura.
