# Ingesta de OpenAlex y Scopus

El pipeline puede consultar ambas fuentes, pero no lo hace siempre: OpenAlex se intenta en cada ejecución; Scopus solo se ejecuta cuando existe una consulta no vacía en `SCOPUS_QUERY`.

## Selección en el orquestador

`src/run_pipeline.py` ejecuta OpenAlex y captura `OpenAlexRequestError` como estado de la ejecución. Luego evalúa Scopus:

| Configuración | Acción Scopus |
|---|---|
| `SCOPUS_QUERY` vacía o ausente | Se omite |
| `SCOPUS_QUERY` no vacía tras quitar espacios | Ingesta particionada por año (`PUBYEAR`) dentro del rango `INGEST_FIRST_YEAR`–`INGEST_LAST_YEAR`; las particiones que superan 5 000 resultados se subdividen por fecha de carga |

Ambas fuentes comparten el rango predeterminado 2011–año actual UTC y el objetivo inicial de 100 000 registros por fuente. La cuota de cada año es proporcional a su volumen disponible (método del resto mayor), sin exigir más registros que los existentes. Primero se consultan los volúmenes anuales (`group_by` en OpenAlex y una consulta de un registro por año en Scopus). El plan se guarda en `bronze/plans/<fuente>.json` y solo se reutiliza si coincide exactamente su clave: fuente, expresión de búsqueda, años y objetivo. En la fase incremental se prioriza el año con menor fracción recuperada, con un límite efectivo de 6 000 registros por fuente y ejecución.

## Selección temática y presupuestos

Los [criterios compartidos](../src/search_criteria.py) incluyen términos de inteligencia artificial en español e inglés. OpenAlex busca en `title_and_abstract.search` y elimina los comodines `*`; la expresión Scopus generada usa `TITLE-ABS-KEY` y conserva los comodines. **Esa expresión generada no habilita Scopus automáticamente**: la ejecución requiere `SCOPUS_QUERY`. No se garantizan poblaciones idénticas ni filtros obligatorios por idioma o tipo de publicación.

Si `OPENALEX_FILTER` está ausente, se usa el filtro de IA predeterminado; `none` lo desactiva. El [orquestador](../src/run_pipeline.py) reserva 70 s de ingesta: con Scopus habilitado, 28 s para OpenAlex y 42 s para Scopus; sin Scopus, los 70 s corresponden a OpenAlex. Son presupuestos cooperativos, no interrupciones forzadas de solicitudes en curso.

## Ciclo de los lotes

Las respuestas originales se escriben por páginas Parquet Snappy (una fila por registro, con `record_json`, `batch_id`, `page_number` y `record_index`) bajo `bronze/openalex/<batch-id>/` o `bronze/scopus/<batch-id>/`. El estado compartido se guarda como checkpoints JSON atómicos bajo `bronze/batch_metadata`; `BatchStateStore` coordina lotes mediante `flock`. Spark admite también páginas JSON heredadas y lotes mixtos; toma únicamente lotes completos con `page_count > 0`.

La ingesta puede reanudar lotes en curso. OpenAlex recorre páginas mediante cursor; Scopus pagina mediante offset. Los metadatos se actualizan de forma atómica y no deben contener el token. Esto permite distinguir datos recibidos de lotes listos para procesar sin borrar la evidencia Bronze.

## OpenAlex

La autenticación usa `OPENALEX_TOKEN`. El tamaño de página no supera 200. El módulo define 100 000 registros iniciales y 1 000 incrementales; el orquestador sustituye el segundo valor por 6 000. Reintentos acotados, control de ritmo y bloqueo forman parte de la ingesta.

## Scopus

La API key se configura como `SCOPUS_API`; la consulta base, en `SCOPUS_QUERY`. El módulo define 100 000 registros iniciales, 1 000 incrementales y 2 solicitudes/s; la ejecución orquestada usa 6 000 incrementales y 5 solicitudes/s. El tamaño de página predeterminado es 25. La paginación usa offset.

La ventana de 5 000 resultados se aplica **por partición**, no como tope global. A `PUBYEAR` se agregan intervalos disjuntos de `ORIG-LOAD-DATE`, divididos recursivamente cuando exceden la ventana. Se conservan extremos abiertos: 1960 y 2040 son referencias de división, no límites absolutos de los datos. Si una partición no puede dividirse más, no se garantiza exhaustividad; revisar estados como `result_window_reached`, fallos y resultados parciales.

## Conectividad y recuperación

Los clientes aplican reintentos de transporte acotados y persisten cursor u offset. La siguiente invocación retoma el lote aplicable; no existe un supervisor independiente que espere indefinidamente la reconexión. Además, los conteos anuales de Scopus pueden propagar errores de transporte sin los mismos reintentos de la descarga. No debe asumirse que todo error produce el evento final JSON ni que un lote fallido equivale a una carga completa.

## Continuación

Para las rutas y variables completas, consultar [Operación y configuración](operacion.md). Las reglas con que Spark acepta o rechaza cada registro se describen en [Filtrado y modelo Silver](bronze-filtrado-silver.md).

Volver al [índice](README.md). Referencias: [cuotas](../src/quotas.py), [ingesta por años](../src/ingestion.py), [OpenAlex](../src/openalex_ingest.py) y [Scopus](../src/scopus_ingest.py).
