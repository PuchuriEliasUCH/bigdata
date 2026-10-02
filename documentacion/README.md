# Pipeline bibliográfico: de Bronze a Silver

Esta documentación explica cómo se seleccionan OpenAlex y Scopus, cómo se almacenan sus respuestas y qué transformaciones aplica Spark. Está dirigida a quienes ejecutan u operan el pipeline; complementa el README del repositorio sin duplicarlo.

## Recorrido

```text
OpenAlex (siempre se intenta) ─┐
Scopus (solo si hay consulta) ──┴─> Bronze: páginas Parquet + metadatos de lote
                                      │ lotes completos con páginas
                                      v
                                  Spark local
                         validar · deduplicar · tipar
                            /                 \
          Bronze/rejected/<lote>       Silver/works/<lote> (Parquet)
                                             + _processing
```

El orquestador intenta OpenAlex primero. Scopus es condicional a `SCOPUS_QUERY`; no se debe interpretar que cada ejecución consulta necesariamente ambas fuentes. La carga a Silver procesa lotes terminados y aún no registrados como procesados.

## Documentos

- [Ingesta y selección de fuentes](ingesta.md): consultas, paginación, checkpoints y límites.
- [Filtrado y modelo Silver](bronze-filtrado-silver.md): validaciones, rechazos, duplicados y campos resultantes.
- [Operación y configuración](operacion.md): ejecución, rutas, variables y discrepancias de configuración documentada.

## Fuentes de implementación

El recorrido se implementa principalmente en `src/run_pipeline.py`, `src/openalex_ingest.py`, `src/scopus_ingest.py` y `src/spark_pipeline.py`. La configuración de contenedores está en `compose.yaml` y la planificación en `scheduler/pipeline.cron`.
