# Ingesta de OpenAlex y Scopus

El pipeline puede consultar ambas fuentes, pero no lo hace siempre: OpenAlex se intenta en cada ejecución; Scopus solo se ejecuta cuando existe una consulta no vacía en `SCOPUS_QUERY`.

## Selección en el orquestador

`src/run_pipeline.py` ejecuta OpenAlex y captura `OpenAlexRequestError` como estado de la ejecución. Luego evalúa Scopus:

| Configuración | Acción Scopus |
|---|---|
| `SCOPUS_QUERY` vacía o ausente | Se omite |
| Consulta presente y sin el par completo `SCOPUS_FIRST_YEAR` / `SCOPUS_LAST_YEAR` | Ingesta ordinaria |
| Consulta y ambos años presentes | Ingesta particionada por año |

La partición anual restringe cada consulta mediante `PUBYEAR`, para separar los intervalos solicitados. Configurar solo uno de los años no activa esa modalidad.

## Ciclo de los lotes

Las respuestas originales se escriben por páginas JSON bajo `bronze/openalex/<batch-id>/` o `bronze/scopus/<batch-id>/`. El estado compartido se guarda bajo `bronze/batch_metadata`; el almacén coordina los lotes y bloqueos. Spark toma únicamente lotes completos con `page_count > 0`.

La ingesta puede reanudar lotes en curso. OpenAlex recorre páginas mediante cursor; Scopus pagina mediante offset. Los metadatos se actualizan de forma atómica y no deben contener el token. Esto permite distinguir datos recibidos de lotes listos para procesar sin borrar la evidencia Bronze.

## OpenAlex

La autenticación usa `OPENALEX_TOKEN`. El tamaño de página no supera 200. El valor predeterminado del objetivo inicial en el módulo es 100 000 registros; el objetivo incremental predeterminado es 1 000. Reintentos, control de ritmo y bloqueo forman parte de la ingesta. Los valores efectivos pueden depender de la configuración de ejecución.

## Scopus

La API key se configura como `SCOPUS_API`; la consulta base, en `SCOPUS_QUERY`. El objetivo inicial predeterminado es 100 000, el incremental 1 000, el tamaño de página 25 y la ventana predeterminada de resultados 5 000. La paginación usa offset.

La ventana/límite de resultados de Scopus restringe lo recuperable; el pipeline no garantiza exhaustividad fuera de ella. Los estados devueltos por la API también afectan el resultado de la ingesta: un lote fallido no debe presentarse como carga completa. Revisar estado y registros de la ejecución ante errores o resultados parciales.

## Continuación

Para las rutas y variables completas, consultar [Operación y configuración](operacion.md). Las reglas con que Spark acepta o rechaza cada registro se describen en [Filtrado y modelo Silver](bronze-filtrado-silver.md).
