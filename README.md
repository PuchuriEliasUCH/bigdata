# Detección de tópicos emergentes en IA: pipeline Bronze → Silver → Gold

Pipeline batch Dockerizado que descarga publicaciones de inteligencia artificial desde OpenAlex y Scopus,
conserva las respuestas crudas en Bronze, las valida y deduplica con PySpark en Silver, y construye en
Gold una tabla tópico × año con la etiqueta `target_emerging` para entrenar modelos que anticipen
tópicos emergentes. El mismo servicio expone JupyterLab con los notebooks de evidencia.

La documentación detallada está en [`documentacion/`](documentacion/0.README.md).

## Inicio rápido

### Requisitos

- Docker Engine con el complemento Docker Compose (`docker compose`).
- Conectividad HTTPS a las API de OpenAlex (y de Scopus, si se usa).
- Un token de OpenAlex y el puerto local `8888` disponible.

El contenedor incluye Python 3.11, Java 17, PySpark, JupyterLab y Supercronic; no es necesario instalar
Spark en el host.

### Configuración segura

Compose carga el archivo `.env` del directorio raíz. Ejemplo sin credenciales reales:

```dotenv
# .env — local, no compartir ni versionar
OPENALEX_TOKEN=replace_with_your_openalex_token
# Opcional: habilita Scopus
SCOPUS_API=replace_with_your_scopus_key
SCOPUS_QUERY=TITLE-ABS-KEY(...)
```

| Variable | Requerida | Comportamiento |
| --- | --- | --- |
| `OPENALEX_TOKEN` | Sí | Se envía a la API; no se imprime ni se guarda en metadatos. |
| `SCOPUS_API`, `SCOPUS_QUERY` | No | Scopus solo se ejecuta si `SCOPUS_QUERY` no está vacía. |
| `BRONZE_PATH`, `SILVER_PATH`, `GOLD_PATH` | No | Compose las fija en `/data/bronze`, `/data/silver` y `/data/gold`. |

El resto de variables (años, cuotas, presupuestos de tiempo) está en
[Operación y configuración](documentacion/5.operacion.md#configuración).

### Arranque

```bash
docker compose -f compose.yaml config --quiet
docker compose -f compose.yaml up --build
```

El primer comando valida la configuración sin imprimirla. El segundo construye la imagen, mantiene
JupyterLab en `http://localhost:8888` y ejecuta ingesta, Silver y compactación cada dos minutos
([`scheduler/pipeline.cron`](scheduler/pipeline.cron)). La carga inicial apunta a 100 000 registros por
fuente repartidos entre 2011 y el año actual; después, cada ejecución añade hasta 6 000 por fuente.

Gold se construye a mano cuando Silver está completo: ver
[Construir Gold](documentacion/5.operacion.md#construir-gold).

## Arquitectura y flujo de datos

```text
OpenAlex Works API ─┐                         src/ingestion.py, src/quotas.py
Scopus Search API ──┴─> ingesta por año ──>  src/openalex_ingest.py, src/scopus_ingest.py
                               │
                               v
Bronze  data/bronze/<fuente>/<lote>/page-*.parquet   respuestas crudas
        data/bronze/batch_metadata/<lote>.json       estado de cada lote
        data/bronze/plans/<fuente>.json              totales y cuotas por año
                               │  lotes terminales
                               v
Silver  src/spark_pipeline.py  reglas mínimas, deduplicación, indicadores de calidad (v4)
        data/silver/works/ · rejected/ · _processing/ · _compact/ (src/compaction.py)
                               │  universo OpenAlex comparable, 2011–2025
                               v
Gold    src/gold_pipeline.py   tabla densa tópico × año, variables sin fuga y etiqueta
        data/gold/year_totals/ · topic_year/ · _manifest.json
                               │
                               v
Modelos (próximo hito)
```

| Fase | Documento |
| --- | --- |
| Proceso general y cifras actuales | [0. Proceso general](documentacion/0.README.md) |
| Consultas, cuotas, lotes y paginación | [1. Ingesta](documentacion/1.ingesta.md) |
| Datos crudos y paso a Silver | [2. Bronze](documentacion/2.bronze.md) |
| Filtrado, deduplicación e indicadores | [3. Silver](documentacion/3.silver.md) |
| Etiqueta, variables y particiones | [4. Gold](documentacion/4.gold.md) |
| Comandos y comprobaciones | [5. Operación](documentacion/5.operacion.md) |
| Sesgos y límites del corpus | [6. Limitaciones](documentacion/6.limitaciones-muestreo.md) |

> **Alcance del corpus.** Bronze es una muestra acotada por cuotas, no un histórico completo de cada API,
> y dentro de cada año sigue el orden de la API (relevancia en OpenAlex), no un orden aleatorio. La
> ausencia de un registro no demuestra que no exista en la fuente. Ver
> [Limitaciones](documentacion/6.limitaciones-muestreo.md).

## Persistencia e inspección

Compose monta `./data` del host en `/data`, así que `data/bronze`, `data/silver` y `data/gold` sobreviven
a la recreación del contenedor. `data/` está ignorado por Git y sus archivos pertenecen a `root`
(los escribe el contenedor).

```bash
# Seguir los conteos y estados no sensibles del pipeline
docker compose -f compose.yaml logs -f spark-pipeline

# Abrir una shell en el contenedor
docker compose -f compose.yaml exec spark-pipeline sh
```

Notebooks de evidencia (solo lectura, no llaman a ninguna API ni leen credenciales):

- [`notebooks/openalex_pipeline_evidence.ipynb`](notebooks/openalex_pipeline_evidence.ipynb): reporte de
  calidad Bronze → Silver v4 (lotes, ventanas de Scopus, procesamiento, cobertura, indicadores,
  rechazos, fracción muestreada y solapamiento por DOI).
- [`notebooks/gold_evidence.ipynb`](notebooks/gold_evidence.ipynb): manifiesto de Gold, clases,
  sensibilidad de la etiqueta, particiones temporales, variables por clase y tópicos emergentes.

## Estructura relevante

```text
.
├── compose.yaml                 Servicio, .env, montajes y puerto 8888
├── Dockerfile                   Python, Java, PySpark, JupyterLab y Supercronic
├── docker/entrypoint.sh         Inicia JupyterLab y Supercronic
├── scheduler/pipeline.cron      Ejecución cada dos minutos
├── src/
│   ├── run_pipeline.py          Orquestación de una ejecución
│   ├── ingestion.py, quotas.py  Ingesta por año y cuotas proporcionales
│   ├── search_criteria.py       Términos de búsqueda comunes
│   ├── openalex_ingest.py       OpenAlex: cursor, estado de lotes y escritura Bronze
│   ├── scopus_ingest.py         Scopus: offset y ventanas de fecha de carga
│   ├── freeze_batches.py        Congelamiento de lotes pausados
│   ├── spark_pipeline.py        Silver: reglas, deduplicación y escritura
│   ├── silver_columns.py        Extracción de columnas por fuente
│   ├── silver_rules.py          Rechazos e indicadores de calidad
│   ├── compaction.py            Compactación y lectores activos de Silver
│   ├── gold_rules.py            Constantes y regla de la etiqueta
│   ├── gold_features.py         Variables de Gold
│   └── gold_pipeline.py         Construcción y publicación de Gold
├── notebooks/                   Evidencia ejecutada
├── documentacion/               Guías por fase
├── tests/                       Pruebas
└── data/                        bronze/, silver/, gold/ (local, ignorado por Git)
```

## Pruebas y verificación

`pyproject.toml` descubre `tests/`. Para ejecutar la suite dentro de la imagen:

```bash
docker compose -f compose.yaml run --rm --build --no-deps spark-pipeline pytest -q
```
