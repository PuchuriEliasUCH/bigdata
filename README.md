# Pipeline OpenAlex Bronze-to-Silver

Pipeline batch Dockerizado que consulta trabajos de OpenAlex mediante cursores, conserva las respuestas crudas en Bronze, valida y deduplica los registros con PySpark, separa los rechazos y escribe las filas válidas en Silver Parquet. El mismo servicio expone un notebook de Jupyter para inspeccionar la evidencia generada.

## Inicio rápido

### Requisitos

- Docker Engine con el complemento Docker Compose (`docker compose`).
- Conectividad HTTPS al API de OpenAlex.
- Un token de OpenAlex y el puerto local `8888` disponible.

El contenedor incluye Python 3.11, Java 17, PySpark y JupyterLab; no es necesario instalar Spark en el host.

### Configuración segura

Compose carga el archivo `.env` del directorio raíz mediante `env_file`. El único valor requerido es `OPENALEX_TOKEN` y debe ser no vacío. Ejemplo seguro, sin credenciales reales:

```dotenv
# .env — local, no compartir ni versionar
OPENALEX_TOKEN=replace_with_your_openalex_token
```

Variables adicionales y sus valores efectivos:

| Variable | Requerida | Valor o comportamiento |
| --- | --- | --- |
| `OPENALEX_TOKEN` | Sí | Se envía al API; no se imprime ni se guarda en metadatos. |
| `BRONZE_PATH` | No | Compose la fija en `/data/bronze`. |
| `SILVER_PATH` | No | Compose la fija en `/data/silver`. |
| `TZ` | No | Compose la fija en `UTC`. |
| `SPARK_MASTER` | No | Usa `local[*]` si no se define. |

### Arranque

Desde la raíz del repositorio:

```bash
docker compose -f compose.yaml config --quiet
docker compose -f compose.yaml up --build
```

El primer comando valida la configuración sin imprimirla. El segundo construye la imagen, inicia el servicio `spark-pipeline`, mantiene JupyterLab disponible y ejecuta la ingesta/procesamiento según la programación de `scheduler/pipeline.cron` (cada dos minutos). La primera ingesta tiene un objetivo predeterminado de 200.000 registros, por lo que puede tardar.

Después de que exista una primera ejecución, abra:

`http://localhost:8888/lab/tree/notebooks/openalex_pipeline_evidence.ipynb`

## Arquitectura y flujo de datos

```text
OpenAlex Works API + OPENALEX_TOKEN
              |
              v
src/openalex_ingest.py
              |
              v
Bronze: data/bronze/openalex/<batch-id>/page-*.parquet
        data/bronze/batch_metadata/<batch-id>.json
              |
              v
src/spark_pipeline.py (validación, normalización y deduplicación)
       |                                      |
       v                                      v
Rejected en Bronze:                    Silver Parquet:
 data/bronze/rejected/                 data/silver/works/<batch-id>/
 <batch-id>/part-*.json                part-*.parquet
              \                                      /
               v                                    v
        data/silver/_processing/<batch-id>.json
                         |
                         v
        notebooks/openalex_pipeline_evidence.ipynb
```

- `src/run_pipeline.py` ejecuta una ingesta y luego procesa los lotes Bronze completados.
- La carga inicial sigue el cursor de OpenAlex hasta su objetivo de 200.000 registros. Una ejecución inicial interrumpida se reanuda como inicial.
- Una vez completada la carga inicial, cada ejecución incremental continúa desde el último cursor completado y está limitada a 1.000 registros.
- Spark conserva registros que no cumplen los requisitos o que son duplicados en `rejected`, con motivos auditables separados por `; `; no los descarta silenciosamente.
- Silver contiene campos normalizados de la obra, como identificador OpenAlex, título, fecha/año de publicación, temas, tipo, DOI, citas, acceso abierto y fuente.
- `_processing` registra el estado de procesamiento; los lotes completados se omiten en ejecuciones posteriores.

> **Alcance de Bronze.** Bronze **no es un histórico completo** ni garantiza un backfill total de OpenAlex: la carga inicial está acotada a 200.000 registros y las ejecuciones posteriores a 1.000 por ejecución. **Sí conserva incrementalmente** las páginas, metadatos y rechazos generados desde el momento en que comienza la ingesta en este entorno. Por tanto, la ausencia de un registro solo indica que no fue recolectado o procesado en las ejecuciones observadas; no demuestra que no exista en OpenAlex.

## Persistencia e inspección

Compose monta `./data` del host en `/data` dentro del contenedor. Por eso, `data/bronze` y `data/silver` sobreviven a la recreación del contenedor y pueden inspeccionarse directamente desde el host. `data/` está ignorado por Git.

Comandos operativos documentados:

```bash
# Seguir los conteos y estados no sensibles del pipeline
docker compose -f compose.yaml logs -f spark-pipeline

# Abrir una shell en el runtime si se necesita inspeccionar el contenedor
docker compose -f compose.yaml exec spark-pipeline sh
```

El notebook usa `/data/bronze` y `/data/silver`, es de solo lectura, informa cuando las zonas están vacías y nunca accede al token ni llama al API.

## Estructura relevante

```text
.
├── compose.yaml                         # Servicio, .env, montaje ./data y puerto 8888
├── Dockerfile                            # Python, Java, PySpark, JupyterLab y scheduler
├── requirements.txt                      # Dependencias fijadas
├── pyproject.toml                        # Configuración de pytest
├── docker/entrypoint.sh                  # Inicia JupyterLab y Supercronic
├── scheduler/pipeline.cron               # Ejecución cada dos minutos
├── src/
│   ├── openalex_ingest.py                # API, cursores y escritura Bronze
│   ├── spark_pipeline.py                 # Validación, rechazos y Silver
│   └── run_pipeline.py                   # Orquestación de una ejecución
├── notebooks/openalex_pipeline_evidence.ipynb
├── tests/                                 # Pruebas de ingesta, Spark y artefactos
└── data/                                 # Salida persistida y local; ignorada por Git
    ├── bronze/
    │   ├── openalex/<batch-id>/
    │   ├── batch_metadata/
    │   └── rejected/<batch-id>/
    └── silver/
        ├── works/<batch-id>/
        └── _processing/
```

## Pruebas y verificación

La configuración de `pyproject.toml` descubre `tests/`. Para ejecutar la suite dentro de la imagen:

```bash
docker compose -f compose.yaml run --rm spark-pipeline pytest -q
```
