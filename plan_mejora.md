# Plan de mejora: Scopus, OpenAlex y archivos Parquet

## Objetivo

Incorporar Scopus al pipeline que actualmente ingiere OpenAlex, reducir la cantidad de archivos Parquet generados por procesamiento y distinguir el fin real de una búsqueda de una interrupción por límites o errores de API.

**Alcance:** Scopus y OpenAlex. La prioridad es reducir la cantidad de archivos, no el tamaño total en bytes.

## Situación actual

- `src/openalex_ingest.py` consulta OpenAlex por cursor, solicita hasta 200 trabajos por página y guarda las respuestas completas en Bronze.
- `src/spark_pipeline.py` transforma los lotes completados y escribe Silver en Parquet con Snappy, en un directorio por lote. La escritura no controla explícitamente las particiones de salida; actualmente se observan unos 5–6 archivos Parquet por procesamiento.
- El manejo de errores de OpenAlex informa fallos HTTP, pero no distingue cuota agotada, exceso de solicitudes y otros errores. Una respuesta vacía antes de alcanzar el objetivo se trata como error.

## Reglas para la ingesta y sus estados

| Condición | Señal | Estado y acción |
|---|---|---|
| Objetivo del lote alcanzado | Se obtuvo la cantidad configurada de registros | `completed_target`. Guardar el cursor siguiente. Completar un lote no significa que se agotaron todos los resultados. |
| Fin de resultados | Señal documentada de fin de paginación; en OpenAlex, cursor siguiente nulo y resultados vacíos | `completed_exhausted`. Registrar los registros obtenidos, aunque sean menos que el objetivo. |
| Cuota de Scopus agotada | HTTP 429 con `X-ELS-Status: QUOTA_EXCEEDED` | `quota_exceeded`. Guardar progreso y programar reanudación según `X-RateLimit-Reset`. |
| Ritmo de solicitudes excedido | HTTP 429 sin señal inequívoca de cuota agotada | `rate_limited`. Reintentar de forma acotada, respetando `Retry-After` cuando exista. Si no puede clasificarse, registrar el 429 como límite no determinado; no asumir su causa. |
| Créditos de OpenAlex próximos a agotarse | Consulta a `/rate-limit` | Pausar preventivamente y registrar créditos restantes y hora de reinicio, sin persistir la API key. |
| Tope de resultados paginables | La consulta llega a su ventana máxima sin demostrar que el conjunto terminó | `result_window_reached`. No declararla completa; subdividir la consulta o usar una paginación que permita continuar. |
| Error transitorio | Timeout, fallo de conexión o HTTP 5xx | Reintentos acotados con espera incremental; después, `transient_error` recuperable. |
| Error permanente | Credenciales inválidas, falta de acceso, parámetros incorrectos o respuesta inválida | `failed`. Informar la causa sin reintentos automáticos indefinidos. |

**Regla de integridad:** avanzar el cursor u offset únicamente después de guardar correctamente la página y actualizar los metadatos del lote. Reanudar un lote no debe duplicar sus registros.

## Tareas de implementación

### 1. Establecer una línea base

- [ ] Medir por lote: fuente, registros válidos y rechazados, cantidad de `part-*.parquet`, tamaño total y duración de escritura.
- [ ] Separar mediciones de carga inicial e incremental.
- [ ] Guardar el procedimiento de medición para comparar el resultado antes y después de los cambios.

**Criterios de aceptación**

- Se puede reproducir el conteo actual de archivos por lote.
- La línea base incluye el volumen de filas y tiempo de procesamiento junto con el número de archivos.

### 2. Definir el contrato de fuentes y estados

- [ ] Definir una interfaz de ingesta común que permita a OpenAlex y Scopus devolver páginas, posición siguiente y metadatos de respuesta, manteniendo separadas las particularidades de cada API.
- [ ] Representar explícitamente estado y razón de detención del lote.
- [ ] Guardar en metadatos: fuente, consulta o partición de búsqueda, lote, páginas, registros, cursor u offset, estado, código HTTP si corresponde y momento de próximo intento.
- [ ] Excluir API keys, URLs con credenciales y cuerpos sensibles de logs y metadatos.

**Criterios de aceptación**

- Para cada lote se puede determinar si terminó, quedó pausado o falló.
- Los metadatos permiten identificar desde dónde reanudar sin revelar secretos.

### 3. Completar la detección en OpenAlex

- [ ] Distinguir el objetivo del lote de la condición de agotamiento del cursor.
- [ ] Reconocer el fin documentado de resultados sin convertirlo automáticamente en fallo por no alcanzar el objetivo.
- [ ] Consultar `/rate-limit` para decidir pausas preventivas cuando corresponda.
- [ ] Clasificar HTTP 429, HTTP 5xx, timeout, errores de autenticación y respuestas inválidas.
- [ ] Probar la reanudación desde el último cursor confirmado.

**Criterios de aceptación**

- Una fuente agotada con menos registros que el objetivo queda marcada `completed_exhausted`.
- Una cuota agotada o un fallo temporal nunca queda marcado como ingesta completa.
- Una reanudación continúa desde el último cursor persistido.

### 4. Incorporar Scopus

- [ ] Crear un cliente Scopus independiente, con credenciales obtenidas del entorno.
- [ ] Implementar paginación, control de solicitudes por segundo y seguimiento de cuota para los endpoints seleccionados.
- [ ] Leer `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset` y `X-ELS-Status` cuando estén disponibles.
- [ ] Detectar el máximo de resultados de una consulta y planificar subdivisiones sin solapamientos no controlados.
- [ ] Guardar respuestas originales en Bronze por fuente y lote, junto con la posición necesaria para reanudar.

**Criterios de aceptación**

- Scopus puede detenerse por cuota, ritmo de solicitudes o ventana de resultados y continuar sin perder páginas ya confirmadas.
- Las credenciales no aparecen en logs, excepciones ni datos persistidos.
- Las consultas subdivididas no omiten ni duplican rangos deliberadamente.

### 5. Normalizar y deduplicar

- [ ] Definir un esquema Silver compartido con procedencia (`source`, ID original y lote).
- [ ] Mapear por separado los campos de Scopus y OpenAlex; conservar los originales en Bronze para reprocesamiento.
- [ ] Normalizar el DOI antes de usarlo como clave entre fuentes.
- [ ] Mantener los registros de ambas fuentes y definir cuál será la vista deduplicada para análisis.
- [ ] Registrar los casos sin DOI o con DOI conflictivo; no descartar registros por coincidencia aproximada de título sin una regla validada.

**Criterios de aceptación**

- Los análisis pueden obtener publicaciones únicas sin perder la evidencia de qué fuente aportó cada registro.
- Los registros sin identificador compartido permanecen disponibles y trazables.

### 6. Reducir archivos Parquet por lote

- [ ] Controlar las particiones antes de escribir Silver: un archivo para lotes pequeños, incluidos los incrementales actuales.
- [ ] Definir un umbral medido para permitir varios archivos en lotes grandes; no forzar toda la carga inicial a una única partición.
- [ ] Mantener inicialmente Snappy y medir el efecto de los cambios en duración de escritura y lectura.
- [ ] Contar `part-*.parquet` como archivos de datos; no confundirlos con archivos auxiliares de Spark.

**Criterios de aceptación**

- Un lote incremental pequeño produce un `part-*.parquet` sin pérdida de filas.
- Los lotes grandes no son forzados a una única partición si eso degrada de forma inaceptable el procesamiento.
- La comparación con la línea base muestra menos archivos por procesamiento.

### 7. Compactar el histórico

- [ ] Diseñar una tarea de compactación independiente de la ingesta para agrupar archivos pequeños acumulados entre lotes.
- [ ] Elegir agrupaciones según las consultas reales —por ejemplo, fuente y año— evitando particiones con pocos registros.
- [ ] Escribir a una ubicación temporal, validar conteo y esquema, y publicar el resultado solo después de verificarlo.
- [ ] Actualizar las referencias que usa el procesamiento para que no lea simultáneamente archivos originales y compactados.

**Criterios de aceptación**

- Ejecutar la compactación más de una vez no duplica filas.
- Después de publicar una compactación, los lectores ven una sola versión coherente de cada conjunto.
- La compactación fallida deja disponibles los datos previos.

### 8. Verificar el flujo completo

- [ ] Probar por fuente: fin normal, objetivo alcanzado, 429 por cuota, 429 por ritmo, HTTP 5xx, timeout, error permanente y respuesta inválida.
- [ ] Probar interrupciones antes y después de persistir una página.
- [ ] Probar el límite de ventana de Scopus y la continuación mediante subdivisión.
- [ ] Comparar registros Bronze y Silver, publicaciones únicas y conteo de filas antes y después de compactar.
- [ ] Ejecutar las pruebas del proyecto y documentar las mediciones finales de archivos por lote.

**Criterios de aceptación**

- Ninguna detención parcial se informa como finalización.
- La reanudación no pierde ni duplica registros confirmados.
- Los lotes pequeños generan un archivo Parquet de datos.
- La compactación preserva el esquema y el conteo esperado.

## Riesgos y mitigaciones

| Riesgo | Mitigación |
|---|---|
| Confundir el objetivo de un lote con el fin de resultados | Usar estados distintos y conservar el cursor siguiente. |
| Clasificar cualquier HTTP 429 como cuota agotada | Examinar encabezados de la API; registrar causa indeterminada cuando falte evidencia. |
| Duplicar o saltar páginas tras una caída | Persistir la página y su punto de control de forma coordinada; reanudar desde el último punto confirmado. |
| Perder trazabilidad al deduplicar Scopus y OpenAlex | Mantener fuente e identificador original y separar registros de origen de la vista unificada. |
| Crear un cuello de botella con un solo Parquet | Aplicar un archivo solo a lotes pequeños y medir lotes grandes. |
| Duplicar datos al compactar | Validar y publicar de forma controlada; hacer la operación reejecutable. |
| Cambios en límites o acceso de las APIs | Leer encabezados efectivos y registrar respuestas; no depender únicamente de valores fijos. |

## Orden recomendado

1. Medición inicial y contrato de estados.
2. Detección y recuperación en OpenAlex.
3. Cliente Scopus y almacenamiento Bronze.
4. Esquema Silver compartido y deduplicación.
5. Control de archivos Parquet por lote.
6. Compactación histórica.
7. Pruebas integrales y comparación con la línea base.

## Referencias

- [Límites y encabezados de Elsevier](https://dev.elsevier.com/api_key_settings.html)
- [Límites y autenticación de OpenAlex](https://docs.openalex.org/how-to-use-the-api/rate-limits-and-authentication)
- [Paginación de OpenAlex](https://docs.openalex.org/how-to-use-the-api/get-lists-of-entities/paging)
