# Contexto general y objetivo del proyecto

## 1. Título del proyecto

**Detección predictiva de tendencias emergentes de investigación en Inteligencia Artificial aplicando CRISP-DM**

## 2. Contexto general

El proyecto se desarrolla en el ámbito de **Big Data, Ciencia de Datos y Machine Learning**, con el propósito de analizar la evolución de la investigación científica en Inteligencia Artificial y anticipar qué líneas o tópicos de investigación tienen mayor probabilidad de convertirse en tendencias emergentes.

El enfoque actual del proyecto ya no se limita a identificar tópicos o describir cómo evolucionan en el tiempo. El problema se formula como una tarea de **predicción supervisada**, en la que se construirá un dataset histórico de publicaciones científicas y se utilizará información disponible hasta un momento determinado para predecir el comportamiento futuro de cada tópico.

La unidad de análisis será un **tópico de investigación observado en un periodo temporal determinado**. Para cada tópico se generarán características basadas únicamente en información histórica disponible hasta un año base `t`, evitando utilizar información del futuro durante el entrenamiento.

Entre las variables candidatas se consideran:

- **Variables temporales:** número de publicaciones por año, tasa de crecimiento, aceleración, prevalencia y antigüedad del tópico.
- **Variables bibliométricas:** citas, promedio de citas, cantidad de autores, instituciones, países, colaboraciones y medios de publicación.
- **Variables semánticas:** términos relevantes, similitud con temas anteriores, aparición de conceptos nuevos, embeddings y representaciones derivadas mediante técnicas de NLP.
- **Variables contextuales:** año base, área temática, tipo de documento y otros metadatos disponibles.

Para identificar y representar las líneas temáticas podrán utilizarse técnicas de **Topic Modeling**, como BERTopic, LDA o STM. Estas técnicas no constituyen el modelo predictivo principal, sino que funcionan como apoyo para construir los tópicos y generar características que posteriormente serán utilizadas por los modelos de Machine Learning.

El proyecto contempla comparar diferentes algoritmos supervisados, entre ellos:

- Logistic Regression
- Decision Tree
- Random Forest
- XGBoost
- LightGBM

La selección del modelo no se realizará por reputación o por resultados reportados en otros trabajos. Cada modelo será evaluado experimentalmente sobre el dataset del proyecto utilizando una estrategia de validación temporal.

## 3. Problema de Machine Learning

El sistema buscará responder la siguiente pregunta:

> A partir de la información histórica disponible sobre un tópico de investigación, ¿es posible predecir si dicho tópico se convertirá en una tendencia emergente en un horizonte futuro?

La variable objetivo será inicialmente de tipo binario:

- `1`: el tópico se convirtió posteriormente en emergente.
- `0`: el tópico no se convirtió en emergente.

La regla exacta utilizada para determinar qué significa que un tópico sea "emergente" todavía debe definirse y justificarse científicamente. Esta etiqueta deberá construirse observando el comportamiento posterior de tópicos históricos, considerando criterios reproducibles como crecimiento sostenido, aceleración, incremento de prevalencia u otros indicadores validados.

## 4. Objetivo general

**Desarrollar y evaluar modelos de Machine Learning capaces de predecir tendencias emergentes de investigación en Inteligencia Artificial a partir de información histórica de literatura científica, aplicando una metodología CRISP-DM adaptada y seleccionando experimentalmente el modelo con mejor desempeño predictivo.**

## 5. Objetivos específicos

1. Construir un corpus histórico de publicaciones científicas relacionadas con Inteligencia Artificial a partir de fuentes científicas documentadas y autorizadas.

2. Diseñar un proceso de ingestión, almacenamiento y actualización de datos que permita incrementar progresivamente el volumen del dataset.

3. Preparar y depurar los datos mediante procesos de limpieza, normalización, deduplicación y control de calidad.

4. Identificar y representar líneas temáticas de investigación mediante técnicas de NLP y Topic Modeling.

5. Construir variables temporales, bibliométricas, semánticas y contextuales que representen la evolución histórica de cada tópico.

6. Definir una regla reproducible para etiquetar históricamente los tópicos como emergentes o no emergentes.

7. Entrenar y comparar diferentes modelos de Machine Learning supervisado bajo las mismas condiciones experimentales.

8. Evaluar los modelos utilizando particiones temporales y métricas como Precision, Recall, F1-score, ROC-AUC, PR-AUC y matriz de confusión.

9. Seleccionar el modelo con el desempeño predictivo más sólido, considerando también estabilidad, interpretabilidad y costo de error.

10. Integrar el modelo seleccionado en un Sistema de Soporte a Decisiones (DSS) que permita visualizar probabilidades de emergencia, rankings, evolución temporal e indicadores relevantes.

## 6. Metodología

El proyecto seguirá una adaptación de **CRISP-DM**, manteniendo sus seis fases principales:

1. **Comprensión del problema:** definir qué se considera una tendencia emergente, el horizonte de predicción y el objetivo analítico.
2. **Comprensión de los datos:** explorar el corpus científico, su cobertura temporal, calidad, sesgos y metadatos disponibles.
3. **Preparación de los datos:** limpieza, normalización, NLP, Topic Modeling, Feature Engineering y construcción de la variable objetivo.
4. **Modelado:** entrenamiento y ajuste de diferentes algoritmos de Machine Learning supervisado.
5. **Evaluación:** comparación de modelos mediante validación temporal y métricas de clasificación.
6. **Despliegue:** integración del modelo seleccionado en un DSS para apoyar la interpretación y priorización de tendencias.

La metodología será iterativa. Los resultados obtenidos durante el modelado y la evaluación podrán obligar a revisar la definición del target, las variables utilizadas o las etapas de preparación de datos.

## 7. Pipeline general

```text
Fuentes científicas
        |
        v
Extracción e ingesta
        |
        v
Calidad y almacenamiento de datos
        |
        v
NLP y Topic Modeling
        |
        v
Identificación de tópicos
        |
        v
Feature Engineering
        |
        v
Construcción del target histórico
        |
        v
Separación temporal
        |
        v
Entrenamiento de modelos ML
        |
        v
Evaluación y comparación
        |
        v
Selección del modelo
        |
        v
Sistema de Soporte a Decisiones
```

## 8. Estrategia de evaluación

La separación entre entrenamiento, validación y prueba deberá respetar el orden cronológico de los datos. No se utilizará un split aleatorio que mezcle registros del pasado y del futuro, debido al riesgo de generar fuga de información temporal.

De forma conceptual:

- **Entrenamiento:** periodos históricos más antiguos.
- **Validación:** periodos posteriores utilizados para ajuste de hiperparámetros.
- **Prueba:** periodos futuros no utilizados durante el entrenamiento ni el ajuste.

La selección del modelo se basará en múltiples métricas y no exclusivamente en Accuracy, debido a que la clase "emergente" podría encontrarse desbalanceada.

## 9. Sistema de Soporte a Decisiones

El resultado final del proyecto no será únicamente un modelo de Machine Learning. El modelo seleccionado será integrado en un **Sistema de Soporte a Decisiones (DSS)**.

El DSS deberá permitir:

- mostrar la probabilidad estimada de emergencia de cada tópico;
- clasificar tópicos como emergentes o no emergentes según un umbral documentado;
- generar rankings de tendencias candidatas;
- mostrar su evolución temporal;
- presentar indicadores bibliométricos y semánticos;
- explicar, cuando sea posible, qué variables influyeron en la predicción;
- apoyar a investigadores, grupos de investigación, universidades y centros de I+D en la priorización de líneas de estudio.

El DSS apoyará la toma de decisiones, pero no sustituirá el criterio humano.

## 10. Alcance actual del proyecto

Actualmente se encuentra definido que:

- el proyecto tendrá un enfoque predictivo supervisado;
- se construirá un dataset histórico de investigación en IA;
- se utilizarán variables temporales, bibliométricas y semánticas;
- se compararán varios modelos de Machine Learning;
- la evaluación será temporal;
- el modelo seleccionado será integrado en un DSS;
- el proceso metodológico seguirá CRISP-DM adaptada.

Todavía deben validarse experimentalmente:

- la definición exacta de tendencia emergente;
- el horizonte de predicción;
- la fuente o combinación definitiva de fuentes científicas;
- el método final de identificación de tópicos;
- las características definitivas del modelo;
- la estrategia frente al posible desbalance de clases;
- la métrica principal para seleccionar el modelo final;
- la tecnología concreta del DSS.

## 11. Síntesis del proyecto

El proyecto busca transformar literatura científica histórica sobre Inteligencia Artificial en un dataset estructurado que permita estudiar la evolución de las líneas de investigación y entrenar modelos capaces de anticipar cuáles podrían convertirse en tendencias emergentes.

La estructura conceptual general es:

```text
Big Data
    -> NLP / Topic Modeling
    -> Feature Engineering
    -> Machine Learning
    -> Predicción
    -> DSS
```

El aporte principal del proyecto consiste en combinar análisis histórico, procesamiento de lenguaje natural, variables bibliométricas y modelos predictivos para generar evidencia cuantitativa que apoye la identificación anticipada de nuevas líneas de investigación en Inteligencia Artificial.
