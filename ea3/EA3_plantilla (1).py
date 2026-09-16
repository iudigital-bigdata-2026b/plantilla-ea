# Databricks notebook source
# MAGIC %md
# MAGIC # EA3 — Proyecto final: procesamiento distribuido y cierre del caso
# MAGIC
# MAGIC **Big Data (ISD-25)** · Ingeniería de Software y Datos · IU Digital de Antioquia
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Grupo** | *(número)* |
# MAGIC | **Integrantes** | *(nombre 1) · (nombre 2) · (nombre 3)* |
# MAGIC | **Caso de estudio** | *(el mismo de la EA1)* |
# MAGIC | **Fecha de entrega** | Domingo 4 de octubre |
# MAGIC | **🎥 Enlace al video** | *(pegar aquí — 8 a 10 minutos, mínimo 3 por integrante)* |
# MAGIC
# MAGIC > ⚠️ **Antes de entregar:** verificar que el enlace del video abra desde una cuenta distinta
# MAGIC > a la propia. Un enlace inaccesible se califica como no entregado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo usar esta plantilla
# MAGIC
# MAGIC Las celdas en *cursiva* son instrucciones: **reemplácenlas por su contenido.**
# MAGIC
# MAGIC | Sección | Qué va ahí | Puntos |
# MAGIC |---|---|---|
# MAGIC | 1 · Contexto | De dónde vienen, en una página | — |
# MAGIC | 2 · Capa oro | La tabla de consumo y el recorrido del dato | 20 |
# MAGIC | 3 · Medición inicial | El plan de ejecución y los tres tiempos | 15 |
# MAGIC | 4 · Optimización | La técnica, el plan nuevo y la comparación | 20 |
# MAGIC | 5 · Límites | Cuándo esa técnica sería contraproducente | 10 |
# MAGIC | 6 · Cierre del caso | La pregunta orientadora y las conclusiones | 10 |
# MAGIC | 7 · Reparto y uso de IA | Quién hizo qué | — |
# MAGIC | ⭐ Opcional | Tablero o Genie sobre la capa oro | +5 |
# MAGIC
# MAGIC Los 25 puntos restantes son individuales y se evalúan en el video.

# COMMAND ----------

from pyspark.sql import functions as F
import time, statistics

# ⬇️ CAMBIAR por su catálogo y esquema de la EA1
CATALOGO = "bigdata_grupoNN"
ESQUEMA  = "wanderbricks"

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

print(f"Trabajando en {CATALOGO}.{ESQUEMA}\n")
display(spark.sql(f"SHOW TABLES IN {CATALOGO}.{ESQUEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 1 · Contexto
# MAGIC
# MAGIC *En un par de párrafos: cuál es su caso, qué construyeron en la EA1 y la EA2, y cuál es la
# MAGIC pregunta de negocio que va a responder la capa oro de este proyecto.*
# MAGIC
# MAGIC *No repitan la EA1 completa: basta con que quien lea esto entienda de dónde vienen.*

# COMMAND ----------

# MAGIC %md
# MAGIC # 2 · Capa oro · 20 puntos
# MAGIC
# MAGIC ## 2.1 · Qué pregunta responde
# MAGIC
# MAGIC *Escriban aquí la pregunta de negocio concreta que responde esta tabla, y quién la usaría.*
# MAGIC
# MAGIC *Recuerden la prueba: si alguien del negocio abre esta tabla, ¿entiende lo que ve sin
# MAGIC explicación? Si necesita procesarla más, todavía es capa plata.*

# COMMAND ----------

# --- Construcción de la capa oro
# TODO: su código


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 · La tabla resultante

# COMMAND ----------

# TODO: display() de su capa oro


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 · El recorrido del dato
# MAGIC
# MAGIC *Cuántos registros hay en cada capa y a qué corresponde cada diferencia. Si algo se
# MAGIC descartó, digan por qué.*

# COMMAND ----------

# Conteo por capa — ajusten los nombres de sus tablas
for capa, tabla in [("bronce", "TABLA_BRONCE"), ("plata", "TABLA_PLATA"), ("oro", "TABLA_ORO")]:
    try:
        n = spark.table(f"{CATALOGO}.{ESQUEMA}.{tabla}").count()
        print(f"{capa:<8}{tabla:<28}{n:>12,} registros")
    except Exception as e:
        print(f"{capa:<8}{tabla:<28}  ⚠️ ajustar el nombre")

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretación:** *¿a qué corresponde cada diferencia entre capas?*

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 · Medición inicial · 15 puntos
# MAGIC
# MAGIC ## 3.1 · La operación que vamos a optimizar
# MAGIC
# MAGIC *Qué consulta eligieron y por qué creen que es costosa. Debe ser un cruce entre dos tablas
# MAGIC que tarde algo medible: si responde en milisegundos, la mejora no se va a distinguir del ruido.*

# COMMAND ----------

# Función de medición — úsenla tal cual, no la modifiquen
def medir(descripcion, funcion, repeticiones=3):
    """Calienta, repite y reporta la mediana. Las tres mediciones quedan visibles."""
    funcion()                                   # calentamiento, se descarta
    tiempos = []
    for _ in range(repeticiones):
        inicio = time.time()
        funcion()
        tiempos.append(time.time() - inicio)

    mediana = statistics.median(tiempos)
    print(f"{descripcion}")
    print(f"   mediciones : {', '.join(f'{t:.2f}s' for t in tiempos)}")
    print(f"   MEDIANA    : {mediana:.2f}s")
    print(f"   variación  : {max(tiempos)-min(tiempos):.2f}s\n")
    return mediana

# COMMAND ----------

# --- Su consulta ANTES de optimizar
consulta_original = None   # TODO: definir su consulta aquí


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 · El plan de ejecución inicial
# MAGIC
# MAGIC *Ejecuten `.explain()` y dejen la salida visible. Después señalen dónde está el costo.*

# COMMAND ----------

# TODO: consulta_original.explain()


# COMMAND ----------

# MAGIC %md
# MAGIC **Qué vemos en el plan:**
# MAGIC
# MAGIC *¿Qué estrategia eligió Spark? ¿Dónde aparece un `Exchange`? ¿Qué se está moviendo entre
# MAGIC máquinas y por qué eso es caro?*

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 · Los tres tiempos

# COMMAND ----------

# TODO
# tiempo_antes = medir("Consulta original", lambda: consulta_original.collect())


# COMMAND ----------

# MAGIC %md
# MAGIC # 4 · Optimización · 20 puntos
# MAGIC
# MAGIC ## 4.1 · Qué técnica aplicamos y por qué
# MAGIC
# MAGIC *Una sola técnica, bien elegida. Expliquen por qué esa y no otra, en función de lo que
# MAGIC vieron en el plan inicial.*

# COMMAND ----------

# --- Su consulta DESPUÉS de optimizar
consulta_optimizada = None   # TODO


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 · El plan de ejecución resultante

# COMMAND ----------

# TODO: consulta_optimizada.explain()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 · Qué cambió entre los dos planes
# MAGIC
# MAGIC *Comparen los dos planes de forma explícita. No basta con poner los dos números: hay que
# MAGIC decir qué operación desapareció o se redujo y por qué eso baja el costo.*
# MAGIC
# MAGIC | | Antes | Después |
# MAGIC |---|---|---|
# MAGIC | Estrategia de cruce | | |
# MAGIC | Movimiento de datos | | |
# MAGIC | Ordenamiento | | |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 · Los tiempos, comparados

# COMMAND ----------

# TODO
# tiempo_despues = medir("Consulta optimizada", lambda: consulta_optimizada.collect())
#
# mejora = (tiempo_antes - tiempo_despues) / tiempo_antes * 100
# print("═" * 48)
# print(f"{'Antes':<24}{tiempo_antes:>8.2f}s")
# print(f"{'Después':<24}{tiempo_despues:>8.2f}s")
# print(f"{'Mejora':<24}{mejora:>8.1f}%")
# print("═" * 48)


# COMMAND ----------

# MAGIC %md
# MAGIC **Conclusión de la optimización:**
# MAGIC
# MAGIC *Qué hacía antes, qué cambió en el plan, y el número. En ese orden.*
# MAGIC
# MAGIC > 💡 **Si no mejoró nada, repórtenlo igual** y expliquen por qué: volumen insuficiente,
# MAGIC > Spark ya lo estaba resolviendo solo, u otra razón. Una medición honesta vale más que un
# MAGIC > número inflado.

# COMMAND ----------

# MAGIC %md
# MAGIC # 5 · Límites de la optimización · 10 puntos
# MAGIC
# MAGIC *¿En qué condiciones esta misma técnica dejaría de servir, o incluso empeoraría el resultado?*
# MAGIC
# MAGIC *Toda técnica tiene un rango donde ayuda y otro donde estorba. Identificar ese límite es lo
# MAGIC que demuestra que entendieron el mecanismo y no solo el procedimiento.*
# MAGIC
# MAGIC *Ata su respuesta a su caso: qué tendría que cambiar en sus datos para que esta optimización
# MAGIC dejara de tener sentido.*

# COMMAND ----------

# MAGIC %md
# MAGIC # 6 · Cierre del caso · 10 puntos
# MAGIC
# MAGIC ## 6.1 · La pregunta orientadora
# MAGIC
# MAGIC > Una empresa recibe datos de su portal web, de Facebook, de Instagram, de TikTok y de
# MAGIC > WhatsApp Business. **¿Bajo qué paradigma, con qué herramientas y con qué arquitectura
# MAGIC > debería analizarlos?**
# MAGIC
# MAGIC *Un par de párrafos, no un ensayo. La respuesta debe apoyarse en lo que construyeron
# MAGIC durante el curso, no en generalidades.*

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 · Conclusiones del proyecto
# MAGIC
# MAGIC *Qué funcionó, qué no, y qué harían distinto si empezaran de nuevo. Deben derivarse de sus
# MAGIC resultados, no de impresiones generales sobre la herramienta.*

# COMMAND ----------

# MAGIC %md
# MAGIC # 7 · Reparto del trabajo y uso de IA
# MAGIC
# MAGIC | Integrante | De qué se encargó | Qué sustenta en el video |
# MAGIC |---|---|---|
# MAGIC | *(nombre 1)* | | |
# MAGIC | *(nombre 2)* | | |
# MAGIC | *(nombre 3)* | | |
# MAGIC
# MAGIC **Uso de asistentes de IA:**
# MAGIC
# MAGIC *Declaren dónde los usaron. Está permitido; lo que se evalúa es que puedan explicar
# MAGIC cualquier línea del código en el video.*

# COMMAND ----------

# MAGIC %md
# MAGIC # ⭐ Opcional · hasta 5 puntos adicionales
# MAGIC
# MAGIC *Elijan **una** de las dos opciones. Es opcional: no hacerlo no resta nada.*
# MAGIC
# MAGIC ### Opción A · Un tablero sobre la capa oro
# MAGIC
# MAGIC *Tres visualizaciones que respondan la pregunta de negocio de su capa oro. Peguen la captura
# MAGIC aquí.*
# MAGIC
# MAGIC ### Opción B · Un espacio de Genie sobre la capa oro
# MAGIC
# MAGIC *Tres preguntas en lenguaje natural con sus respuestas, y **qué tuvieron que ajustar** para
# MAGIC que Genie entendiera: nombres de columnas, descripciones, instrucciones del espacio.*
# MAGIC
# MAGIC *Eso último es lo más interesante del ejercicio: si Genie no entiende su tabla, es porque la
# MAGIC tabla no está bien diseñada.*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ✅ Antes de entregar
# MAGIC
# MAGIC - [ ] El notebook corre completo de arriba abajo sin errores
# MAGIC - [ ] La capa oro existe y sus columnas se entienden sin explicación
# MAGIC - [ ] Las **tres mediciones** están visibles, antes y después, con la mediana
# MAGIC - [ ] Los **dos planes de ejecución** están en el notebook, con la salida visible
# MAGIC - [ ] Está escrito por qué mejoró y cuándo la técnica sería contraproducente
# MAGIC - [ ] La pregunta orientadora está respondida, apoyada en lo que construyeron
# MAGIC - [ ] No quedaron textos en cursiva de esta plantilla sin reemplazar
# MAGIC - [ ] La sección 7 coincide con lo que cada uno sustenta en el video
# MAGIC - [ ] El enlace del video está en la portada y abre desde otra cuenta
# MAGIC - [ ] El `.ipynb` está confirmado en el repositorio, en `/ea3`
# MAGIC - [ ] El HTML con salidas visibles está subido a Canvas
# MAGIC
# MAGIC ### ⚠️ Sobre la cuota
# MAGIC
# MAGIC Este proyecto pide ejecutar la misma consulta **ocho veces** —calentamiento y tres
# MAGIC mediciones, dos veces—. Eso gasta bastante más de lo habitual.
# MAGIC **No dejen las mediciones para la última noche.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Entrega: domingo 4 de octubre, 11:59 p. m.**
