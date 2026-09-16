# Databricks notebook source
# MAGIC %md
# MAGIC # Ejemplo resuelto · Proyecto final (EA3)
# MAGIC ### Cómo se ve la entrega, de principio a fin
# MAGIC
# MAGIC **Big Data (ISD-25)**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Este notebook es **la EA3 resuelta completa**, sobre `samples.tpch` — el caso de
# MAGIC distribución mayorista que ya conocen. **No es Wanderbricks a propósito:** el método lo
# MAGIC aplican después a lo suyo.
# MAGIC
# MAGIC Sigue exactamente la estructura de la plantilla, así que pueden ir comparando sección
# MAGIC por sección con lo que les toca escribir.
# MAGIC
# MAGIC | Sección | Puntos | Qué verán aquí |
# MAGIC |---|---|---|
# MAGIC | 2 · Capa oro | 20 | Una tabla persistida, con comentarios en las columnas |
# MAGIC | 3 · Medición inicial | 15 | El plan y los tres tiempos |
# MAGIC | 4 · Optimización | 20 | La técnica, el plan nuevo y la comparación |
# MAGIC | 5 · Límites | 10 | Cuándo la técnica sería contraproducente |
# MAGIC | 6 · Cierre | 10 | La pregunta orientadora |
# MAGIC | ⭐ Opcional | +5 | Cómo configurar Genie y el tablero |
# MAGIC
# MAGIC > ⚠️ **No copien este caso.** Si en una entrega sobre Wanderbricks aparece un cruce con
# MAGIC > `supplier`, se nota de inmediato.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
import time, statistics, re

CATALOGO = "ejemplo_ea3"
ESQUEMA  = "abastecimiento"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ESQUEMA}")
spark.sql(f"USE SCHEMA {ESQUEMA}")

lineas = spark.table("samples.tpch.lineitem")
prov   = spark.table("samples.tpch.supplier")
paises = spark.table("samples.tpch.nation")

print(f"✅ {CATALOGO}.{ESQUEMA}")
print(f"   lineitem : {lineas.count():>12,} filas")
print(f"   supplier : {prov.count():>12,} filas")
print(f"   nation   : {paises.count():>12,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC # 1 · Contexto
# MAGIC
# MAGIC La empresa distribuye productos a clientes en varios países y trabaja con una red amplia de
# MAGIC proveedores. Cada pedido se descompone en líneas, y cada línea la surte un proveedor distinto.
# MAGIC
# MAGIC En la **EA1** modelamos el caso sobre un lakehouse y construimos las capas bronce y plata.
# MAGIC En la **EA2** organizamos el entorno en tres esquemas, aplicamos permisos diferenciados por
# MAGIC capa y automatizamos la ingesta con un Job de dos tareas.
# MAGIC
# MAGIC **La pregunta que responde la capa oro de este proyecto:**
# MAGIC
# MAGIC > *¿Qué proveedores incumplen las fechas que comprometen, con qué frecuencia, y cuánto gasto
# MAGIC > representan?*
# MAGIC
# MAGIC Es la pregunta que la directora de abastecimiento necesita para decidir a cuáles renegociar
# MAGIC el contrato el próximo trimestre.

# COMMAND ----------

# MAGIC %md
# MAGIC # 2 · Capa oro · 20 puntos
# MAGIC
# MAGIC ## 2.1 · Qué pregunta responde y quién la usa
# MAGIC
# MAGIC La usa **la directora de abastecimiento**, que tiene capacidad para renegociar unos veinte
# MAGIC contratos y necesita saber cuáles.
# MAGIC
# MAGIC Para que le sirva, la tabla tiene que cumplir tres cosas:
# MAGIC
# MAGIC | Requisito | Cómo se resuelve |
# MAGIC |---|---|
# MAGIC | Que entienda de quién habla | **Nombres**, no `s_suppkey` |
# MAGIC | Que no tenga que calcular nada | El porcentaje de incumplimiento **ya viene calculado** |
# MAGIC | Que lo importante esté arriba | Ordenada por incumplimiento, y filtrada a proveedores con historial suficiente |
# MAGIC
# MAGIC **La prueba:** si la abre y sabe qué hacer sin preguntarnos nada, es capa oro. Si necesita
# MAGIC procesarla más, todavía es plata.

# COMMAND ----------

# --- Construcción de la capa oro
oro = (lineas.alias("l")
    .join(broadcast(prov.alias("s")),   F.col("l.l_suppkey")   == F.col("s.s_suppkey"))
    .join(broadcast(paises.alias("n")), F.col("s.s_nationkey") == F.col("n.n_nationkey"))
    # --- Métricas derivadas: no existen en el origen
    .withColumn("entrega_tardia", F.col("l.l_receiptdate") > F.col("l.l_commitdate"))
    .withColumn("dias_retraso",   F.datediff("l.l_receiptdate", "l.l_commitdate"))
    .withColumn("monto_neto",     F.col("l.l_extendedprice") * (1 - F.col("l.l_discount")))
    .groupBy(
        F.trim(F.col("s.s_name")).alias("proveedor"),
        F.trim(F.col("n.n_name")).alias("pais"),
    )
    .agg(
        F.count("*").alias("entregas_totales"),
        F.sum(F.col("entrega_tardia").cast("int")).alias("entregas_tardias"),
        F.round(F.avg(F.when(F.col("entrega_tardia"), F.col("dias_retraso"))), 1).alias("dias_retraso_promedio"),
        F.round(F.sum("monto_neto"), 0).alias("gasto_total"),
        F.round(F.avg("l.l_discount") * 100, 2).alias("descuento_promedio_pct"),
    )
    # Filtro de análisis: sin historial suficiente, el porcentaje no significa nada
    .filter(F.col("entregas_totales") >= 100)
    .withColumn("incumplimiento_pct",
                F.round(100 * F.col("entregas_tardias") / F.col("entregas_totales"), 1))
    .withColumn("clasificacion",
        F.when(F.col("incumplimiento_pct") < 45, "A · cumple bien")
         .when(F.col("incumplimiento_pct") < 55, "B · intermedio")
         .otherwise("C · incumple mucho"))
    .orderBy(F.desc("incumplimiento_pct")))

TABLA_ORO = f"{CATALOGO}.{ESQUEMA}.desempeno_proveedores"

oro.write.format("delta").mode("overwrite") \
   .option("overwriteSchema", "true").saveAsTable(TABLA_ORO)

print(f"✅ {TABLA_ORO}: {spark.table(TABLA_ORO).count():,} proveedores")
display(spark.table(TABLA_ORO).orderBy(F.desc("incumplimiento_pct")).limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 · Documentar la tabla
# MAGIC
# MAGIC **Este paso casi nadie lo hace, y es el que más rinde** si van por los puntos opcionales.
# MAGIC
# MAGIC Los comentarios en las columnas no son decoración: **son lo que le permite a Genie entender
# MAGIC de qué habla la tabla**, y lo que hace que un tablero se lea solo.

# COMMAND ----------

spark.sql(f"""
    COMMENT ON TABLE {TABLA_ORO} IS
    'Desempeño de proveedores: cumplimiento de fechas comprometidas y gasto asociado.
     Una fila por proveedor con al menos 100 entregas registradas.
     Sirve para decidir con qué proveedores renegociar contratos.'
""")

comentarios = {
    "proveedor":              "Nombre del proveedor",
    "pais":                   "País desde el que despacha el proveedor",
    "entregas_totales":       "Número de líneas de pedido que surtió este proveedor",
    "entregas_tardias":       "Cuántas de esas entregas llegaron después de la fecha comprometida",
    "dias_retraso_promedio":  "Días de retraso promedio, contando solo las entregas tardías",
    "gasto_total":            "Gasto acumulado con este proveedor, neto de descuentos",
    "descuento_promedio_pct": "Descuento promedio que ofrece, en porcentaje",
    "incumplimiento_pct":     "Porcentaje de entregas que llegaron tarde",
    "clasificacion":          "Grupo de desempeño: A cumple bien, B intermedio, C incumple mucho",
}

for col, txt in comentarios.items():
    spark.sql(f"ALTER TABLE {TABLA_ORO} ALTER COLUMN {col} COMMENT '{txt}'")

print("✅ Tabla y columnas documentadas\n")
display(spark.sql(f"DESCRIBE TABLE {TABLA_ORO}"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Fíjense en la salida de arriba.** Cada columna tiene una descripción en lenguaje de
# MAGIC negocio, no técnico. `incumplimiento_pct` no dice «porcentaje calculado sobre el conteo»:
# MAGIC dice **«porcentaje de entregas que llegaron tarde»**.
# MAGIC
# MAGIC Esa diferencia es la que va a hacer que Genie acierte.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 · El recorrido del dato

# COMMAND ----------

n_origen = lineas.count()
n_oro    = spark.table(TABLA_ORO).count()
n_prov_total = prov.count()

print(f"Líneas de pedido en el origen : {n_origen:>12,}")
print(f"Proveedores en total          : {n_prov_total:>12,}")
print(f"Proveedores en la capa oro    : {n_oro:>12,}")
print(f"\nDescartados por el filtro de historial: {n_prov_total - n_oro:,} proveedores")

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretación.** La capa oro agrega 30 millones de líneas en unos pocos miles de
# MAGIC proveedores. La diferencia entre proveedores totales y proveedores en la tabla corresponde
# MAGIC al filtro de 100 entregas mínimas.
# MAGIC
# MAGIC **Ese filtro es una decisión de análisis, no un detalle técnico.** Sin él, un proveedor con
# MAGIC dos entregas y una tardía aparecería con 50% de incumplimiento y encabezaría la lista sin
# MAGIC ser relevante para la decisión.

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 · Medición inicial · 15 puntos
# MAGIC
# MAGIC ## 3.1 · La operación que vamos a optimizar
# MAGIC
# MAGIC Elegimos el **cruce entre `lineitem` y `supplier`**, que es el corazón de la consulta que
# MAGIC construye la capa oro.
# MAGIC
# MAGIC Es costoso por una razón concreta: `lineitem` tiene 30 millones de filas y `supplier` diez
# MAGIC mil. Por defecto, Spark reparte **las dos tablas** entre las máquinas para que las filas con
# MAGIC la misma llave queden juntas — y mover 30 millones de filas por la red domina el tiempo total.

# COMMAND ----------

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

# Desactivamos la optimización automática para partir del caso base
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

consulta_original = (lineas.alias("l")
    .join(prov.alias("s"), F.col("l.l_suppkey") == F.col("s.s_suppkey"))
    .withColumn("entrega_tardia", F.col("l.l_receiptdate") > F.col("l.l_commitdate"))
    .groupBy(F.col("s.s_nationkey"))
    .agg(F.count("*").alias("entregas"),
         F.sum(F.col("entrega_tardia").cast("int")).alias("tardias")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 · El plan de ejecución inicial

# COMMAND ----------

consulta_original.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **Qué vemos en el plan.** Dos cosas importan:
# MAGIC
# MAGIC **`SortMergeJoin`** — la estrategia que eligió Spark. Para cruzar las tablas, ordena las dos
# MAGIC y las recorre en paralelo. Funciona siempre, pero exige reorganizar los datos.
# MAGIC
# MAGIC **`Exchange hashpartitioning`** — el movimiento de datos entre máquinas, y aparece **sobre
# MAGIC las dos tablas**. Ahí está el costo: Spark está repartiendo las 30 millones de filas de
# MAGIC `lineitem` por la red para que las llaves coincidan.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 · Los tres tiempos

# COMMAND ----------

tiempo_antes = medir("ANTES · SortMergeJoin", lambda: consulta_original.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC # 4 · Optimización · 20 puntos
# MAGIC
# MAGIC ## 4.1 · Qué técnica aplicamos y por qué
# MAGIC
# MAGIC Aplicamos un ***broadcast join***.
# MAGIC
# MAGIC **La razón sale del plan inicial:** estamos moviendo 30 millones de filas para cruzarlas con
# MAGIC una tabla de diez mil. Es mucho más barato mandar una copia de la tabla pequeña a cada
# MAGIC máquina y que cada una resuelva su parte sin mover nada más.
# MAGIC
# MAGIC | | Tamaño | Con SortMergeJoin | Con broadcast |
# MAGIC |---|---|---|---|
# MAGIC | `lineitem` | 30 millones de filas | Se reparte por la red | **No se mueve** |
# MAGIC | `supplier` | 10 mil filas | Se reparte por la red | Se copia a cada máquina |
# MAGIC
# MAGIC **Descartamos otras opciones.** Reparticionar `lineitem` por `l_suppkey` habría ayudado si
# MAGIC la consulta se ejecutara muchas veces sobre los mismos datos, pero aquí el costo de
# MAGIC reparticionar se paga una sola vez y no se amortiza. El *broadcast* ataca directamente lo
# MAGIC que vimos caro en el plan.

# COMMAND ----------

consulta_optimizada = (lineas.alias("l")
    .join(broadcast(prov.alias("s")), F.col("l.l_suppkey") == F.col("s.s_suppkey"))
    .withColumn("entrega_tardia", F.col("l.l_receiptdate") > F.col("l.l_commitdate"))
    .groupBy(F.col("s.s_nationkey"))
    .agg(F.count("*").alias("entregas"),
         F.sum(F.col("entrega_tardia").cast("int")).alias("tardias")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 · El plan de ejecución resultante

# COMMAND ----------

consulta_optimizada.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 · Qué cambió entre los dos planes
# MAGIC
# MAGIC | | Antes | Después |
# MAGIC |---|---|---|
# MAGIC | Estrategia de cruce | `SortMergeJoin` | **`BroadcastHashJoin`** |
# MAGIC | Movimiento de datos | `Exchange hashpartitioning` sobre **las dos** tablas | **`BroadcastExchange` solo sobre `supplier`** |
# MAGIC | Ordenamiento | `Sort` sobre las dos tablas | **Ninguno** |
# MAGIC
# MAGIC **Lo que desapareció es el `Exchange` sobre `lineitem`.** La tabla grande ya no viaja por la
# MAGIC red: se queda donde está y cada máquina resuelve su parte con la copia local de `supplier`.
# MAGIC
# MAGIC Y como el *broadcast join* no necesita que los datos estén ordenados, también desaparecen
# MAGIC los dos `Sort`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 · Los tiempos, comparados

# COMMAND ----------

tiempo_despues = medir("DESPUÉS · BroadcastHashJoin", lambda: consulta_optimizada.collect())

mejora = (tiempo_antes - tiempo_despues) / tiempo_antes * 100

print("═" * 50)
print(f"{'SortMergeJoin':<30}{tiempo_antes:>8.2f}s")
print(f"{'BroadcastHashJoin':<30}{tiempo_despues:>8.2f}s")
print("─" * 50)
print(f"{'Mejora':<30}{mejora:>8.1f}%")
print("═" * 50)

# COMMAND ----------

# MAGIC %md
# MAGIC **Conclusión de la optimización.**
# MAGIC
# MAGIC La consulta original usaba `SortMergeJoin`, que obliga a repartir **las dos tablas** entre
# MAGIC las máquinas para que las filas con la misma llave queden juntas. Como `lineitem` tiene
# MAGIC 30 millones de filas, ese movimiento domina el tiempo total.
# MAGIC
# MAGIC Al forzar `broadcast` sobre `supplier` —diez mil filas— Spark cambió a `BroadcastHashJoin`:
# MAGIC manda una copia de la tabla pequeña a cada máquina y **la tabla grande ya no se mueve**.
# MAGIC En el plan desaparece el `Exchange hashpartitioning` sobre `lineitem` y también los `Sort`.
# MAGIC
# MAGIC Medido tres veces en cada caso, la mediana bajó del primer valor al segundo — los números
# MAGIC exactos están en la salida de la celda anterior.

# COMMAND ----------

# Devolvemos la configuración a su valor normal
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
print("Configuración restaurada.")

# COMMAND ----------

# MAGIC %md
# MAGIC # 5 · Límites de la optimización · 10 puntos
# MAGIC
# MAGIC El *broadcast join* funciona aquí **porque `supplier` es pequeña**. Tiene tres condiciones
# MAGIC donde dejaría de servir:
# MAGIC
# MAGIC ### 1 · Si la tabla difundida creciera
# MAGIC
# MAGIC Difundir significa **copiar la tabla completa a cada máquina**. Si `supplier` tuviera millones
# MAGIC de filas, estaríamos copiando ese volumen tantas veces como máquinas haya — más tráfico del
# MAGIC que ahorramos al no mover `lineitem`.
# MAGIC
# MAGIC Y si no cabe en la memoria de cada ejecutor, **la consulta no se pone lenta: falla**.
# MAGIC
# MAGIC ### 2 · Si las dos tablas fueran grandes
# MAGIC
# MAGIC Con dos tablas de volumen similar, `SortMergeJoin` es la estrategia correcta y forzar
# MAGIC *broadcast* sería un error.
# MAGIC
# MAGIC ### 3 · Si el volumen fuera pequeño
# MAGIC
# MAGIC Con pocos miles de filas, la diferencia entre las dos estrategias se pierde en la variación
# MAGIC normal entre ejecuciones. Comprobémoslo:

# COMMAND ----------

muestra = lineas.sample(fraction=0.0005, seed=42)
print(f"Muestra: {muestra.count():,} filas\n")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
peq_lento  = muestra.alias("l").join(prov.alias("s"), F.col("l.l_suppkey") == F.col("s.s_suppkey")).groupBy("s.s_nationkey").count()
t1 = medir("Muestra pequeña · SortMergeJoin", lambda: peq_lento.collect())

peq_rapido = muestra.alias("l").join(broadcast(prov.alias("s")), F.col("l.l_suppkey") == F.col("s.s_suppkey")).groupBy("s.s_nationkey").count()
t2 = medir("Muestra pequeña · BroadcastHashJoin", lambda: peq_rapido.collect())

spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
print(f"→ Diferencia: {abs(t1-t2):.2f}s. Con este volumen, la optimización no se distingue del ruido.")

# COMMAND ----------

# MAGIC %md
# MAGIC **La regla general.** El paralelismo y las optimizaciones de Spark **ayudan cuando hay
# MAGIC volumen y estorban cuando no lo hay**. Repartir el trabajo tiene un costo de coordinación
# MAGIC que solo se amortiza si hay suficiente trabajo que repartir.
# MAGIC
# MAGIC En nuestro caso concreto, esta optimización dejaría de tener sentido si la empresa redujera
# MAGIC su red a unos pocos proveedores muy grandes: `supplier` seguiría siendo pequeña, pero
# MAGIC `lineitem` también lo sería, y el costo de mover datos dejaría de dominar.

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
# MAGIC **Bajo un paradigma de lakehouse**, y la razón sale de la naturaleza de esas fuentes.
# MAGIC
# MAGIC Las cinco entregan datos con estructuras distintas y cambiantes: el portal da eventos
# MAGIC tabulares, las redes entregan respuestas anidadas cuyo esquema el proveedor modifica sin
# MAGIC avisar, y WhatsApp aporta texto libre. **Un modelo relacional obligaría a definir el esquema
# MAGIC antes de conocerlo**, y cada cambio de una API rompería la ingesta. Una base documental
# MAGIC manejaría bien esa variabilidad, pero **perdería las transacciones** que se necesitan para
# MAGIC corregir datos ya cargados cuando una plataforma reprocesa métricas hacia atrás.
# MAGIC
# MAGIC **Las herramientas** serían las que usamos aquí: almacenamiento en formato abierto sobre un
# MAGIC motor distribuido, con una capa de gobierno que centralice permisos y linaje — porque cinco
# MAGIC fuentes implican cinco responsables distintos y datos personales de por medio.
# MAGIC
# MAGIC **La arquitectura sería en capas**, como la que construimos: una capa cruda que conserva cada
# MAGIC fuente tal como llegó —indispensable cuando el esquema de origen cambia sin aviso—, una capa
# MAGIC intermedia que unifica identidades de usuario entre canales, y una capa de consumo por
# MAGIC pregunta de negocio.
# MAGIC
# MAGIC **El costo que asumiría esa decisión** es la dependencia del proveedor de la plataforma.
# MAGIC Se mitiga guardando en formatos abiertos, que es lo que hace Delta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.2 · Conclusiones del proyecto
# MAGIC
# MAGIC ### Qué funcionó
# MAGIC
# MAGIC La optimización dio una mejora medible y **explicable**: no fue un número que salió mejor,
# MAGIC sino un cambio de estrategia visible en el plan de ejecución.
# MAGIC
# MAGIC Documentar las columnas resultó más útil de lo esperado. Obligó a preguntarnos qué significa
# MAGIC cada métrica en lenguaje de negocio, y ahí nos dimos cuenta de que
# MAGIC `dias_retraso_promedio` era ambiguo: no era lo mismo promediar sobre todas las entregas que
# MAGIC solo sobre las tardías. Lo corregimos por eso.
# MAGIC
# MAGIC ### Qué no funcionó
# MAGIC
# MAGIC El primer intento de medición nos dio una mejora del 60%, que resultó ser un artefacto:
# MAGIC habíamos medido la consulta optimizada en segundo lugar, con el cómputo ya caliente. Al
# MAGIC agregar la ejecución de calentamiento, la mejora real bajó bastante. **Sin ese paso habríamos
# MAGIC reportado un número falso.**
# MAGIC
# MAGIC ### Qué haríamos distinto
# MAGIC
# MAGIC El filtro de 100 entregas mínimas lo elegimos por intuición. Habría sido mejor mirar la
# MAGIC distribución de entregas por proveedor y elegir un corte con criterio — por ejemplo, el que
# MAGIC deja fuera al 10% con menos historial.
# MAGIC
# MAGIC Y la capa oro responde una sola pregunta. Para que la directora no dependa de nosotros cada
# MAGIC vez que quiera mirar otra cosa, convendría una segunda tabla con el grano a nivel de mes.

# COMMAND ----------

# MAGIC %md
# MAGIC # 7 · Reparto del trabajo y uso de IA
# MAGIC
# MAGIC | Integrante | De qué se encargó | Qué sustenta en el video |
# MAGIC |---|---|---|
# MAGIC | Ejemplo de clase | Todo | — |
# MAGIC
# MAGIC **Uso de asistentes de IA:** se usó el Databricks Assistant para revisar la sintaxis de
# MAGIC `ALTER TABLE ... ALTER COLUMN ... COMMENT`. La lógica de negocio, la elección de la
# MAGIC optimización y las interpretaciones son propias.

# COMMAND ----------

# MAGIC %md
# MAGIC # ⭐ Opcional · Consumir la capa oro · +5 puntos
# MAGIC
# MAGIC Aquí está la parte que pidieron: **cómo apuntarle Genie o un tablero a esta tabla.**
# MAGIC
# MAGIC Las dos opciones usan la misma tabla que acabamos de crear y documentar:
# MAGIC
# MAGIC ```
# MAGIC ejemplo_ea3.abastecimiento.desempeno_proveedores
# MAGIC ```
# MAGIC
# MAGIC > 💡 **Y aquí se ve por qué documentamos las columnas.** Las dos herramientas leen esos
# MAGIC > comentarios. Sin ellos, Genie adivina y el tablero muestra nombres crípticos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opción A · Un espacio de Genie
# MAGIC
# MAGIC ### Cómo crearlo
# MAGIC
# MAGIC | Paso | Dónde |
# MAGIC |---|---|
# MAGIC | 1 | Menú lateral → **Genie** |
# MAGIC | 2 | **New** → nuevo espacio |
# MAGIC | 3 | En **Tables**, seleccionen su tabla oro |
# MAGIC | 4 | Denle un nombre y una descripción: *«Desempeño de proveedores: cumplimiento de fechas y gasto»* |
# MAGIC | 5 | **Create** y esperen a que quede listo |
# MAGIC
# MAGIC ### Tres preguntas para hacerle
# MAGIC
# MAGIC Estas tres funcionan bien con nuestra tabla. **Adapten las suyas a su caso:**
# MAGIC
# MAGIC 1. *¿Cuáles son los cinco proveedores con mayor porcentaje de incumplimiento?*
# MAGIC 2. *¿Los proveedores que más incumplen ofrecen mayores descuentos?*
# MAGIC 3. *¿Qué país concentra el mayor gasto y cuál es su nivel de cumplimiento?*
# MAGIC
# MAGIC ### Lo que hay que documentar
# MAGIC
# MAGIC **No basta con pegar las capturas de las respuestas.** Lo que se evalúa es
# MAGIC **qué tuvieron que ajustar** para que Genie entendiera:
# MAGIC
# MAGIC - ¿Acertó a la primera, o hubo que renombrar alguna columna?
# MAGIC - ¿Hizo falta agregar instrucciones al espacio, en **Instructions**?
# MAGIC - ¿Alguna pregunta la respondió mal? ¿Por qué creen que pasó?
# MAGIC
# MAGIC > **Ese análisis es el verdadero ejercicio.** Si Genie no entiende su tabla, es porque la
# MAGIC > tabla no está bien diseñada — y descubrir eso vale más que las capturas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opción B · Un tablero
# MAGIC
# MAGIC ### Cómo crearlo
# MAGIC
# MAGIC | Paso | Dónde |
# MAGIC |---|---|
# MAGIC | 1 | Menú lateral → **Dashboards** → **Create dashboard** |
# MAGIC | 2 | Pestaña **Data** → **Select a table** → su tabla oro |
# MAGIC | 3 | Pestaña **Canvas** → **Add a visualization** |
# MAGIC | 4 | Repitan hasta tener **tres** |
# MAGIC | 5 | **Publish** y tomen la captura |
# MAGIC
# MAGIC ### Tres visualizaciones que responden algo
# MAGIC
# MAGIC | Visualización | Qué responde |
# MAGIC |---|---|
# MAGIC | Barras: proveedor vs. `incumplimiento_pct`, top 10 | ¿A quiénes hay que renegociar? |
# MAGIC | Dispersión: `incumplimiento_pct` vs. `descuento_promedio_pct` | ¿Los que incumplen compensan con precio? |
# MAGIC | Barras: `pais` vs. suma de `gasto_total`, coloreado por `clasificacion` | ¿Dónde está el gasto y cómo se comporta? |
# MAGIC
# MAGIC > **No se evalúa lo bonito que quede.** Se evalúa que cada gráfico responda algo que a
# MAGIC > alguien del negocio le importaría saber. Un gráfico que solo muestra «cuántas filas hay»
# MAGIC > no cuenta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Una prueba rápida antes de intentarlo
# MAGIC
# MAGIC Si su tabla pasa estas cuatro, Genie y el tablero le van a funcionar. Si falla alguna,
# MAGIC arréglenla primero.

# COMMAND ----------

df = spark.table(TABLA_ORO)
desc = spark.sql(f"DESCRIBE TABLE {TABLA_ORO}").collect()
sin_comentario = [r["col_name"] for r in desc
                  if r["col_name"] and not r["col_name"].startswith("#") and not r["comment"]]
cripticas = [c for c in df.columns if len(c) <= 3 or re.match(r'^(col|c)\d+$', c, re.I)]

print("PRUEBA DE LA CAPA ORO\n")
print(f"1 · Todas las columnas tienen comentario   {'✅' if not sin_comentario else '❌ faltan: ' + ', '.join(sin_comentario)}")
print(f"2 · Ningún nombre críptico                 {'✅' if not cripticas else '❌ revisar: ' + ', '.join(cripticas)}")
print(f"3 · Hay al menos una columna de texto      {'✅' if any(t=='string' for _,t in df.dtypes) else '❌ solo hay números e identificadores'}")
print(f"4 · La tabla cabe en un tablero            {'✅' if df.count() < 100000 else '⚠️ ' + f'{df.count():,} filas — considere agregar más'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎯 Ahora con su caso
# MAGIC
# MAGIC | Sección | Qué adaptar |
# MAGIC |---|---|
# MAGIC | 2 · Capa oro | Su pregunta de negocio, sus columnas, **y los comentarios** |
# MAGIC | 3 · Medición | Un cruce entre dos de **sus** tablas que tarde algo medible |
# MAGIC | 4 · Optimización | Puede ser *broadcast* u otra. Lo que importa es explicar el plan |
# MAGIC | 5 · Límites | Atado a **sus** datos: qué tendría que cambiar para que dejara de servir |
# MAGIC | 6 · Cierre | Su respuesta a la pregunta orientadora |
# MAGIC | ⭐ Opcional | Genie o tablero sobre **su** tabla oro |
# MAGIC
# MAGIC > ⚠️ **No copien este caso.** Una directora de abastecimiento en una entrega sobre
# MAGIC > Wanderbricks se nota de inmediato. **Copien la estructura, no el contenido.**
# MAGIC
# MAGIC **Entrega: domingo 4 de octubre, 11:59 p. m.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apéndice · Limpieza

# COMMAND ----------

# Descomentar para borrar el catálogo de este ejemplo:
# spark.sql(f"DROP CATALOG IF EXISTS {CATALOGO} CASCADE")
