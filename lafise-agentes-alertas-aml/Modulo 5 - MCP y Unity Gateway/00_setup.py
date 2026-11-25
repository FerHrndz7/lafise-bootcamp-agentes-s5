# Databricks notebook source
# MAGIC %md
# MAGIC # Módulo 5 · Preparación — El Agente de Alertas se conecta por MCP
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Duración** | ~75 min de práctica (esta preparación: ~8 min) |
# MAGIC | **Nivel** | Intermedio |
# MAGIC | **Lenguaje** | Python |
# MAGIC | **Cómputo** | Clúster **clásico** (DBR **17.3 LTS** recomendado, modo de acceso *Standard*) |
# MAGIC | **Catalog.Schema** | `<catálogo_asignado>.pld_aml_<tu_usuario>` (el mismo de las Sesiones 1 a 4) |
# MAGIC
# MAGIC ## Qué haremos hoy 🔌
# MAGIC 1. `00_setup` — dependencias, datos y conexión con los servicios PLD (**estás aquí**).
# MAGIC 2. `01_que_es_mcp` — el protocolo en vivo con los **MCP de Databricks**.
# MAGIC 3. `02_servicios_pld_por_mcp` — los **sistemas del banco** (simulados) expuestos por MCP.
# MAGIC 4. `03_gobernar_con_unity_gateway` — **tu** MCP service: herramientas permitidas, rate limit, permisos, auditoría.
# MAGIC 5. `04_agente_con_mcp` — el **Agente de Alertas** usando esos servicios, gobernado y trazado.
# MAGIC 6. `05_reto` — **tarea**: un agente de consulta de solo lectura.
# MAGIC
# MAGIC ### Objetivos de aprendizaje
# MAGIC - Explicar qué es MCP (`tools/list`, `tools/call`) y por qué conviene frente a integraciones "a mano".
# MAGIC - Consumir MCP gestionados por Databricks y un servidor MCP propio desde un agente.
# MAGIC - Registrar y **gobernar** un servidor MCP en **Unity Gateway**: `EXECUTE`, herramientas permitidas, rate limits,
# MAGIC   uso, auditoría y service policies.
# MAGIC - Conectar el Agente de Alertas a los servicios PLD y leer en la traza qué hizo por MCP.
# MAGIC
# MAGIC > ⚠️ **Datos sintéticos.** Todo es inventado. Nunca uses PII real de clientes en un workspace de laboratorio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 1 — Dependencias
# MAGIC Versiones **fijas** y verificadas en DBR 17.3 LTS (noviembre 2026): con versiones libres, `pip` puede tardar más
# MAGIC de 15 minutos resolviendo dependencias. Si el clúster ya tiene `requirements.txt` como librerías de clúster, esta
# MAGIC celda termina rápido.

# COMMAND ----------

# MAGIC %pip install --quiet "databricks-mcp==0.9.2" "databricks-langchain==0.22.0" "langgraph==1.2.20" "mcp>=1.24,<2" "mlflow[databricks]>=3.17" "unitycatalog-langchain[databricks]>=0.3.0" "faker>=25.0.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 2 — Configuración (aislamiento por participante)
# MAGIC
# MAGIC - **catalog** (requerido): el **mismo catálogo de las Sesiones 1 a 4**. Tu schema lleva tu usuario automáticamente.
# MAGIC - **llm** (opcional): el modelo del agente. Vacío = el mejor modelo con *tool calling* disponible.
# MAGIC - **schema_servicios** (opcional): el schema donde el facilitador dejó la **conexión** y el **MCP service** de los
# MAGIC   servicios PLD. Déjalo como está salvo que el facilitador te indique otro.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "pld_aml", "Schema base")
dbutils.widgets.text("llm", "", "Endpoint LLM del agente (opcional)")
dbutils.widgets.text("schema_servicios", "pld_servicios_mcp", "Schema de los servicios PLD")

user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, ("Escribe en el widget 'catalog' el catálogo que te asignó el facilitador "
                 "(el mismo de las Sesiones 1 a 4).")
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'pld_aml')}_{user_prefix}"
FQ = f"`{CATALOG}`.{SCHEMA}"
SCHEMA_SERV = dbutils.widgets.get("schema_servicios").strip() or "pld_servicios_mcp"
CONEXION = f"{CATALOG}.{SCHEMA_SERV}.conn_servicios_pld"          # credencial del sistema externo (no la ves)
MCP_COMPARTIDO = f"{CATALOG}.{SCHEMA_SERV}.servicios_pld"          # MCP service del facilitador
EXPERIMENTO = f"/Users/{user_email}/lafise_s5_mcp_gateway"

print(f"Usuario:            {user_email}")
print(f"Tu schema:          {CATALOG}.{SCHEMA}")
print(f"Servicios PLD (MCP): {MCP_COMPARTIDO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 3 — Verificación del entorno

# COMMAND ----------

import sys
from importlib.metadata import version

print(f"Python: {sys.version.split()[0]}")
assert sys.version_info >= (3, 10), "Se requiere Python 3.10+"
for p in ["mcp", "databricks-mcp", "databricks-langchain", "langgraph", "mlflow", "databricks-sdk"]:
    try:
        print(f"  {p:22s} {version(p)}")
    except Exception:
        print(f"  {p:22s} ⚠️ no instalado")
try:
    print(f"DBR del clúster: {spark.conf.get('spark.databricks.clusterUsageTags.sparkVersion')}")
except Exception:
    print("DBR del clúster: no legible aquí (confirma 17.3 LTS en la configuración del clúster).")

# COMMAND ----------

# MAGIC %run ./utils/agente_mcp_utils

# COMMAND ----------

# MAGIC %run ./utils/fuente_servidor_pld

# COMMAND ----------

w = cliente_ws()
LLM = elegir_llm(w, dbutils.widgets.get("llm").strip())
print(f"Workspace:         {ws_url()}")
print(f"Modelo del agente: {LLM}")
from databricks_langchain import ChatDatabricks
print("Respuesta del modelo:", ChatDatabricks(endpoint=LLM, temperature=0).invoke("Responde solo: listo").content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 4 — Permisos
# MAGIC Hoy crearás **tablas**, **funciones** y un **MCP service** (un *service* de Unity Catalog) en tu schema.

# COMMAND ----------

try:
    spark.sql(f"USE CATALOG `{CATALOG}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
    spark.sql(f"CREATE OR REPLACE TABLE {FQ}._prueba_permiso AS SELECT 1 AS ok")
    spark.sql(f"CREATE OR REPLACE FUNCTION {FQ}._prueba_permiso_fn() RETURNS STRING RETURN 'ok'")
    spark.sql(f"DROP TABLE IF EXISTS {FQ}._prueba_permiso")
    spark.sql(f"DROP FUNCTION IF EXISTS {FQ}._prueba_permiso_fn")
    print(f"✅ Puedes crear tablas y funciones en {CATALOG}.{SCHEMA} (eres dueño del schema: también services)")
except Exception as e:
    raise PermissionError(
        f"No se pueden crear objetos en {CATALOG}.{SCHEMA}. Pide al administrador USE CATALOG + USE SCHEMA + "
        f"CREATE TABLE + CREATE FUNCTION + CREATE SERVICE. Error subyacente: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 5 — Datos y herramientas del lakehouse
# MAGIC El bootstrap **reutiliza** lo que construiste en las sesiones anteriores (clientes, transacciones, alertas,
# MAGIC base de conocimiento, funciones de UC) y refresca las **13 alertas testigo** `AT-001..013`.

# COMMAND ----------

# MAGIC %run ./data/bootstrap_sesion5 $catalog=$catalog $schema=$schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 6 — ¿Llegamos a los servicios PLD?
# MAGIC Probamos el MCP service que preparó el facilitador **a través de Unity Gateway**. Si no responde (el workspace
# MAGIC no tiene el servidor compartido), los notebooks usarán la copia **en memoria** de los mismos servicios.

# COMMAND ----------

cliente = ClienteMCP(url_mcp_service(MCP_COMPARTIDO), "servicios_pld (facilitador)")
try:
    nombres = [t["name"] for t in cliente.herramientas()]
    estado = cliente.llamar("estado_servicio")
    MODO_SERVICIOS = "gateway"
    print(f"✅ Servicios PLD por Unity Gateway: {nombres}")
    print(f"   El servidor ve la identidad: {estado.get('identidad_vista_por_el_servidor')}")
except Exception as e:
    MODO_SERVICIOS = "local"
    print(f"⚠️  No se pudo llegar a {MCP_COMPARTIDO} por Unity Gateway: {str(e)[:200]}")
    local = ClienteMCPLocal()
    print(f"➡️  Plan B: servicios PLD en memoria con {[t['name'] for t in local.herramientas()]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 7 — Resumen

# COMMAND ----------

print("=" * 66)
print("PREPARACIÓN COMPLETA ✅")
print("=" * 66)
print(f"Catalog.Schema      : {CATALOG}.{SCHEMA}")
print(f"Modelo del agente   : {LLM}")
print(f"Servicios PLD       : {MODO_SERVICIOS}  ({'Unity Gateway' if MODO_SERVICIOS == 'gateway' else 'en memoria, plan B'})")
print(f"Experimento MLflow  : {EXPERIMENTO}")
display(spark.sql(f"SELECT a.alerta_id, c.nombre, c.cliente_id, a.tipologia, c.es_pep FROM {FQ}.alertas a "
                  f"JOIN {FQ}.clientes c USING (cliente_id) WHERE a.alerta_id LIKE 'AT-%' ORDER BY a.alerta_id"))
print("➡️  Continúa con 01_que_es_mcp")
