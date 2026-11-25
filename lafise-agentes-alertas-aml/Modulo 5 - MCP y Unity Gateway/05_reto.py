# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Reto: un agente de consulta de solo lectura
# MAGIC
# MAGIC El área de **Riesgo de Cliente** quiere su propio asistente para revisar clientes antes de una vinculación: puede
# MAGIC **consultar** listas, PEP e historial de casos, pero **nunca** registrar ni cerrar casos. Además, cada analista
# MAGIC debe tener su **propio tope** de llamadas, para que uno solo no agote la cuota del proveedor.
# MAGIC
# MAGIC **Tu reto (⏱️ ~20 min, tarea):**
# MAGIC 1. Registrar un **segundo MCP service** `servicios_pld_consulta` en tu schema, sobre la misma conexión, que
# MAGIC    exponga **solo** las herramientas de lectura.
# MAGIC 2. Ponerle un **rate limit por usuario** de 20 llamadas por minuto.
# MAGIC 3. Construir el **agente de consulta** con ese servicio y comprobar que no puede registrar casos.
# MAGIC 4. *(Bonus)* Contar en `system.ai_gateway.usage` tus llamadas por servicio y por herramienta.
# MAGIC
# MAGIC > Gobernar no exige otro servidor: **el mismo** servidor MCP se publica dos veces con distinta configuración,
# MAGIC > cada una con sus propios permisos, topes y auditoría.

# COMMAND ----------

# MAGIC %pip install --quiet "databricks-mcp==0.9.2" "databricks-langchain==0.22.0" "langgraph==1.2.20" "mcp>=1.24,<2" "mlflow[databricks]>=3.17" "unitycatalog-langchain[databricks]>=0.3.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import re, json, time

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "pld_aml", "Schema base")
dbutils.widgets.text("llm", "", "Endpoint LLM del agente (opcional)")
dbutils.widgets.text("schema_servicios", "pld_servicios_mcp", "Schema de los servicios PLD")
user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe en el widget 'catalog' el mismo catálogo que usaste en 00_setup."
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'pld_aml')}_{user_prefix}"
FQ = f"`{CATALOG}`.{SCHEMA}"
SCHEMA_SERV = dbutils.widgets.get("schema_servicios").strip() or "pld_servicios_mcp"
CONEXION = f"{CATALOG}.{SCHEMA_SERV}.conn_servicios_pld"
SERVICIO = "servicios_pld_consulta"
MI_MCP_CONSULTA = f"{CATALOG}.{SCHEMA}.{SERVICIO}"
EXPERIMENTO = f"/Users/{user_email}/lafise_s5_mcp_gateway"

# COMMAND ----------

# MAGIC %run ./utils/agente_mcp_utils

# COMMAND ----------

w = cliente_ws()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Registrar el MCP service de solo lectura
# MAGIC Pista: en el `03` usaste `crear_mcp_service(...)` con `include_tool_selectors`. Las herramientas de lectura
# MAGIC empiezan con `screening_`, `verificar_` y `consultar_`.

# COMMAND ----------

# TODO Paso 1: lista de patrones de herramientas de SOLO LECTURA (nombres exactos o prefijos con *)
SOLO_LECTURA = <FILL_IN>

if obtener_mcp_service(w, MI_MCP_CONSULTA):
    borrar_mcp_service(w, MI_MCP_CONSULTA)
    time.sleep(3)
crear_mcp_service(w, CATALOG, SCHEMA, SERVICIO, CONEXION, include_tool_selectors=SOLO_LECTURA,
                  comentario="Servicios PLD de solo lectura para el agente de consulta de Riesgo de Cliente")
consulta = ClienteMCP(url_mcp_service(MI_MCP_CONSULTA), SERVICIO)
herramientas = esperar_herramientas(consulta, intentos=6)
print("Herramientas del servicio de consulta:", herramientas)

# COMMAND ----------

# Validación — ['screening_sanciones', 'verificar_pep', 'consultar_casos_cliente']
assert set(herramientas) == {"screening_sanciones", "verificar_pep", "consultar_casos_cliente"}, herramientas
print("✅ Paso 1: solo herramientas de lectura")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Rate limit por usuario (20 QPM)
# MAGIC Pista: en el `03` el tope era para todo el servicio (`RATE_LIMIT_KEY_SERVICE`). Para un tope **por usuario** la
# MAGIC clave es `RATE_LIMIT_KEY_USER` y se indica el `principal` (un usuario o un grupo).

# COMMAND ----------

# TODO Paso 2: completa la clave del tope por usuario y el principal (tu usuario)
actualizar_mcp_service(w, MI_MCP_CONSULTA, "config.rate_limits", {"rate_limits": [
    {"key": <FILL_IN>, "principal": <FILL_IN>,
     "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE", "requests": 20}]})
print(json.dumps(obtener_mcp_service(w, MI_MCP_CONSULTA)["config"].get("rate_limits"), indent=2))

# COMMAND ----------

# Validación — un rate limit RATE_LIMIT_KEY_USER de 20 por minuto para tu usuario
rl = obtener_mcp_service(w, MI_MCP_CONSULTA)["config"].get("rate_limits") or []
assert rl and rl[0]["key"] == "RATE_LIMIT_KEY_USER" and int(rl[0]["requests"]) == 20, rl
print("✅ Paso 2: tope por usuario de 20/min")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — El agente de consulta
# MAGIC Pista: `construir_agente_mcp(w, spark, CATALOG, SCHEMA, LLM, <cliente MCP>, instrucciones=...)`. Ajusta las
# MAGIC instrucciones para que el agente **no** intente registrar casos.

# COMMAND ----------

import mlflow
mlflow.set_experiment(EXPERIMENTO)
mlflow.langchain.autolog()
LLM = elegir_llm(w, dbutils.widgets.get("llm").strip())

# TODO Paso 3: instrucciones del agente de consulta. Parte de INSTRUCCIONES_AGENTE_MCP (utils) y cambia el paso 7
#               para que NO registre casos (por ejemplo con .replace(...)).
INSTRUCCIONES_CONSULTA = <FILL_IN>
agente_consulta, info = construir_agente_mcp(w, spark, CATALOG, SCHEMA, LLM, consulta,
                                             instrucciones=INSTRUCCIONES_CONSULTA)
print("Herramientas MCP del agente de consulta:", info["herramientas_mcp"])

dictamen, llamadas = dictaminar(agente_consulta, "Analiza la alerta AT-005 y emite tu dictamen.",
                                version="s5-consulta")
mostrar_llamadas(llamadas, set(info["herramientas_mcp"]))
print("\n" + json.dumps(dictamen, ensure_ascii=False, indent=2))

# COMMAND ----------

# Validación — sin crear_caso en las llamadas, caso_id nulo y una disposición válida
usadas = [h for h, _, _ in llamadas]
assert "crear_caso" not in info["herramientas_mcp"] and "crear_caso" not in usadas
assert not dictamen.get("caso_id"), dictamen.get("caso_id")
assert dictamen.get("disposicion") in DISPOSICIONES, dictamen
print(f"✅ Paso 3: AT-005 → {dictamen['disposicion']} sin registrar casos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 (Bonus) — Tus llamadas por servicio y herramienta
# MAGIC Recuerda que `system.ai_gateway.usage` llega con 15–60 min de retraso.

# COMMAND ----------

# TODO Paso 4: completa el método JSON-RPC de las llamadas a herramientas
try:
    display(spark.sql(f"""
        SELECT service_name, mcp_metadata.tool_name AS herramienta, status_code, count(*) AS llamadas
        FROM system.ai_gateway.usage
        WHERE service_type = 'MCP_SERVICE' AND requester = '{user_email}'
          AND service_name IN ('{CATALOG}.{SCHEMA}.servicios_pld_agente', '{MI_MCP_CONSULTA}')
          AND mcp_metadata.json_rpc_method = '<FILL_IN>'
          AND event_time >= current_timestamp() - INTERVAL 7 DAYS
        GROUP BY ALL ORDER BY service_name, llamadas DESC"""))
except Exception as e:
    print(f"⚠️  {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¿Terminaste?
# MAGIC Compara con `solutions/05_reto_solution` y lee sus **enfoques alternativos** (service policies por agente, permisos
# MAGIC por grupo, topes por grupo).
