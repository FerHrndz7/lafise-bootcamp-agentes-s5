# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gobernar con Unity Gateway: tu propio MCP service
# MAGIC
# MAGIC **⏱️ ~22 min** · **Objetivos:** registrar un servidor MCP como **objeto de Unity Catalog** y aplicarle los cinco
# MAGIC controles que pediría Riesgo Tecnológico o Auditoría Interna antes de dejar que un agente toque un sistema del
# MAGIC banco.
# MAGIC
# MAGIC | # | Control | Pregunta que responde | Dónde |
# MAGIC |---|---|---|---|
# MAGIC | 1 | **Herramientas permitidas** | ¿Qué puede hacer el agente en ese sistema? | `config.include_tool_selectors` |
# MAGIC | 2 | **Permisos** | ¿Quién puede llamar? | `EXECUTE` sobre el MCP service |
# MAGIC | 3 | **Rate limit** | ¿Cuántas llamadas por minuto? | `config.rate_limits` (QPM) |
# MAGIC | 4 | **Uso y auditoría** | ¿Quién llamó qué, cuándo y con qué resultado? | `system.ai_gateway.usage`, `system.access.audit` |
# MAGIC | 5 | **Service policies** (Beta) | ¿Esta llamada concreta cumple la política? | Función SQL adjunta al MCP service |
# MAGIC
# MAGIC El facilitador dejó en Unity Catalog una **conexión** (`conn_servicios_pld`) con la credencial del sistema externo.
# MAGIC Tienes `USE CONNECTION` sobre ella: puedes **registrar** un MCP service que la use, pero no puedes ver el secreto.

# COMMAND ----------

# MAGIC %pip install --quiet "databricks-mcp==0.9.2" "databricks-langchain==0.22.0" "langgraph==1.2.20" "mcp>=1.24,<2" "mlflow[databricks]>=3.17" "unitycatalog-langchain[databricks]>=0.3.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import re, json, time

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "pld_aml", "Schema base")
dbutils.widgets.text("schema_servicios", "pld_servicios_mcp", "Schema de los servicios PLD")
user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe en el widget 'catalog' el mismo catálogo que usaste en 00_setup."
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'pld_aml')}_{user_prefix}"
FQ = f"`{CATALOG}`.{SCHEMA}"
SCHEMA_SERV = dbutils.widgets.get("schema_servicios").strip() or "pld_servicios_mcp"
CONEXION = f"{CATALOG}.{SCHEMA_SERV}.conn_servicios_pld"
MCP_COMPARTIDO = f"{CATALOG}.{SCHEMA_SERV}.servicios_pld"

SERVICIO = "servicios_pld_agente"                       # tu MCP service, en TU schema
MI_MCP = f"{CATALOG}.{SCHEMA}.{SERVICIO}"

# COMMAND ----------

# MAGIC %run ./utils/agente_mcp_utils

# COMMAND ----------

w = cliente_ws()
conexion_ok = True
try:
    w.api_client.do("GET", f"/api/2.1/unity-catalog/connections/{CONEXION}")
except Exception as e:
    conexion_ok = False
    print(f"⚠️  No encuentro la conexión {CONEXION}: {str(e)[:150]}\n"
          "   Este notebook se presenta como DEMO del facilitador (los servicios PLD no están en tu workspace).")
print(f"Conexión:        {CONEXION}  {'✅' if conexion_ok else '❌'}\nTu MCP service:  {MI_MCP}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control 1 — Registrar tu MCP service con solo las herramientas que el agente necesita
# MAGIC
# MAGIC El agente necesita **consultar** (screening, PEP, historial) y **registrar** casos. No necesita **cerrarlos**, ni
# MAGIC la herramienta técnica `estado_servicio`. Con `include_tool_selectors` publicamos solo esas herramientas: nombres
# MAGIC exactos o prefijos con `*`.
# MAGIC
# MAGIC     POST /api/2.1/unity-catalog/mcp-services?parent=schemas/<catálogo>.<schema>&mcp_service_id=<servicio>
# MAGIC     {"config": {"source_connection": {"name": "connections/<conexión>"},
# MAGIC                 "include_tool_selectors": ["screening_*", "verificar_*", "consultar_*", "crear_caso"]}}
# MAGIC
# MAGIC (Lo mismo se hace en la UI: **Unity Gateway ▸ MCPs ▸ + MCP**, con Terraform o con bundles.)

# COMMAND ----------

PERMITIDAS = ["screening_*", "verificar_*", "consultar_*", "crear_caso"]

if conexion_ok:
    if obtener_mcp_service(w, MI_MCP):
        borrar_mcp_service(w, MI_MCP)          # idempotente: lo recreamos desde cero
        time.sleep(3)
    creado = crear_mcp_service(w, CATALOG, SCHEMA, SERVICIO, CONEXION, include_tool_selectors=PERMITIDAS,
                               comentario="Servicios PLD para el Agente de Alertas: consulta + registro de casos, sin cierre")
    print("✅ Registrado:", creado["name"])
    print(json.dumps(creado.get("config"), ensure_ascii=False, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC Tu servicio ya es un objeto de Unity Catalog: aparece en **Catalog Explorer** dentro de tu schema y en
# MAGIC **Unity Gateway ▸ MCPs**, con su URL propia. Comparemos lo que anuncia frente al del facilitador:

# COMMAND ----------

mi_cliente = ClienteMCP(url_mcp_service(MI_MCP), SERVICIO)
compartido = ClienteMCP(url_mcp_service(MCP_COMPARTIDO), "servicios_pld")
if conexion_ok:
    print("URL de tu MCP service:", mi_cliente.url, "\n")
    print("Facilitador (todas):", [t["name"] for t in compartido.herramientas()])
    print("Tuyo (permitidas):  ", esperar_herramientas(mi_cliente, intentos=6))

# COMMAND ----------

# MAGIC %md
# MAGIC ¿Y si alguien (o un agente manipulado con *prompt injection*) intenta llamar `cerrar_caso` de todas formas? El
# MAGIC gateway la rechaza **antes** de que llegue al sistema del banco:

# COMMAND ----------

if conexion_ok:
    try:
        mi_cliente.llamar("cerrar_caso", {"caso_id": "CASO-2026-0108", "motivo": "prueba"})
        print("❌ La llamada pasó (no debería)")
    except MCPError as e:
        print("✅ Bloqueada por Unity Gateway →", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control 2 — Permisos: ¿quién puede llamar?
# MAGIC
# MAGIC Para usar un MCP service se necesita `EXECUTE` sobre él, más `USE CATALOG` y `USE SCHEMA` sobre sus padres.
# MAGIC Quien **usa** el servicio **no** necesita `USE CONNECTION`: ese permiso es solo para quien registra servicios,
# MAGIC porque permitiría usar la conexión saltándose la selección de herramientas y las políticas.
# MAGIC
# MAGIC Tú eres dueño de tu servicio. Así se lo compartirías a un equipo (no lo ejecutamos):
# MAGIC
# MAGIC     PATCH /api/2.1/unity-catalog/permissions/mcp_service/<catálogo>.<schema>.<servicio>
# MAGIC     {"changes": [{"principal": "pld-analistas", "add": ["EXECUTE"]}]}
# MAGIC
# MAGIC     -- o con el CLI:  databricks grants update mcp_service <nombre> --json '{"changes": [...]}'

# COMMAND ----------

if conexion_ok:
    print(f"Dueño de tu servicio: {obtener_mcp_service(w, MI_MCP).get('owner')}  (el dueño tiene todos los privilegios)")
    p = w.api_client.do("GET", f"/api/2.1/unity-catalog/effective-permissions/mcp_service/{MCP_COMPARTIDO}",
                        query={"principal": user_email})
    privs = sorted({x["privilege"] for a in p.get("privilege_assignments", []) for x in a.get("privileges", [])})
    print(f"Tus privilegios sobre el servicio del facilitador {MCP_COMPARTIDO}: {privs}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control 3 — Rate limit: que un agente en bucle no tumbe el sistema del banco
# MAGIC
# MAGIC Un agente con un error de lógica puede llamar la misma herramienta cientos de veces por minuto. El rate limit
# MAGIC de un MCP service se mide en **llamadas `tools/call` por minuto (QPM)**; por encima, el gateway responde **429**.
# MAGIC Puede ser para todo el servicio (`RATE_LIMIT_KEY_SERVICE`) o por usuario o grupo (`RATE_LIMIT_KEY_USER` +
# MAGIC `principal`).
# MAGIC
# MAGIC Ponemos un tope **didáctico** de 3 por minuto, provocamos el 429 y lo devolvemos a 60. El cambio tarda unos
# MAGIC segundos en propagarse; la demo reintenta hasta ver 2 rechazos (máx. ~2 min).

# COMMAND ----------

def poner_rate_limit(qpm: int):
    actualizar_mcp_service(w, MI_MCP, "config.rate_limits", {"rate_limits": [
        {"key": "RATE_LIMIT_KEY_SERVICE", "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE", "requests": qpm}]})

if conexion_ok:
    poner_rate_limit(3)
    print("Tope temporal: 3 llamadas/min. Llamando screening_sanciones cada 3 s...")
    rechazos, t0 = 0, time.time()
    for i in range(40):
        try:
            mi_cliente.llamar("screening_sanciones", {"nombre": "Ramiro Quintanilla Rojas"})
            print(f"  llamada {i + 1:2d}: ✅ 200")
        except MCPError as e:
            rechazos += 1
            print(f"  llamada {i + 1:2d}: ⛔ {str(e)[:110]}")
            if rechazos >= 2:
                break
        time.sleep(3)
    poner_rate_limit(60)
    print(f"\n{'✅' if rechazos else '⚠️'} {rechazos} rechazos en {time.time() - t0:.0f} s. Tope restaurado a 60/min.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control 4 — Uso y auditoría: ¿quién llamó qué?
# MAGIC
# MAGIC Cada llamada por Unity Gateway queda en dos tablas de sistema:
# MAGIC
# MAGIC - **`system.ai_gateway.usage`** (`service_type = 'MCP_SERVICE'`): servicio, **quién** llamó (`requester`),
# MAGIC   herramienta (`mcp_metadata.tool_name`), método, código HTTP y latencia. Aquí ves los 429 del paso anterior.
# MAGIC - **`system.access.audit`** (`action_name = 'mcpCall'`): la auditoría de la cuenta, junto con quién creó o
# MAGIC   cambió cada MCP service (`createMcpService`, `updateMcpService`, ...).
# MAGIC
# MAGIC Recuerda el `02`: el sistema externo veía al service principal. El gateway, en cambio, registra **tu usuario**.
# MAGIC
# MAGIC > ⏱️ Las tablas de sistema llegan con **retraso (15–60 min)**. Si tus llamadas de hoy aún no aparecen, mira las
# MAGIC > de los últimos días o vuelve a esta celda al final de la sesión. Requiere `SELECT` sobre `system.ai_gateway` y
# MAGIC > `system.access` (el facilitador te dirá si tu usuario lo tiene).

# COMMAND ----------

try:
    display(spark.sql(f"""
        SELECT service_name, requester, mcp_metadata.json_rpc_method AS metodo, mcp_metadata.tool_name AS herramienta,
               status_code, count(*) AS llamadas, round(avg(latency_ms)) AS latencia_ms, max(event_time) AS ultima
        FROM system.ai_gateway.usage
        WHERE service_type = 'MCP_SERVICE' AND event_time >= current_timestamp() - INTERVAL 7 DAYS
          AND (service_name IN ('{MI_MCP}', '{MCP_COMPARTIDO}') OR service_name LIKE 'system.ai.%')
          AND requester = '{user_email}'
        GROUP BY ALL ORDER BY ultima DESC"""))
except Exception as e:
    print(f"⚠️  No puedes leer system.ai_gateway.usage: {str(e)[:200]}")

# COMMAND ----------

try:
    display(spark.sql(f"""
        SELECT event_time, action_name, user_identity.email AS usuario, response.status_code AS http,
               coalesce(request_params['fully_qualified_name'], request_params['name'], request_params['parent']) AS objeto,
               request_params['json_rpc_method'] AS metodo
        FROM system.access.audit
        WHERE event_date >= current_date() - 7 AND user_identity.email = '{user_email}'
          AND (action_name = 'mcpCall' OR action_name ILIKE '%McpService%')
        ORDER BY event_time DESC LIMIT 30"""))
except Exception as e:
    print(f"⚠️  No puedes leer system.access.audit: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control 5 — Service policies (Beta): una regla PLD sobre cada llamada
# MAGIC
# MAGIC La selección de herramientas dice **qué** herramientas existen. Una **service policy** evalúa **cada llamada**
# MAGIC (herramienta + argumentos + quién llama) antes de que se ejecute (`request`) y su resultado (`response`), y decide
# MAGIC `ALLOW`, `DENY` o `ASK` (pedir aprobación humana). Hay políticas listas (contenido inseguro, jailbreak,
# MAGIC **datos sensibles**) y políticas propias como **función SQL** de Unity Catalog.
# MAGIC
# MAGIC Nuestra política PLD, en una función:
# MAGIC - **Nadie** cierra casos por esta vía (defensa en profundidad, aunque `cerrar_caso` esté expuesta).
# MAGIC - Un caso **no** puede registrarse con datos de identificación en el resumen (minimización de datos).
# MAGIC
# MAGIC > La función se escribe en un SQL **restringido** (se traduce a CEL): `CASE`, comparaciones, `LIKE`, `CONTAINS`,
# MAGIC > `LOWER`, `COALESCE`, acceso `event:...`; **sin** expresiones regulares ni subconsultas. Para lógica más rica
# MAGIC > existen las políticas en Python (otro preview).

# COMMAND ----------

POLITICA = f"{CATALOG}.{SCHEMA}.politica_servicios_pld"
spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.politica_servicios_pld(event VARIANT)
RETURNS VARIANT
LANGUAGE SQL
COMMENT 'Service policy PLD: bloquea el cierre de casos por agentes y los resúmenes de caso con datos de identificación.'
RETURN
  CASE
    WHEN event:type::string = 'request'
         AND CAST(COALESCE(event:context.tool.name, '') AS STRING) = 'cerrar_caso'
      THEN to_variant_object(named_struct('result', 'DENY',
             'reason', 'Política PLD: solo un analista puede cerrar casos, nunca un agente.'))
    WHEN event:type::string = 'request'
         AND CAST(COALESCE(event:context.tool.name, '') AS STRING) = 'crear_caso'
         AND (CONTAINS(LOWER(CAST(COALESCE(event:context.tool.arguments.resumen, '') AS STRING)), 'cedula')
              OR CONTAINS(LOWER(CAST(COALESCE(event:context.tool.arguments.resumen, '') AS STRING)), 'cédula')
              OR CONTAINS(LOWER(CAST(COALESCE(event:context.tool.arguments.resumen, '') AS STRING)), 'pasaporte')
              OR CONTAINS(CAST(COALESCE(event:context.tool.arguments.resumen, '') AS STRING), '@'))
      THEN to_variant_object(named_struct('result', 'DENY',
             'reason', 'Política PLD: el resumen del caso no puede incluir documentos de identidad ni correos.'))
    ELSE to_variant_object(named_struct('result', 'ALLOW', 'reason', ''))
  END
""")
print(f"✅ Función de política: {POLITICA}")

# COMMAND ----------

# MAGIC %md
# MAGIC Antes de adjuntarla, la **probamos en SQL** con eventos simulados (esto funciona en cualquier workspace, con o
# MAGIC sin el preview):

# COMMAND ----------

eventos = {
    "screening (permitido)": {"type": "request", "context": {"tool": {"name": "screening_sanciones", "arguments": {"nombre": "X"}}}},
    "cerrar_caso":           {"type": "request", "context": {"tool": {"name": "cerrar_caso", "arguments": {"caso_id": "CASO-1"}}}},
    "crear_caso con cédula": {"type": "request", "context": {"tool": {"name": "crear_caso", "arguments": {
                                 "resumen": "Cliente con cédula 001-150385-0012K realizó depósitos fraccionados"}}}},
    "crear_caso limpio":     {"type": "request", "context": {"tool": {"name": "crear_caso", "arguments": {
                                 "resumen": "9 depósitos en efectivo bajo el umbral en 5 días"}}}},
    "respuesta de una tool": {"type": "response", "context": {"tool": {"name": "crear_caso"}}},
}
for nombre, ev in eventos.items():
    d = json.loads(spark.sql(f"SELECT to_json({FQ}.politica_servicios_pld(parse_json(:e))) AS d",
                             args={"e": json.dumps(ev, ensure_ascii=False)}).first().d)
    print(f"{nombre:24s} → {d['result']:5s} {d['reason']}")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Adjuntar la política (UI · Beta · requiere el preview *Unity Gateway Service Policies*)
# MAGIC
# MAGIC <div style="border-left: 4px solid #FF3621; background: #FFF5F2; padding: 12px 16px; border-radius: 4px;">
# MAGIC
# MAGIC 1. Barra lateral ▸ **Unity Gateway** ▸ pestaña **MCPs** ▸ abre tu servicio <code>servicios_pld_agente</code>.
# MAGIC 2. Pestaña **Policies** ▸ **New policy** ▸ nombre <code>politica_pld</code>.
# MAGIC 3. **Guardrail type: Custom** ▸ **Implementation: SQL function** ▸ elige <code>politica_servicios_pld</code> de tu schema.
# MAGIC 4. **Rank** 1 ▸ **Create policy**. Espera ~1 min y ejecuta la celda siguiente.
# MAGIC
# MAGIC Si no ves la pestaña **Policies**, el preview no está activo: el facilitador lo muestra en su workspace.
# MAGIC </div>

# COMMAND ----------

if conexion_ok:
    for intento in range(8):
        try:
            r = mi_cliente.llamar("crear_caso", {
                "alerta_id": "AT-005", "cliente_id": "CT-005", "disposicion_recomendada": "REPORTAR_ROS",
                "resumen": "Cliente con cédula 0801-1985-01234 realizó 6 depósitos en efectivo bajo el umbral."})
            print("⚠️  El caso se registró: la política aún no está adjunta (o no se ha propagado).\n  ",
                  json.dumps(r, ensure_ascii=False)[:300])
        except MCPError as e:
            if e.codigo == -32005 or e.http == 429:      # el tope de 3/min del control 3 todavía se está retirando
                print(f"  intento {intento + 1}: rate limit todavía activo (429), reintento en 10 s...")
                time.sleep(10)
                continue
            print("✅ Bloqueado por la service policy →", e)
        break

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC | Control | Lo que hiciste |
# MAGIC |---|---|
# MAGIC | Herramientas permitidas | Tu servicio expone 4 de 6 herramientas; `cerrar_caso` ni siquiera aparece |
# MAGIC | Permisos | `EXECUTE` sobre el MCP service; `USE CONNECTION` solo para quien registra |
# MAGIC | Rate limit | 3 QPM → 429 → de vuelta a 60 QPM |
# MAGIC | Uso y auditoría | Quién llamó qué herramienta, con qué resultado, aunque el sistema externo vea una cuenta técnica |
# MAGIC | Service policy | Regla PLD como función SQL, probada con eventos simulados y adjunta en la UI (Beta) |
# MAGIC
# MAGIC ➡️ En el `04` el Agente de Alertas usa **tu** MCP service.
