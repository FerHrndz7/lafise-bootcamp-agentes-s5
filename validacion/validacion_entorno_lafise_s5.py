# Databricks notebook source
# MAGIC %md
# MAGIC # Validación de entorno — Workshop de Agentes AML (Sesión 5: MCP + Unity AI Gateway)
# MAGIC
# MAGIC Ejecuta este notebook en el **workspace de desarrollo de LAFISE**, con un **clúster clásico** adjunto
# MAGIC (DBR **17.3 LTS**, modo *Standard*), para confirmar que el entorno soporta el laboratorio de la Sesión 5.
# MAGIC
# MAGIC Prueba cada pieza que usa la sesión (MCP de Databricks, Unity Gateway, registro y gobierno de un MCP service,
# MAGIC Databricks Apps para el servidor de "servicios PLD", tablas de sistema y service policies) y reporta
# MAGIC **PASA / FALLA / ALTERNATIVA / REVISAR** por requisito. Al final borra lo que creó.
# MAGIC
# MAGIC ## Instrucciones
# MAGIC 1. Adjunta un **clúster clásico** DBR 17.3 LTS. Idealmente ejecútalo con un usuario **sin permisos de admin**
# MAGIC    (como un participante). Los requisitos 6 y 7a son del **facilitador/admin**: si fallan para un participante es
# MAGIC    normal; repítelos con el usuario que preparará los servicios PLD.
# MAGIC 2. En el widget **catalog** escribe el catálogo de los asistentes (el de las Sesiones 1 a 4).
# MAGIC 3. **Run all** (~6 min). Comparte la **tabla resumen final** con el facilitador.
# MAGIC
# MAGIC | Estado | Significado |
# MAGIC |---|---|
# MAGIC | **PASA** | Funciona tal cual en la sesión |
# MAGIC | **ALTERNATIVA** | No está disponible, pero el laboratorio tiene un camino alternativo (se indica cuál) |
# MAGIC | **REVISAR** | Requiere una verificación manual (se indica cuál) |
# MAGIC | **FALLA** | Bloqueante: hay que resolverlo con el administrador antes de la sesión |

# COMMAND ----------

# MAGIC %pip install --quiet "databricks-mcp==0.9.2" "databricks-langchain==0.22.0" "langgraph==1.2.20" "mcp>=1.24,<2" "mlflow[databricks]>=3.17" "unitycatalog-langchain[databricks]>=0.3.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catálogo (requerido)")
dbutils.widgets.text("schema", "validacion_s5", "Schema de prueba")
dbutils.widgets.text("schema_servicios", "pld_servicios_mcp", "Schema de los servicios PLD (si ya se prepararon)")

import json, re, time, uuid
import requests
resultados = {}

def marcar(req, estado, detalle=""):
    resultados[req] = (estado, str(detalle)[:300])
    print(f"[{estado}] {req} — {detalle}")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip() or "validacion_s5"
assert CATALOG, "Escribe el catálogo en el widget 'catalog' antes de ejecutar."
FQ = f"`{CATALOG}`.{SCHEMA}"
SCHEMA_SERV = dbutils.widgets.get("schema_servicios").strip() or "pld_servicios_mcp"
usuario = spark.sql("SELECT current_user()").collect()[0][0]
SUF = uuid.uuid4().hex[:6]

from importlib.metadata import version
from databricks.sdk import WorkspaceClient
WS_URL = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
# En clúster clásico el cliente por defecto apunta al host regional (allí /ai-gateway responde 404).
w = WorkspaceClient(host=WS_URL, token=TOKEN)
marcar("0. Librerías instaladas desde PyPI (versiones fijas)", "PASA",
       f"mcp {version('mcp')}, databricks-mcp {version('databricks-mcp')}, databricks-langchain {version('databricks-langchain')}")

def mcp_rpc(url, method, params=None):
    cuerpo = {"jsonrpc": "2.0", "id": 1, "method": method, **({"params": params} if params is not None else {})}
    r = requests.post(url, json=cuerpo, timeout=120, headers={"Authorization": f"Bearer {TOKEN}",
                      "Content-Type": "application/json", "Accept": "application/json, text/event-stream"})
    texto = r.text
    if "data:" in texto and not texto.lstrip().startswith("{"):
        texto = [l[5:].strip() for l in texto.splitlines() if l.startswith("data:")][-1]
    try:
        msg = json.loads(texto)
    except Exception:
        raise RuntimeError(f"HTTP {r.status_code}: {texto[:200] or r.reason}")
    if "error" in msg:
        raise RuntimeError(f"HTTP {r.status_code} · MCP {msg['error'].get('code')}: {msg['error'].get('message')}")
    return msg["result"]

# COMMAND ----------

try:
    dbr = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
    marcar("1. Clúster clásico y runtime", "PASA" if dbr.startswith("17.") else "ALTERNATIVA", dbr)
except Exception as e:
    marcar("1. Clúster clásico y runtime", "ALTERNATIVA", f"No legible: {e}")

# COMMAND ----------

LLM = None
try:
    from databricks_langchain import ChatDatabricks
    from langchain_core.tools import tool
    servidos = {e.name for e in w.serving_endpoints.list()}
    LLM = next(c for c in ["databricks-claude-sonnet-4-6", "databricks-claude-sonnet-4-5", "databricks-claude-sonnet-5-5",
                           "databricks-gpt-5-4-mini", "databricks-meta-llama-3-3-70b-instruct",
                           "databricks-llama-4-maverick"] if c in servidos)

    @tool
    def screening(nombre: str) -> str:
        """Coteja un nombre contra listas de sanciones."""
        return "sin coincidencias"

    r = ChatDatabricks(endpoint=LLM, temperature=0).bind_tools([screening]).invoke(
        "Verifica en listas a 'Ramiro Quintanilla Rojas'. Usa la herramienta.")
    marcar("2. Modelo con tool calling (Foundation Model APIs)", "PASA" if r.tool_calls else "FALLA",
           f"{LLM} · tool_calls={bool(r.tool_calls)}")
except Exception as e:
    marcar("2. Modelo con tool calling (Foundation Model APIs)", "FALLA", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Gateway y MCP de Databricks (notebook 01)

# COMMAND ----------

try:
    lista = w.api_client.do("GET", "/api/2.1/unity-catalog/mcp-services", query={"parent": "schemas/system.ai"})
    nombres = sorted(s["name"].split("/")[-1] for s in lista.get("mcp_services", []))
    tools = mcp_rpc(f"{WS_URL}/ai-gateway/mcp-services/system.ai.genie_one_mcp", "tools/list")["tools"]
    marcar("3. Unity Gateway: MCP de system.ai (genie_one_mcp, GA)", "PASA",
           f"{len(nombres)} MCP en system.ai · genie_one_mcp → {[t['name'] for t in tools]}")
except Exception as e:
    marcar("3. Unity Gateway: MCP de system.ai (genie_one_mcp, GA)", "FALLA",
           f"{str(e)[:180]} · Sin Unity Gateway los notebooks 01-04 usan solo el modo en memoria")

# COMMAND ----------

try:
    r = mcp_rpc(f"{WS_URL}/ai-gateway/mcp-services/system.ai.dbsql", "tools/call",
                {"name": "execute_sql", "arguments": {"query": "SELECT 1 AS ok"}})
    marcar("4. MCP system.ai.dbsql (Beta) con execute_sql", "PASA" if not r.get("isError") else "ALTERNATIVA",
           json.dumps(r.get("structuredContent") or r.get("content"), ensure_ascii=False)[:120])
except Exception as e:
    marcar("4. MCP system.ai.dbsql (Beta) con execute_sql", "ALTERNATIVA",
           f"{str(e)[:160]} · Requiere el preview 'Enhanced Unity Gateway' (cuenta). El paso 2 del 01 se omite")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {FQ}.saludo_validacion(nombre STRING COMMENT 'Nombre a saludar')
              RETURNS STRING COMMENT 'Función de prueba de la validación S5' RETURN concat('hola ', nombre)""")
URL_UC = f"{WS_URL}/api/2.0/mcp/functions/{CATALOG}/{SCHEMA}"
try:
    tools = mcp_rpc(URL_UC, "tools/list")["tools"]
    try:
        r = mcp_rpc(URL_UC, "tools/call", {"name": tools[0]["name"], "arguments": {"nombre": "LAFISE"}})
        marcar("5. Endpoint MCP de funciones UC (Public Preview)", "PASA",
               f"tools/list + tools/call → {json.dumps(r.get('content'), ensure_ascii=False)[:100]}")
    except Exception as e:
        marcar("5. Endpoint MCP de funciones UC (Public Preview)", "ALTERNATIVA",
               f"tools/list OK; tools/call falla ({str(e)[:120]}): ejecuta en serverless. El 01 lo muestra sin ejecutar")
except Exception as e:
    marcar("5. Endpoint MCP de funciones UC (Public Preview)", "ALTERNATIVA",
           f"{str(e)[:150]} · Requiere el preview 'Managed MCP Servers' (workspace). El paso 1 del 01 se omite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Apps y conexión (preparación del facilitador)

# COMMAND ----------

APP_PRUEBA = f"mcp-validacion-s5-{SUF}"
try:
    w.api_client.do("POST", "/api/2.0/apps", query={"no_compute": "true"},
                    body={"name": APP_PRUEBA, "description": "Validación S5 (se borra)"})
    marcar("6. [Facilitador] Crear una Databricks App", "PASA", "create (sin cómputo); se borra al final")
except Exception as e:
    marcar("6. [Facilitador] Crear una Databricks App", "FALLA",
           f"{str(e)[:180]} · Sin Apps no hay servidor de servicios PLD: los notebooks usan el modo en memoria y el "
           "03 se presenta como demo")

# COMMAND ----------

CONN = f"{CATALOG}.{SCHEMA}.conn_validacion"
conn_ok = False
try:
    w.api_client.do("POST", "/api/2.1/unity-catalog/connections", body={
        "name": "conn_validacion", "parent": f"schemas/{CATALOG}.{SCHEMA}", "connection_type": "HTTP",
        "options": {"host": "https://example.com", "port": "443", "base_path": "/mcp", "bearer_token": "token-de-prueba"}})
    conn_ok = True
    marcar("7a. [Facilitador] CREATE CONNECTION (HTTP) en un schema", "PASA", CONN)
except Exception as e:
    marcar("7a. [Facilitador] CREATE CONNECTION (HTTP) en un schema", "FALLA",
           f"{str(e)[:180]} · El facilitador necesita CREATE CONNECTION en el schema compartido")

# COMMAND ----------

MCP_PRUEBA = f"{CATALOG}.{SCHEMA}.mcp_validacion"
if conn_ok:
    try:
        w.api_client.do("POST", "/api/2.1/unity-catalog/mcp-services",
                        query={"parent": f"schemas/{CATALOG}.{SCHEMA}", "mcp_service_id": "mcp_validacion"},
                        body={"comment": "validación S5", "config": {
                            "source_connection": {"name": f"connections/{CONN}"},
                            "include_tool_selectors": ["consultar_*", "screening_*"]}})
        w.api_client.do("PATCH", f"/api/2.1/unity-catalog/mcp-services/{MCP_PRUEBA}",
                        query={"update_mask": "config.rate_limits"},
                        body={"config": {"rate_limits": [{"key": "RATE_LIMIT_KEY_SERVICE",
                              "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE", "requests": 60}]}})
        cfg = w.api_client.do("GET", f"/api/2.1/unity-catalog/mcp-services/{MCP_PRUEBA}")["config"]
        marcar("7b. Registrar un MCP service (CREATE SERVICE) + tools + rate limit", "PASA",
               f"tools={cfg.get('include_tool_selectors')} · rate_limits={len(cfg.get('rate_limits') or [])}")
    except Exception as e:
        marcar("7b. Registrar un MCP service (CREATE SERVICE) + tools + rate limit", "FALLA",
               f"{str(e)[:180]} · Cada participante necesita CREATE SERVICE en su schema (el dueño lo tiene)")
else:
    marcar("7b. Registrar un MCP service (CREATE SERVICE) + tools + rate limit", "REVISAR",
           "No se pudo probar sin conexión (7a). Repetir con el usuario del facilitador")

# COMMAND ----------

MCP_SERV = f"{CATALOG}.{SCHEMA_SERV}.servicios_pld"
try:
    tools = mcp_rpc(f"{WS_URL}/ai-gateway/mcp-services/{MCP_SERV}", "tools/list")["tools"]
    marcar("8. Servicios PLD del facilitador por Unity Gateway", "PASA", f"{MCP_SERV} → {[t['name'] for t in tools]}")
except Exception as e:
    marcar("8. Servicios PLD del facilitador por Unity Gateway", "REVISAR",
           f"{str(e)[:150]} · Normal si aún no se corrió facilitador/00_preparar_servicios_pld")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clientes MCP en Python (notebooks 01 y 04)

# COMMAND ----------

try:
    from databricks_langchain import DatabricksMCPServer, DatabricksMultiServerMCPClient
    herr = await DatabricksMultiServerMCPClient([DatabricksMCPServer(
        name="genie", url=f"{WS_URL}/ai-gateway/mcp-services/system.ai.genie_one_mcp", workspace_client=w)]).get_tools()
    marcar("9. Cliente oficial DatabricksMultiServerMCPClient (async)", "PASA", [h.name for h in herr])
except Exception as e:
    marcar("9. Cliente oficial DatabricksMultiServerMCPClient (async)", "ALTERNATIVA",
           f"{str(e)[:180]} · El laboratorio usa su propio cliente JSON-RPC")

# COMMAND ----------

try:
    import asyncio, threading
    from mcp.server.fastmcp import FastMCP
    from mcp.shared.memory import create_connected_server_and_client_session
    srv = FastMCP(name="validacion", log_level="WARNING")

    @srv.tool()
    def eco(texto: str) -> dict:
        """Devuelve el texto."""
        return {"eco": texto}

    async def _go():
        async with create_connected_server_and_client_session(srv._mcp_server) as s:
            return (await s.call_tool("eco", {"texto": "ok"})).model_dump(mode="json")
    salida = {}
    t = threading.Thread(target=lambda: salida.setdefault("r", asyncio.run(_go()))); t.start(); t.join()
    marcar("10. Servidor MCP en memoria (plan B de los servicios PLD)", "PASA", salida["r"].get("content"))
except Exception as e:
    marcar("10. Servidor MCP en memoria (plan B de los servicios PLD)", "FALLA", e)

# COMMAND ----------

try:
    from unitycatalog.ai.core.databricks import DatabricksFunctionClient
    r = DatabricksFunctionClient().execute_function(f"{CATALOG}.{SCHEMA}.saludo_validacion", {"nombre": "LAFISE"})
    assert not r.error, r.error
    marcar("11. Funciones UC en serverless (DatabricksFunctionClient)", "PASA", r.value)
except Exception as e:
    marcar("11. Funciones UC en serverless (DatabricksFunctionClient)", "ALTERNATIVA",
           f"{str(e)[:150]} · El agente usa spark.sql en el clúster clásico (automático, como en la S2)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auditoría y service policies (notebook 03)

# COMMAND ----------

for req, sql in [
    ("12. Tabla system.ai_gateway.usage", "SELECT count(*) n FROM system.ai_gateway.usage WHERE service_type = 'MCP_SERVICE' "
                                          "AND event_time >= current_timestamp() - INTERVAL 7 DAYS"),
    ("13. Tabla system.access.audit (mcpCall)", "SELECT count(*) n FROM system.access.audit WHERE event_date >= current_date() - 7 "
                                                "AND action_name = 'mcpCall'"),
]:
    try:
        n = spark.sql(sql).first().n
        marcar(req, "PASA", f"{n} registros MCP en 7 días")
    except Exception as e:
        marcar(req, "ALTERNATIVA", f"{str(e)[:150]} · Pedir SELECT sobre la tabla; si no, el facilitador la muestra")

# COMMAND ----------

try:
    spark.sql(f"""CREATE OR REPLACE FUNCTION {FQ}.politica_validacion(event VARIANT) RETURNS VARIANT LANGUAGE SQL RETURN
      CASE WHEN event:type::string = 'request' AND CAST(COALESCE(event:context.tool.name, '') AS STRING) = 'cerrar_caso'
           THEN to_variant_object(named_struct('result', 'DENY', 'reason', 'prueba'))
           ELSE to_variant_object(named_struct('result', 'ALLOW', 'reason', '')) END""")
    d = spark.sql(f"""SELECT to_json({FQ}.politica_validacion(parse_json('{{"type":"request","context":{{"tool":{{"name":"cerrar_caso"}}}}}}'))) d""").first().d
    assert json.loads(d)["result"] == "DENY", d
    marcar("14. Función de service policy (VARIANT) creada y probada", "PASA", d)
    marcar("15. Adjuntar service policies a un MCP service (Beta, UI)", "REVISAR",
           "Un admin verifica en Settings ▸ Previews que 'Unity Gateway Service Policies' esté ON. Si no, el paso 5 del 03 es demo")
except Exception as e:
    marcar("14. Función de service policy (VARIANT) creada y probada", "ALTERNATIVA", f"{str(e)[:180]} · El paso 5 del 03 es demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpieza y resumen

# COMMAND ----------

def _borrar_app():
    for _ in range(30):                      # una app recién creada no se puede borrar mientras está STARTING
        try:
            w.api_client.do("DELETE", f"/api/2.0/apps/{APP_PRUEBA}")
            return
        except Exception as e:
            if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                return
            time.sleep(10)
    print(f"⚠️ No se pudo borrar la app {APP_PRUEBA}: bórrala a mano en Compute ▸ Apps")

for paso, f in [("app de prueba", _borrar_app), ("MCP service", lambda: w.api_client.do("DELETE", f"/api/2.1/unity-catalog/mcp-services/{MCP_PRUEBA}")),
                ("conexión", lambda: w.api_client.do("DELETE", f"/api/2.1/unity-catalog/connections/{CONN}")),
                ("schema", lambda: spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE"))]:
    try:
        f()
    except Exception as e:
        if "does not exist" not in str(e).lower() and "not found" not in str(e).lower():
            print(f"⚠️ Limpieza de {paso}: {str(e)[:150]}")
print("🧹 Limpieza completa")

print(f"\n{'Requisito':70s} {'Estado':12s} Detalle")
for req, (estado, det) in resultados.items():
    print(f"{req:70s} {estado:12s} {det[:110]}")
