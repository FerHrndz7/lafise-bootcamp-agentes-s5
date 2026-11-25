# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · ¿Qué es MCP? El protocolo en vivo con los MCP de Databricks
# MAGIC
# MAGIC **⏱️ ~12 min** · **Objetivos:** entender las dos operaciones centrales de MCP (`tools/list` y `tools/call`),
# MAGIC verlas viajar por la red y distinguir los dos tipos de MCP que ofrece Databricks.
# MAGIC
# MAGIC ## El problema que resuelve MCP
# MAGIC En la Sesión 2 conectamos el agente a sus herramientas **a mano**: cada función de UC se envolvió en código de
# MAGIC LangChain. Si mañana el mismo screening de sanciones lo quiere usar un Genie Agent, Claude Code o un asistente de
# MAGIC otro equipo, cada uno tendría que repetir esa integración. Con *N* agentes y *M* sistemas son *N × M*
# MAGIC integraciones.
# MAGIC
# MAGIC **MCP (*Model Context Protocol*)** es un estándar abierto: el sistema publica sus herramientas **una vez** como
# MAGIC *servidor MCP* y cualquier agente (*cliente MCP*) las descubre y las llama de la misma forma.
# MAGIC
# MAGIC     Cliente MCP (agente)                         Servidor MCP (sistema)
# MAGIC        │  tools/list  ─────────────────────────▶   "tengo estas herramientas, con estos argumentos"
# MAGIC        │  tools/call {name, arguments} ────────▶   ejecuta y devuelve el resultado
# MAGIC
# MAGIC Por debajo son mensajes **JSON-RPC 2.0** sobre HTTPS (*Streamable HTTP*). Nada más.
# MAGIC
# MAGIC ## Dos familias de MCP en Databricks
# MAGIC | | Endpoints MCP del workspace | MCP services de **Unity Gateway** |
# MAGIC |---|---|---|
# MAGIC | Ejemplos | `/api/2.0/mcp/functions/{catálogo}/{schema}`, `/api/2.0/mcp/ai-search/...`, `/api/2.0/mcp/genie/{id}` | `system.ai.genie_one_mcp`, `system.ai.dbsql`, y los que **registras tú** |
# MAGIC | URL | `https://<workspace>/api/2.0/mcp/...` | `https://<workspace>/ai-gateway/mcp-services/<catálogo>.<schema>.<servicio>` |
# MAGIC | Gobierno | Permisos del recurso (función, índice, Genie Agent) | Además: `EXECUTE` sobre el MCP, herramientas permitidas, rate limits, políticas, uso y trazas |
# MAGIC | Estado | Public Preview (preview *Managed MCP Servers*) | **GA** (`dbsql` en Beta) |

# COMMAND ----------

# MAGIC %pip install --quiet "databricks-mcp==0.9.2" "databricks-langchain==0.22.0" "langgraph==1.2.20" "mcp>=1.24,<2" "mlflow[databricks]>=3.17" "unitycatalog-langchain[databricks]>=0.3.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import re, json

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "pld_aml", "Schema base")
user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe en el widget 'catalog' el mismo catálogo que usaste en 00_setup."
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'pld_aml')}_{user_prefix}"
FQ = f"`{CATALOG}`.{SCHEMA}"

# COMMAND ----------

# MAGIC %run ./utils/agente_mcp_utils

# COMMAND ----------

w = cliente_ws()
print("Workspace:", ws_url())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Tus funciones de UC ya son un servidor MCP
# MAGIC
# MAGIC Databricks publica **automáticamente** como servidor MCP todas las funciones de un schema. La URL es
# MAGIC `/api/2.0/mcp/functions/<catálogo>/<schema>` (o `.../<función>` para una sola). Sin escribir ni desplegar nada,
# MAGIC las funciones de la Sesión 2 (`obtener_alerta`, `features_transaccionales`, `verificar_sanciones`) quedan
# MAGIC disponibles para **cualquier** cliente MCP.
# MAGIC
# MAGIC Primero el saludo del protocolo: `initialize`. El cliente dice qué versión de MCP habla y el servidor responde
# MAGIC con su nombre y sus capacidades. Con `ver=True` imprimimos el mensaje que viaja por la red.

# COMMAND ----------

URL_UC = f"{ws_url()}/api/2.0/mcp/functions/{CATALOG}/{SCHEMA}"
print("Servidor MCP de tus funciones:", URL_UC, "\n")
try:
    saludo = mcp_rpc(URL_UC, "initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                            "clientInfo": {"name": "notebook-lafise", "version": "1.0"}}, ver=True)
    print("\nServidor:", saludo.get("serverInfo"), "· protocolo:", saludo.get("protocolVersion"))
    MCP_UC_OK = True
except MCPError as e:
    MCP_UC_OK = False
    print(f"⚠️  {e}\n   El endpoint MCP de funciones requiere el preview 'Managed MCP Servers'. Sigue con el Paso 3.")

# COMMAND ----------

# MAGIC %md
# MAGIC Ahora **`tools/list`**: el agente pregunta "¿qué puedes hacer?". Fíjate en que el nombre, la descripción y los
# MAGIC parámetros salen **del COMMENT de la función y de sus parámetros** en Unity Catalog. Documentar bien las funciones
# MAGIC es, literalmente, escribir el *prompt* de la herramienta.

# COMMAND ----------

if MCP_UC_OK:
    uc = ClienteMCP(URL_UC, "funciones UC")
    import pandas as pd
    display(pd.DataFrame(tabla_herramientas(uc)))
    print(json.dumps(uc.herramientas()[0]["inputSchema"], ensure_ascii=False, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC Y **`tools/call`**: el agente pide ejecutar una herramienta con argumentos. Los nombres llevan el prefijo
# MAGIC `catálogo__schema__función` para que no choquen entre servidores.
# MAGIC
# MAGIC > **Cómputo.** Este endpoint ejecuta la función en **serverless** (cómputo general de Databricks). Si tu
# MAGIC > workspace no lo permite, `tools/list` funciona pero `tools/call` devuelve un error: es el mismo límite que vimos
# MAGIC > en la Sesión 2, y por eso el agente de hoy ejecuta sus funciones de UC con `spark.sql` en el clúster clásico.

# COMMAND ----------

if MCP_UC_OK:
    herramienta = next(t["name"] for t in uc.herramientas() if t["name"].endswith("obtener_alerta"))
    try:
        r = mcp_rpc(URL_UC, "tools/call", {"name": herramienta, "arguments": {"alerta_id": "AT-003"}}, ver=True)
        print("\nResultado:", json.dumps(_resultado(r), ensure_ascii=False)[:600])
    except MCPError as e:
        print(f"⚠️  tools/call no disponible aquí: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Los MCP de Databricks gobernados por Unity Gateway (`system.ai`)
# MAGIC
# MAGIC Databricks también ofrece MCP listos para usar como **objetos de Unity Catalog** en el schema `system.ai`. Se
# MAGIC llaman por **Unity Gateway** (`/ai-gateway/mcp-services/system.ai.<nombre>`), así que heredan su gobierno: hay
# MAGIC que tener `EXECUTE`, y cada llamada queda en las tablas de uso y auditoría.
# MAGIC
# MAGIC | MCP | Para qué |
# MAGIC |---|---|
# MAGIC | `system.ai.genie_one_mcp` (GA) | Preguntas de negocio en lenguaje natural sobre los datos del workspace |
# MAGIC | `system.ai.dbsql` (Beta) | Ejecutar SQL concreto con los permisos de quien llama |
# MAGIC | `system.ai.slack`, `github`, `microsoft_365`, ... | Sistemas externos, cada usuario inicia sesión con su cuenta |
# MAGIC
# MAGIC Listamos los MCP de `system.ai` que existen en el workspace (es la misma API con la que registrarás el tuyo en
# MAGIC el `03`):

# COMMAND ----------

lista = w.api_client.do("GET", "/api/2.1/unity-catalog/mcp-services", query={"parent": "schemas/system.ai"})
print("MCP de Databricks en system.ai:", sorted(s["name"].split("/")[-1] for s in lista.get("mcp_services", [])))

# COMMAND ----------

# MAGIC %md
# MAGIC `system.ai.dbsql` expone la herramienta `execute_sql`. La llamamos para contar las alertas testigo: el MCP usa
# MAGIC **tus** permisos de Unity Catalog (si no puedes leer la tabla, el MCP tampoco).

# COMMAND ----------

dbsql = ClienteMCP(url_mcp_service("system.ai.dbsql"), "system.ai.dbsql")
try:
    print("Herramientas:", [t["name"] for t in dbsql.herramientas()])
    r = dbsql.llamar("execute_sql", {"query": f"SELECT tipologia, count(*) AS alertas FROM {CATALOG}.{SCHEMA}.alertas "
                                              f"WHERE alerta_id LIKE 'AT-%' GROUP BY tipologia ORDER BY alertas DESC"})
    print("Estado:", r["status"]["state"], "· statement_id:", r["statement_id"])
    print(filas_dbsql(r))
except MCPError as e:
    print(f"⚠️  system.ai.dbsql no disponible: {e}\n   (Beta: requiere el preview 'Enhanced Unity Gateway' de la cuenta.)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — El cliente oficial en Python
# MAGIC
# MAGIC En un agente real no armamos JSON-RPC a mano: usamos el cliente de Databricks, que resuelve la autenticación y
# MAGIC convierte las herramientas MCP en herramientas del framework. `DatabricksMultiServerMCPClient` (de
# MAGIC `databricks-langchain`) se conecta a **varios servidores a la vez**. Es asíncrono: en un notebook se usa con
# MAGIC `await`.

# COMMAND ----------

from databricks_langchain import DatabricksMCPServer, DatabricksMultiServerMCPClient

servidores = [DatabricksMCPServer(name="dbsql", url=url_mcp_service("system.ai.dbsql"), workspace_client=w)]
if MCP_UC_OK:
    servidores.append(DatabricksMCPServer(name="funciones_uc", url=URL_UC, workspace_client=w))
try:
    herramientas_oficiales = await DatabricksMultiServerMCPClient(servidores).get_tools()
    for h in herramientas_oficiales:
        print(f"  {h.name:70s} {h.description[:60]!r}")
except Exception as e:
    print(f"⚠️  {type(e).__name__}: {str(e)[:300]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC - Un servidor MCP **anuncia** herramientas (`tools/list`) y las **ejecuta** (`tools/call`). Son mensajes JSON-RPC
# MAGIC   sobre HTTPS.
# MAGIC - Tus funciones de UC ya son un servidor MCP (endpoint del workspace, Public Preview, sin gateway).
# MAGIC - Los MCP de `system.ai` pasan por **Unity Gateway**: permisos, uso y auditoría en un solo lugar.
# MAGIC - El cliente oficial (`DatabricksMultiServerMCPClient`) junta varios servidores para un agente.
# MAGIC
# MAGIC ➡️ En el `02` conectamos el agente al **mundo exterior**: los servicios PLD del banco.
