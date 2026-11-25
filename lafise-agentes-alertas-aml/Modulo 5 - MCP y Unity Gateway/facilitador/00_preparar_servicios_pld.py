# Databricks notebook source
# MAGIC %md
# MAGIC # Facilitador · Preparar los «servicios PLD» (servidor MCP compartido + Unity Gateway)
# MAGIC
# MAGIC **Lo corre una sola vez el facilitador o un admin del workspace, ANTES de la sesión** (~10 min). Los
# MAGIC participantes no lo ejecutan.
# MAGIC
# MAGIC Deja listo el "sistema externo" al que se conecta el agente en la Sesión 5:
# MAGIC
# MAGIC | Paso | Qué crea | Para qué |
# MAGIC |---|---|---|
# MAGIC | 1 | **Databricks App** `mcp-lafise-servicios-pld` | Servidor MCP con los servicios PLD **simulados**: screening de sanciones y PEP + gestión de casos |
# MAGIC | 2 | Permiso `CAN USE` del service principal sobre la app | El gateway entra a la app con la identidad del SP |
# MAGIC | 3 | Schema compartido + **conexión HTTP OAuth M2M** `conn_servicios_pld` | La credencial vive en Unity Catalog; nadie la ve |
# MAGIC | 4 | **MCP service** `servicios_pld` (del facilitador) | Prueba de punta a punta por Unity Gateway |
# MAGIC | 5 | Grants al grupo de participantes | `USE CONNECTION` (cada uno registra su propio MCP service) + `EXECUTE` sobre el del facilitador |
# MAGIC
# MAGIC ### Antes de empezar (lo hace un admin del workspace)
# MAGIC 1. Un **service principal** con un **secreto OAuth**, guardados en un *secret scope*:
# MAGIC
# MAGIC        databricks secrets create-scope lafise_s5_mcp
# MAGIC        databricks service-principals create --display-name lafise-s5-mcp-gateway        # anota id y applicationId
# MAGIC        databricks secrets put-secret lafise_s5_mcp sp_client_id --string-value <applicationId>
# MAGIC        databricks service-principal-secrets-proxy create <id> --lifetime 2592000s       # copia "secret"
# MAGIC        databricks secrets put-secret lafise_s5_mcp sp_client_secret                     # pega el secreto
# MAGIC
# MAGIC 2. **Databricks Apps** habilitado en el workspace y permiso para crear apps.
# MAGIC 3. En el catálogo del curso: `CREATE SCHEMA` (o un schema ya creado) y `CREATE CONNECTION` en ese schema.
# MAGIC
# MAGIC > **Estado del producto (noviembre 2026).** Unity Gateway y los MCP services están en **GA**. Registrar como
# MAGIC > MCP service un servidor que vive en **Databricks Apps** funciona con una conexión OAuth M2M, pero la
# MAGIC > documentación todavía lo marca como *no soportado oficialmente*. Por eso aquí
# MAGIC > se trata como un **servicio externo**: en producción, el servidor MCP lo publica el proveedor o el área de
# MAGIC > TI del banco, y el registro en Unity Gateway es idéntico.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catálogo del curso (REQUERIDO)")
dbutils.widgets.text("schema_compartido", "pld_servicios_mcp", "Schema compartido")
dbutils.widgets.text("app_name", "mcp-lafise-servicios-pld", "Nombre de la app (prefijo mcp-)")
dbutils.widgets.text("secret_scope", "lafise_s5_mcp", "Secret scope del SP")
dbutils.widgets.text("grupo", "account users", "Grupo de participantes")
dbutils.widgets.dropdown("redesplegar", "no", ["no", "si"], "¿Redesplegar el código de la app?")

CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe el catálogo del curso en el widget 'catalog'."
SCHEMA_C = dbutils.widgets.get("schema_compartido").strip()
APP = dbutils.widgets.get("app_name").strip()
SCOPE = dbutils.widgets.get("secret_scope").strip()
GRUPO = dbutils.widgets.get("grupo").strip()
CONEXION = f"{CATALOG}.{SCHEMA_C}.conn_servicios_pld"
MCP_FACILITADOR = f"{CATALOG}.{SCHEMA_C}.servicios_pld"

import json, time, requests
from databricks.sdk import WorkspaceClient

WS_URL = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
# En clúster clásico el cliente por defecto apunta al host regional; usamos la URL del workspace.
w = WorkspaceClient(host=WS_URL, token=TOKEN)
yo = w.current_user.me().user_name
print(f"Workspace: {WS_URL}\nUsuario:   {yo}\nConexión:  {CONEXION}\nMCP:       {MCP_FACILITADOR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Desplegar la Databricks App con el servidor MCP

# COMMAND ----------

# MAGIC %run ../utils/fuente_servidor_pld

# COMMAND ----------

from databricks.sdk.service.workspace import ImportFormat

# Usamos la API REST de Apps directamente: funciona igual con cualquier versión del SDK del runtime.
def app_get(nombre):
    try:
        return w.api_client.do("GET", f"/api/2.0/apps/{nombre}")
    except Exception:
        return None

def esperar(cond, desc, minutos=15):
    for _ in range(minutos * 6):
        estado = cond()
        if estado is True:
            return
        time.sleep(10)
    raise TimeoutError(f"Tiempo agotado esperando: {desc} (último estado: {estado})")

RUTA_CODIGO = f"/Workspace/Users/{yo}/lafise-s5-servidor-mcp"
w.workspace.mkdirs(RUTA_CODIGO)
for nombre, contenido in FUENTE_SERVIDOR_PLD.items():
    w.workspace.upload(f"{RUTA_CODIGO}/{nombre}", contenido.encode("utf-8"), format=ImportFormat.AUTO, overwrite=True)
print(f"✅ Código del servidor en {RUTA_CODIGO}")

desplegar = dbutils.widgets.get("redesplegar") == "si"
if app_get(APP) is None:
    print(f"Creando la app {APP} (2-4 min)...")
    w.api_client.do("POST", "/api/2.0/apps",
                    body={"name": APP, "description": "LAFISE S5 - servicios PLD simulados (servidor MCP)"})
    desplegar = True
def _compute():
    st = (app_get(APP).get("compute_status") or {}).get("state")
    return True if st == "ACTIVE" else st
if (app_get(APP).get("compute_status") or {}).get("state") in ("STOPPED", "ERROR"):
    print("La app estaba detenida: iniciándola...")
    w.api_client.do("POST", f"/api/2.0/apps/{APP}/start")
    desplegar = True
esperar(_compute, "cómputo de la app ACTIVE")
if desplegar:
    print("Desplegando el código (1-3 min)...")
    dep = w.api_client.do("POST", f"/api/2.0/apps/{APP}/deployments", body={"source_code_path": RUTA_CODIGO})
    def _deploy():
        st = w.api_client.do("GET", f"/api/2.0/apps/{APP}/deployments/{dep['deployment_id']}")["status"]
        if st["state"] == "FAILED":
            raise RuntimeError(f"El despliegue falló: {st.get('message')} (revisa los logs de la app)")
        return True if st["state"] == "SUCCEEDED" else st["state"]
    esperar(_deploy, "despliegue SUCCEEDED")
app = app_get(APP)
APP_URL = app["url"].rstrip("/")
APP_MCP_URL = APP_URL + "/mcp"
print(f"✅ App {APP}: {(app.get('app_status') or {}).get('state')} · MCP en {APP_MCP_URL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Permiso del service principal sobre la app

# COMMAND ----------

SP_CLIENT_ID = dbutils.secrets.get(SCOPE, "sp_client_id").strip()
sp = next(iter(w.service_principals.list(filter=f'applicationId eq "{SP_CLIENT_ID}"')), None)
assert sp, f"No encontré el service principal {SP_CLIENT_ID} en el workspace."
w.api_client.do("PATCH", f"/api/2.0/permissions/apps/{APP}",
                body={"access_control_list": [{"service_principal_name": SP_CLIENT_ID,
                                               "permission_level": "CAN_USE"}]})
print(f"✅ {sp.display_name} tiene CAN USE sobre {APP}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema compartido y conexión HTTP (OAuth M2M)
# MAGIC La conexión guarda el `client_id` y el secreto del SP. Unity Gateway pide un token OAuth al endpoint del
# MAGIC workspace y lo presenta a la app en cada llamada. Los participantes reciben `USE CONNECTION` para poder
# MAGIC **registrar** su propio MCP service, pero nunca ven el secreto.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA_C}` "
          f"COMMENT 'LAFISE S5: conexión y MCP service compartidos de los servicios PLD simulados'")

host = APP_URL
cuerpo = {
    "name": "conn_servicios_pld", "parent": f"schemas/{CATALOG}.{SCHEMA_C}", "connection_type": "HTTP",
    "comment": "Servicios PLD simulados (servidor MCP en Databricks Apps) - OAuth M2M con service principal",
    "options": {"host": host, "port": "443", "base_path": "/mcp",
                "client_id": SP_CLIENT_ID, "client_secret": dbutils.secrets.get(SCOPE, "sp_client_secret").strip(),
                "oauth_scope": "all-apis", "token_endpoint": f"{WS_URL}/oidc/v1/token"},
}
try:
    w.api_client.do("GET", f"/api/2.1/unity-catalog/connections/{CONEXION}")
    print(f"♻️  La conexión {CONEXION} ya existe (no se modifica)")
except Exception:
    w.api_client.do("POST", "/api/2.1/unity-catalog/connections", body=cuerpo)
    print(f"✅ Conexión {CONEXION} creada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MCP service del facilitador y prueba por Unity Gateway

# COMMAND ----------

def mcp_rpc(url, method, params=None, timeout=90):
    """Una llamada JSON-RPC de MCP (Streamable HTTP). Devuelve el objeto 'result' o lanza el error."""
    r = requests.post(url, timeout=timeout, headers={
        "Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream"},
        json={"jsonrpc": "2.0", "id": 1, "method": method, **({"params": params} if params else {})})
    texto = r.text
    if "data:" in texto:
        texto = [l[5:].strip() for l in texto.splitlines() if l.startswith("data:")][-1]
    if r.status_code >= 400 and not texto.strip().startswith("{"):
        raise RuntimeError(f"HTTP {r.status_code}: {texto[:300]}")
    msg = json.loads(texto)
    if "error" in msg:
        raise RuntimeError(f"MCP error: {msg['error']}")
    return msg["result"]

try:
    w.api_client.do("GET", f"/api/2.1/unity-catalog/mcp-services/{MCP_FACILITADOR}")
    print(f"♻️  El MCP service {MCP_FACILITADOR} ya existe")
except Exception:
    w.api_client.do("POST", "/api/2.1/unity-catalog/mcp-services",
                    query={"parent": f"schemas/{CATALOG}.{SCHEMA_C}", "mcp_service_id": "servicios_pld"},
                    body={"comment": "Servicios PLD simulados (todas las herramientas) - demo del facilitador",
                          "config": {"source_connection": {"name": f"connections/{CONEXION}"}}})
    print(f"✅ MCP service {MCP_FACILITADOR} creado")

GW_URL = f"{WS_URL}/ai-gateway/mcp-services/{MCP_FACILITADOR}"
for intento in range(6):
    try:
        herramientas = [t["name"] for t in mcp_rpc(GW_URL, "tools/list")["tools"]]
        break
    except Exception as e:
        print(f"  intento {intento + 1}: {str(e)[:200]}")
        time.sleep(15)
else:
    raise RuntimeError("Unity Gateway no pudo listar las herramientas. Revisa el SP, el CAN USE y la conexión.")
print("✅ Herramientas por Unity Gateway:", herramientas)
r = mcp_rpc(GW_URL, "tools/call", {"name": "estado_servicio", "arguments": {}})
print("   estado_servicio →", r.get("structuredContent") or r["content"][0]["text"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Permisos para los participantes

# COMMAND ----------

def otorgar(tipo, nombre, privilegios):
    try:
        w.api_client.do("PATCH", f"/api/2.1/unity-catalog/permissions/{tipo}/{nombre}",
                        body={"changes": [{"principal": GRUPO, "add": privilegios}]})
        print(f"✅ {privilegios} sobre {tipo} {nombre} para {GRUPO}")
    except Exception as e:
        print(f"⚠️  {privilegios} sobre {nombre}: {str(e)[:200]}")

otorgar("schema", f"{CATALOG}.{SCHEMA_C}", ["USE_SCHEMA"])
# USE CONNECTION: cada participante registra SU propio MCP service sobre esta conexión (notebook 03).
otorgar("connection", CONEXION, ["USE_CONNECTION"])
otorgar("mcp_service", MCP_FACILITADOR, ["EXECUTE"])
# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen (copia estos valores para la sesión)

# COMMAND ----------

print("=" * 70)
print("SERVICIOS PLD LISTOS ✅")
print("=" * 70)
print(f"App (servidor MCP)        : {APP}  ->  {APP_MCP_URL}")
print(f"Conexión (widget del 00)  : {CONEXION}")
print(f"MCP service del facilitador: {MCP_FACILITADOR}")
print(f"URL por Unity Gateway     : {GW_URL}")
print(f"Grupo con acceso          : {GRUPO}")
print("\nAl terminar el bootcamp: detén la app (Compute > Apps) y borra la conexión y el schema compartido.")
