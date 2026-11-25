# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Los servicios PLD del banco por MCP
# MAGIC
# MAGIC **⏱️ ~13 min** · **Objetivos:** consumir un servidor MCP **propio** que representa sistemas externos al
# MAGIC lakehouse, entender cómo se define una herramienta MCP y quién es "el que llama" cuando pasa por el gateway.
# MAGIC
# MAGIC ## El escenario
# MAGIC En las sesiones anteriores la lista de sanciones era una **tabla** de nuestro lakehouse. En la Unidad PLD real:
# MAGIC
# MAGIC | Sistema | Dónde vive | Qué hace |
# MAGIC |---|---|---|
# MAGIC | **Proveedor de listas** | Fuera del banco (servicio contratado) | Screening contra OFAC, ONU, UAF y registro PEP, siempre actualizado |
# MAGIC | **Gestión de casos PLD** | Sistema interno del banco | Historial de casos por cliente; registro de casos nuevos para el analista |
# MAGIC
# MAGIC Para el laboratorio, ambos están **simulados** en un servidor MCP que el facilitador desplegó en una
# MAGIC **Databricks App** y registró en **Unity Gateway** como `servicios_pld`. Desde el punto de vista del agente da
# MAGIC igual dónde corra: en producción, el servidor lo publicaría el proveedor o el área de TI y el registro sería el
# MAGIC mismo.

# COMMAND ----------

# MAGIC %pip install --quiet "databricks-mcp==0.9.2" "databricks-langchain==0.22.0" "langgraph==1.2.20" "mcp>=1.24,<2" "mlflow[databricks]>=3.17" "unitycatalog-langchain[databricks]>=0.3.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import re, json

dbutils.widgets.text("catalog", "", "Catálogo asignado (REQUERIDO)")
dbutils.widgets.text("schema", "pld_aml", "Schema base")
dbutils.widgets.text("schema_servicios", "pld_servicios_mcp", "Schema de los servicios PLD")
user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
assert CATALOG, "Escribe en el widget 'catalog' el mismo catálogo que usaste en 00_setup."
SCHEMA = f"{(dbutils.widgets.get('schema').strip() or 'pld_aml')}_{user_prefix}"
FQ = f"`{CATALOG}`.{SCHEMA}"
MCP_COMPARTIDO = f"{CATALOG}.{dbutils.widgets.get('schema_servicios').strip() or 'pld_servicios_mcp'}.servicios_pld"

# COMMAND ----------

# MAGIC %run ./utils/agente_mcp_utils

# COMMAND ----------

# MAGIC %run ./utils/fuente_servidor_pld

# COMMAND ----------

w = cliente_ws()
servicios = ClienteMCP(url_mcp_service(MCP_COMPARTIDO), "servicios_pld")
try:
    servicios.herramientas()
    MODO = "gateway"
except Exception as e:
    print(f"⚠️  Unity Gateway no respondió ({str(e)[:150]}). Usamos los servicios PLD en memoria (plan B).")
    servicios, MODO = ClienteMCPLocal(), "local"
print(f"Servicios PLD: {servicios.url}  (modo {MODO})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — ¿Qué herramientas ofrecen los servicios PLD?
# MAGIC Igual que en el `01`: `tools/list`. Anota la última herramienta sensible, `cerrar_caso`: un agente automático
# MAGIC **no** debería poder cerrar casos. Lo resolvemos con gobierno en el `03`.

# COMMAND ----------

import pandas as pd
display(pd.DataFrame(tabla_herramientas(servicios)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Así se define una herramienta MCP
# MAGIC El servidor usa el **SDK oficial de MCP para Python** (`mcp`, clase `FastMCP`). Una herramienta es una función
# MAGIC con un decorador: el **nombre** de la función, sus **parámetros con tipo** y su **docstring** se convierten en lo
# MAGIC que viste en `tools/list`. Este es el código real del screening (archivo `servicios_pld.py` de la app):
# MAGIC
# MAGIC     from mcp.server.fastmcp import FastMCP
# MAGIC
# MAGIC     mcp = FastMCP(name="servicios-pld-banco", stateless_http=True, streamable_http_path="/mcp")
# MAGIC
# MAGIC     @mcp.tool()
# MAGIC     def screening_sanciones(nombre: str) -> dict:
# MAGIC         """Coteja el nombre de una persona o empresa contra las listas de sanciones del proveedor externo
# MAGIC         (OFAC SDN, Consejo de Seguridad de la ONU y lista local de la UAF). Tolera tildes y errores menores
# MAGIC         de escritura. Una coincidencia obliga a bloquear la operación y escalar; nunca se cierra sin acción."""
# MAGIC         ...
# MAGIC
# MAGIC     app = mcp.streamable_http_app()      # la Databricks App sirve esto en https://<app>/mcp
# MAGIC
# MAGIC Para publicar el **API REST** que el banco ya tiene sin reescribirlo, Databricks ofrece la plantilla de app
# MAGIC **MCP Server (OpenAPI)**: lee la especificación OpenAPI y expone cada endpoint como herramienta.
# MAGIC
# MAGIC Puedes ver el archivo completo en la celda de abajo (es el mismo que despliega el facilitador).

# COMMAND ----------

print(FUENTE_SERVIDOR_PLD["servicios_pld.py"][:3200], "\n...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Screening de sanciones con el proveedor
# MAGIC Probamos tres clientes de las alertas testigo. `Hector Ivan Montalvo` está **sin tildes** en nuestro sistema y
# MAGIC con tildes en la lista de la ONU: el proveedor normaliza y lo detecta.

# COMMAND ----------

for nombre in ["Ramiro Quintanilla Rojas", "Hector Ivan Montalvo", "Marvin Antonio Zelaya Ríos"]:
    r = servicios.llamar("screening_sanciones", {"nombre": nombre})
    hits = [(c["lista"], c["nombre_listado"], c["similitud"]) for c in r["coincidencias"]]
    print(f"{nombre:32s} → {'⚠️  COINCIDENCIA ' + str(hits) if r['hay_coincidencia'] else '✅ sin coincidencias'}")

# COMMAND ----------

# MAGIC %md
# MAGIC Comparemos con la función `verificar_sanciones` de la Sesión 2 (la tabla del lakehouse). Mismo resultado para
# MAGIC estos casos, pero hay una diferencia clave: la lista del proveedor la mantiene **el proveedor** (se actualiza
# MAGIC sola) y aquí aparece una entrada que nuestra tabla no tiene.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {FQ}.verificar_sanciones('Hector Ivan Montalvo')"))
print("Transportes del Istmo Ltda. →",
      "lakehouse:", spark.sql(f"SELECT count(*) n FROM {FQ}.verificar_sanciones('Transportes del Istmo Ltda.')").first().n,
      "coincidencias · proveedor:", len(servicios.llamar("screening_sanciones", {"nombre": "Transportes del Istmo Ltda."})["coincidencias"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — PEP e historial de casos

# COMMAND ----------

for nombre in ["Gabriela Ortiz Salinas", "Mariela Isabel Duarte Vega", "Lucía Fernanda Castillo Mena"]:
    r = servicios.llamar("verificar_pep", {"nombre": nombre})
    reg = r["registros"][0] if r["registros"] else {}
    print(f"{nombre:32s} → PEP: {r['es_pep']}  {reg.get('cargo', '')} {'(vigente)' if reg.get('vigente') else '(ya no vigente)' if reg else ''}")

historial = servicios.llamar("consultar_casos_cliente", {"cliente_id": "CT-011"})
print(f"\nCT-011 tiene {historial['n_casos']} caso(s) previo(s):")
for c in historial["casos"]:
    print(f"  {c['caso_id']} · {c['estado']} · {c['disposicion']} · {c['resumen']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Registrar un caso… ¿y quién lo registró?
# MAGIC Registramos un caso como lo haría el agente. Fíjate en el campo `registrado_por`.

# COMMAND ----------

nuevo = servicios.llamar("crear_caso", {
    "alerta_id": "AT-001", "cliente_id": "CT-001", "disposicion_recomendada": "REPORTAR_ROS", "prioridad": "alta",
    "resumen": f"Prueba del notebook 02 ({user_prefix}): 9 depósitos en efectivo bajo el umbral en 5 días."})
print(json.dumps(nuevo, ensure_ascii=False, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC `registrado_por` **no eres tú**: es el identificador del **service principal** de la conexión. Unity Gateway
# MAGIC entra al sistema externo con la credencial guardada en la conexión de Unity Catalog (OAuth M2M), así que el
# MAGIC sistema ve una identidad **compartida**.
# MAGIC
# MAGIC | Método de la conexión | Qué identidad ve el sistema externo | Cuándo usarlo |
# MAGIC |---|---|---|
# MAGIC | Bearer token / **OAuth M2M** | Una cuenta técnica compartida | Servicios "de sistema" (screening, consultas) |
# MAGIC | **OAuth U2M Per User** | La cuenta de **cada usuario** (inicia sesión una vez) | Cuando el sistema debe aplicar sus propios permisos por persona |
# MAGIC
# MAGIC Lo importante: aunque el sistema externo vea una cuenta compartida, **Unity Gateway sí sabe quién eres** y lo
# MAGIC registra en sus tablas de uso y auditoría. Lo verás en el `03`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC - Un servidor MCP propio publica herramientas de **sistemas fuera del lakehouse** con un estándar.
# MAGIC - Una herramienta MCP = función + tipos + docstring. La docstring es el *prompt* que lee el agente.
# MAGIC - Detrás de Unity Gateway, la credencial del sistema vive en una **conexión de UC**; nadie la ve ni la copia.
# MAGIC - Problema pendiente: `cerrar_caso` está expuesta y cualquiera con acceso podría llamarla.
# MAGIC
# MAGIC ➡️ En el `03` registras **tu propio** MCP service y lo gobiernas.
