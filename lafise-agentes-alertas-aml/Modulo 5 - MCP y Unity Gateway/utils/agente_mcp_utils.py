# Databricks notebook source
# MAGIC %md
# MAGIC # Utilidades compartidas — Agente de Alertas AML conectado por MCP (Sesión 5)
# MAGIC
# MAGIC Los notebooks `00`–`05` lo cargan con `%run ./utils/agente_mcp_utils`. Contiene:
# MAGIC
# MAGIC - **Conexión al workspace desde clúster clásico**: `ws_url()` y `cliente_ws()`. En clásico el cliente por defecto
# MAGIC   apunta al host *regional* del plano de control, donde `/ai-gateway/...` responde `404`; usamos la URL del
# MAGIC   workspace con el token de la sesión del notebook.
# MAGIC - **Cliente MCP**: `mcp_rpc(...)` (una llamada JSON-RPC, para *ver* el protocolo) y `ClienteMCP` (lista y llama
# MAGIC   herramientas de un servidor MCP). `ClienteMCPLocal` levanta los mismos servicios PLD **en memoria** (plan B si el
# MAGIC   workspace no tiene el servidor compartido).
# MAGIC - **Gobierno en Unity Gateway**: `crear_mcp_service`, `actualizar_mcp_service`, `borrar_mcp_service`.
# MAGIC - **El agente** de las sesiones 2 a 4 (LangGraph + funciones de UC + retrieval), ahora con las herramientas de los
# MAGIC   servicios PLD que llegan **por MCP**: `construir_agente_mcp(...)`, `dictaminar(...)`.

# COMMAND ----------

import fnmatch, json, re, threading, time, unicodedata
from typing import Any

import mlflow
import requests
from databricks.sdk import WorkspaceClient
from langchain_core.messages import AIMessage, SystemMessage, ToolMessage
from langchain_core.tools import StructuredTool
from mlflow.entities import Document, SpanType
from pydantic import BaseModel, Field, create_model

C, S, E, R = "CERRAR_SIN_ACCION", "SOLICITAR_INFORMACION", "ESCALAR_INVESTIGACION", "REPORTAR_ROS"
DISPOSICIONES = [C, S, E, R]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Workspace y cliente MCP

# COMMAND ----------

def ws_url() -> str:
    """URL pública del workspace (no el host regional que usa el SDK en clúster clásico)."""
    return "https://" + spark.conf.get("spark.databricks.workspaceUrl")


def _token() -> str:
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()


def cliente_ws() -> WorkspaceClient:
    """WorkspaceClient contra la URL del workspace, autenticado con la sesión del notebook."""
    return WorkspaceClient(host=ws_url(), token=_token())


def url_mcp_service(nombre_completo: str) -> str:
    """URL de un MCP service de Unity Gateway: system.ai.dbsql, <catalogo>.<schema>.<servicio>, ..."""
    return f"{ws_url()}/ai-gateway/mcp-services/{nombre_completo}"


class MCPError(Exception):
    def __init__(self, mensaje, http=None, codigo=None):
        super().__init__(mensaje)
        self.http, self.codigo = http, codigo


_ID = iter(range(1, 10**9))

def mcp_rpc(url: str, method: str, params: dict | None = None, timeout: int = 120, ver: bool = False) -> dict:
    """Una llamada JSON-RPC 2.0 de MCP por Streamable HTTP. Devuelve 'result' o lanza MCPError.
    ver=True imprime el mensaje que viaja por la red (útil en el notebook 01)."""
    cuerpo = {"jsonrpc": "2.0", "id": next(_ID), "method": method}
    if params is not None:
        cuerpo["params"] = params
    if ver:
        print("➡️ ", json.dumps(cuerpo, ensure_ascii=False))
    r = requests.post(url, json=cuerpo, timeout=timeout, headers={
        "Authorization": f"Bearer {_token()}", "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream"})
    texto = r.text
    if "data:" in texto and not texto.lstrip().startswith("{"):
        datos = [l[5:].strip() for l in texto.splitlines() if l.startswith("data:")]
        texto = datos[-1] if datos else texto
    try:
        msg = json.loads(texto)
    except Exception:
        raise MCPError(f"HTTP {r.status_code}: {texto[:300] or r.reason}", http=r.status_code)
    if "error" in msg:
        e = msg["error"]
        raise MCPError(f"HTTP {r.status_code} · MCP {e.get('code')}: {e.get('message')}", http=r.status_code,
                       codigo=e.get("code"))
    if ver:
        print("⬅️ ", json.dumps(msg, ensure_ascii=False)[:1500])
    return msg["result"]


def _resultado(res: dict) -> Any:
    """Normaliza el resultado de tools/call: structuredContent si existe; si no, el texto (JSON si se puede)."""
    if res.get("isError"):
        return {"error": " ".join(b.get("text", "") for b in res.get("content", []))[:1500]}
    if res.get("structuredContent") is not None:
        sc = res["structuredContent"]
        return sc.get("result", sc) if isinstance(sc, dict) and set(sc) == {"result"} else sc
    texto = "".join(b.get("text", "") for b in res.get("content", []) if b.get("type") == "text")
    try:
        return json.loads(texto)
    except Exception:
        return texto


class ClienteMCP:
    """Cliente mínimo de un servidor MCP (Unity Gateway, endpoint MCP de Databricks o app)."""
    def __init__(self, url: str, nombre: str = ""):
        self.url, self.nombre, self._tools = url, nombre or url.rsplit("/", 1)[-1], None

    def herramientas(self, refrescar: bool = False) -> list:
        if self._tools is None or refrescar:
            self._tools = mcp_rpc(self.url, "tools/list")["tools"]
        return self._tools

    def llamar(self, herramienta: str, argumentos: dict | None = None) -> Any:
        return _resultado(mcp_rpc(self.url, "tools/call", {"name": herramienta, "arguments": argumentos or {}}))


class ClienteMCPLocal:
    """Plan B: los MISMOS servicios PLD, pero corriendo en memoria dentro del notebook (sin app ni gateway).
    Sirve para aprender el protocolo y probar el agente; no tiene gobierno de Unity Gateway.
    Requiere `%run ./utils/fuente_servidor_pld` antes. include = patrones de herramientas a exponer."""
    def __init__(self, include: list | None = None):
        ns: dict = {}
        exec(compile(FUENTE_SERVIDOR_PLD["servicios_pld.py"], "servicios_pld.py", "exec"), ns)
        self.servidor = ns["crear_servidor"]()
        self.include, self.url, self.nombre, self._tools = include, "memoria://servicios-pld", "servicios-pld (local)", None

    def _ejecutar(self, metodo, *args):
        import asyncio
        from mcp.shared.memory import create_connected_server_and_client_session

        async def _go():
            async with create_connected_server_and_client_session(self.servidor._mcp_server) as sesion:
                r = await getattr(sesion, metodo)(*args)
                return r.model_dump(mode="json", exclude_none=True)
        salida = {}
        hilo = threading.Thread(target=lambda: salida.setdefault("r", asyncio.run(_go())))
        hilo.start(); hilo.join()
        return salida["r"]

    def _permitida(self, nombre):
        return not self.include or any(fnmatch.fnmatch(nombre, p) for p in self.include)

    def herramientas(self, refrescar: bool = False) -> list:
        if self._tools is None or refrescar:
            self._tools = [t for t in self._ejecutar("list_tools")["tools"] if self._permitida(t["name"])]
        return self._tools

    def llamar(self, herramienta: str, argumentos: dict | None = None) -> Any:
        if not self._permitida(herramienta):
            raise MCPError("Tool not allowed by MCP service configuration (simulado en modo local)")
        return _resultado(self._ejecutar("call_tool", herramienta, argumentos or {}))


def filas_dbsql(r: dict) -> list:
    """Convierte la respuesta de execute_sql (system.ai.dbsql) en una lista de diccionarios."""
    cols = [c["name"] for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
    salida = []
    for fila in (r.get("result") or {}).get("data_typed_array", []) or []:
        vals = [next(iter(v.values()), None) if isinstance(v, dict) and v else None for v in fila.get("values", [])]
        salida.append(dict(zip(cols, vals)))
    return salida


def tabla_herramientas(cliente) -> list:
    """Resumen legible de tools/list: nombre, parámetros y primera línea de la descripción."""
    filas = []
    for t in cliente.herramientas():
        props = (t.get("inputSchema") or {}).get("properties", {})
        req = set((t.get("inputSchema") or {}).get("required", []))
        filas.append({"herramienta": t["name"],
                      "parametros": ", ".join(f"{p}{'' if p in req else '?'}" for p in props),
                      "descripcion": (t.get("description") or "").strip().split("\n")[0][:110]})
    return filas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Herramientas MCP → herramientas de LangChain

# COMMAND ----------

_TIPOS = {"string": str, "integer": int, "number": float, "boolean": bool, "array": list, "object": dict}

def herramientas_langchain(cliente, solo: list | None = None) -> list:
    """Convierte las herramientas que anuncia un servidor MCP (tools/list) en StructuredTools de LangChain.
    El agente no sabe que detrás hay MCP: ve nombre, descripción y esquema de argumentos, igual que con una
    función de UC. Cada llamada del agente se convierte en un tools/call."""
    salida = []
    for t in cliente.herramientas():
        if solo and t["name"] not in solo:
            continue
        esquema = t.get("inputSchema") or {}
        requeridos = set(esquema.get("required", []))
        campos = {}
        for p, d in (esquema.get("properties") or {}).items():
            tipo = _TIPOS.get(d.get("type"), str)
            defecto = ... if p in requeridos else d.get("default", None)
            campos[p] = (tipo if p in requeridos else (tipo | None), Field(default=defecto, description=d.get("description") or d.get("title") or p))
        Args = create_model(f"Args_{re.sub(r'[^A-Za-z0-9_]', '_', t['name'])}", **campos)

        def _llamar(_nombre=t["name"], **kwargs):
            try:
                r = cliente.llamar(_nombre, {k: v for k, v in kwargs.items() if v is not None})
            except MCPError as e:
                r = {"error": str(e)}
            return json.dumps(r, ensure_ascii=False, default=str)

        salida.append(StructuredTool.from_function(
            func=_llamar, name=t["name"], args_schema=Args,
            description=(t.get("description") or t["name"]).strip()[:1500]))
    return salida

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gobierno: MCP services en Unity Gateway (API REST)

# COMMAND ----------

API_MCP = "/api/2.1/unity-catalog/mcp-services"

def obtener_mcp_service(w: WorkspaceClient, nombre_completo: str):
    try:
        return w.api_client.do("GET", f"{API_MCP}/{nombre_completo}")
    except Exception:
        return None


def crear_mcp_service(w: WorkspaceClient, catalogo: str, schema: str, servicio: str, conexion: str,
                      include_tool_selectors: list | None = None, comentario: str = "", rate_limit_qpm: int | None = None):
    """Registra un MCP service en <catalogo>.<schema>.<servicio> sobre una conexión HTTP de UC.
    include_tool_selectors: nombres exactos o prefijos con * (p. ej. 'consultar_*'); None/[] = todas."""
    config = {"source_connection": {"name": f"connections/{conexion}"}}
    if include_tool_selectors:
        config["include_tool_selectors"] = include_tool_selectors
    if rate_limit_qpm:
        config["rate_limits"] = [{"key": "RATE_LIMIT_KEY_SERVICE", "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE",
                                  "requests": rate_limit_qpm}]
    return w.api_client.do("POST", API_MCP, query={"parent": f"schemas/{catalogo}.{schema}", "mcp_service_id": servicio},
                           body={"comment": comentario, "config": config})


def actualizar_mcp_service(w: WorkspaceClient, nombre_completo: str, campo: str, config: dict):
    """PATCH de un campo de la configuración (p. ej. 'config.include_tool_selectors' o 'config.rate_limits')."""
    return w.api_client.do("PATCH", f"{API_MCP}/{nombre_completo}", query={"update_mask": campo},
                           body={"config": config})


def borrar_mcp_service(w: WorkspaceClient, nombre_completo: str) -> bool:
    try:
        w.api_client.do("DELETE", f"{API_MCP}/{nombre_completo}")
        return True
    except Exception:
        return False


def esperar_herramientas(cliente, esperadas=None, intentos: int = 8, pausa: int = 10) -> list:
    """Los cambios de configuración tardan unos segundos en propagarse: reintenta tools/list."""
    ultimo = None
    for _ in range(intentos):
        try:
            nombres = [t["name"] for t in cliente.herramientas(refrescar=True)]
            if esperadas is None or set(nombres) == set(esperadas):
                return nombres
            ultimo = nombres
        except MCPError as e:
            ultimo = str(e)
        time.sleep(pausa)
    return ultimo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. El Agente de Alertas (sesiones 2–4) con los servicios PLD por MCP

# COMMAND ----------

LLM_PREFERIDOS = [
    "databricks-claude-sonnet-4-6", "databricks-claude-sonnet-4-5", "databricks-claude-sonnet-5-5",
    "databricks-claude-sonnet-5", "databricks-gpt-5-4-mini", "databricks-meta-llama-3-3-70b-instruct",
    "databricks-llama-4-maverick", "databricks-qwen3-next-80b-a3b-instruct", "databricks-gemini-3-5-flash",
]

def elegir_llm(w: WorkspaceClient, preferido: str = "") -> str:
    """Endpoint de chat con tool calling: el del widget si existe; si no, el primero disponible."""
    servidos = {e.name for e in w.serving_endpoints.list()}
    if preferido and preferido in servidos:
        return preferido
    for cand in LLM_PREFERIDOS:
        if cand in servidos:
            return cand
    raise RuntimeError("No encontré un modelo de chat con tool calling servido. Revisa Foundation Model APIs "
                       "o escribe el nombre de un endpoint en el widget 'llm'.")


def _cliente_uc_serverless(probar_fn: str, params: dict):
    try:
        from unitycatalog.ai.core.databricks import DatabricksFunctionClient
        cliente = DatabricksFunctionClient()
        r = cliente.execute_function(probar_fn, params)
        return cliente if not r.error else None
    except Exception:
        return None


def _herramienta_spark(w: WorkspaceClient, nombre_fn: str, spark_) -> StructuredTool:
    """Función de UC como herramienta que se ejecuta con spark.sql en el clúster clásico."""
    info = w.functions.get(nombre_fn)
    params = list(info.input_params.parameters) if info.input_params else []
    Args = create_model(f"Args_{info.name}", **{p.name: (str, Field(description=p.comment or p.name)) for p in params})
    marcadores = ", ".join(f":{p.name}" for p in params)

    def _ejecutar(**kwargs):
        filas = spark_.sql(f"SELECT * FROM {nombre_fn}({marcadores})", args=kwargs).limit(50).collect()
        return json.dumps([r.asDict() for r in filas], ensure_ascii=False, default=str)

    return StructuredTool.from_function(func=_ejecutar, name=info.name, args_schema=Args,
                                        description=info.comment or info.name)


def herramientas_uc(w: WorkspaceClient, nombres_fn: list, probar: tuple, spark_, modo: str = "auto"):
    """Funciones de UC -> herramientas. 'auto': UCFunctionToolkit (serverless) si funciona; si no, spark.sql."""
    import os
    modo = os.environ.get("AGENTE_UC_MODO", modo)
    if modo == "auto":
        cliente = _cliente_uc_serverless(*probar)
        if cliente is not None:
            from databricks_langchain import UCFunctionToolkit
            tools = list(UCFunctionToolkit(function_names=nombres_fn, client=cliente).tools)
            for t in tools:
                t.name = next(f.split(".")[-1] for f in nombres_fn if t.name.endswith(f.split(".")[-1]))
            return tools, "serverless"
    return [_herramienta_spark(w, f, spark_) for f in nombres_fn], "clasico (spark.sql)"


def _normalizar(txt: str) -> str:
    txt = unicodedata.normalize("NFKD", str(txt).lower())
    return "".join(ch for ch in txt if not unicodedata.combining(ch))


class _ArgsNormativa(BaseModel):
    consulta: str = Field(description="Qué buscar en la normativa PLD, en lenguaje natural")


def herramienta_normativa(w: WorkspaceClient, spark_, index_name: str, tabla_chunks: str, k: int = 3):
    """Retrieval sobre la base de conocimiento de la Sesión 1 (AI Search si está listo; si no, palabras clave)."""
    columnas = ["chunk_id", "source_doc", "seccion", "chunk_to_retrieve"]
    filas = [r.asDict() for r in spark_.table(tabla_chunks).select(*columnas).collect()]

    def _ai_search(consulta):
        r = w.vector_search_indexes.query_index(index_name=index_name, columns=columnas, query_text=consulta,
                                                num_results=k, query_type="HYBRID")
        nombres = [c.name for c in r.manifest.columns]
        return [dict(zip(nombres, fila)) for fila in (r.result.data_array or [])]

    def _palabras_clave(consulta):
        terminos = [t for t in re.findall(r"\w+", _normalizar(consulta)) if len(t) >= 4]
        puntaje = lambda f: sum(_normalizar(f["seccion"] + " " + f["chunk_to_retrieve"]).count(t[:6]) for t in terminos)
        return sorted(filas, key=puntaje, reverse=True)[:k]

    try:
        _ai_search("umbral de efectivo"); modo = "ai_search"
    except Exception:
        modo = "palabras_clave"
    buscar = _ai_search if modo == "ai_search" else _palabras_clave

    @mlflow.trace(name="recuperar_normativa", span_type=SpanType.RETRIEVER)
    def recuperar_normativa(consulta: str):
        return [Document(page_content=f["chunk_to_retrieve"], id=f["chunk_id"],
                         metadata={"doc_uri": f["source_doc"], "seccion": f["seccion"]}) for f in buscar(consulta)]

    def buscar_normativa_pld(consulta: str) -> str:
        return json.dumps([{"documento": d.metadata["doc_uri"], "referencia": d.metadata["seccion"],
                            "texto": d.page_content} for d in recuperar_normativa(consulta)], ensure_ascii=False)

    return StructuredTool.from_function(
        func=buscar_normativa_pld, name="buscar_normativa_pld", args_schema=_ArgsNormativa,
        description=("Busca en la base de conocimiento PLD: artículos de la política PLD/FT, tipologías, instructivo "
                     "de disposición de alertas y precedentes. Úsala para fundamentar y CITAR la disposición.")), modo


FORMATO_DICTAMEN = """Responde en español con un único bloque JSON y nada más:
{"alerta_id": "...", "disposicion": "...", "nivel_confianza": "alta|media|baja",
 "senales_clave": ["..."], "justificacion": "...",
 "citas": [{"documento": "...", "referencia": "..."}], "caso_id": "... o null"}"""

INSTRUCCIONES_AGENTE_MCP = """Eres el Agente de Alertas AML de la Unidad PLD de un banco centroamericano. Analizas UNA alerta del
monitoreo transaccional y recomiendas su disposición a un analista humano, que toma la decisión final.

Tienes dos tipos de herramientas:
- Del lakehouse (Unity Catalog): obtener_alerta, features_transaccionales y buscar_normativa_pld.
- De los servicios PLD del banco, por MCP: screening_sanciones y verificar_pep (proveedor externo de listas),
  consultar_casos_cliente y crear_caso (sistema de gestión de casos).

Procedimiento:
1. obtener_alerta para leer la alerta y el perfil KYC del cliente.
2. features_transaccionales para las señales de los 30 días previos.
3. screening_sanciones y verificar_pep con el nombre del cliente.
4. consultar_casos_cliente con el cliente_id para ver si hay casos previos (reincidencia).
5. buscar_normativa_pld para el artículo, la tipología y un precedente. Haz al menos una búsqueda.
6. Decide la disposición.
7. Si la disposición NO es CERRAR_SIN_ACCION, registra el caso con crear_caso (prioridad alta si hay coincidencia
   en sanciones o el cliente es PEP). Si consultar_casos_cliente ya muestra un caso PENDIENTE_REVISION para
   esta misma alerta, no lo dupliques: usa su caso_id. Si crear_caso no está disponible o devuelve un error,
   continúa y deja caso_id en null.

Disposiciones posibles (elige exactamente una):
- CERRAR_SIN_ACCION: el análisis descarta el indicio y la actividad es consistente con el perfil del cliente.
- SOLICITAR_INFORMACION: faltan datos para decidir (por ejemplo, soporte del origen de los fondos).
- ESCALAR_INVESTIGACION: persisten indicios que requieren profundizar (pass-through, testaferros, alto riesgo).
- REPORTAR_ROS: los hechos configuran una tipología con evidencia clara; corresponde un ROS a la UAF.

Reglas duras:
- Nunca inventes cifras ni datos: usa solo lo que devuelven las herramientas.
- Cita al menos un fundamento recuperado con buscar_normativa_pld (nombre exacto del documento).
- Si screening_sanciones devuelve una coincidencia, la disposición es ESCALAR_INVESTIGACION o REPORTAR_ROS.
- Si el cliente es PEP o hay contrapartes en jurisdicciones de alto riesgo, no cierres sin acción.
- Nunca cierres casos ni intentes modificar casos existentes: eso lo hace solo un analista.
- No incluyas números de documento de identidad, cuentas ni datos de contacto en el resumen del caso.
- Nunca sugieras informar al cliente sobre el análisis (tipping-off).

""" + FORMATO_DICTAMEN


def crear_agente(llm, herramientas: list, instrucciones: str):
    """Agente de tool calling (ReAct) con LangGraph: modelo -> ¿pidió herramientas? -> herramientas -> modelo."""
    from langgraph.graph import END, MessagesState, StateGraph
    from langgraph.prebuilt import ToolNode

    llm_ = llm.bind_tools(herramientas)

    def llamar_modelo(state: MessagesState):
        return {"messages": [llm_.invoke([SystemMessage(content=instrucciones)] + state["messages"])]}

    def decidir(state: MessagesState):
        ultimo = state["messages"][-1]
        return "herramientas" if isinstance(ultimo, AIMessage) and ultimo.tool_calls else END

    grafo = StateGraph(MessagesState)
    grafo.add_node("modelo", llamar_modelo)
    grafo.add_node("herramientas", ToolNode(herramientas, handle_tool_errors=True))
    grafo.set_entry_point("modelo")
    grafo.add_conditional_edges("modelo", decidir, {"herramientas": "herramientas", END: END})
    grafo.add_edge("herramientas", "modelo")
    return grafo.compile()


def construir_agente_mcp(w, spark_, catalog: str, schema: str, llm_endpoint: str, cliente_mcp,
                         instrucciones: str = INSTRUCCIONES_AGENTE_MCP):
    """Agente de Alertas: 2 funciones de UC + retrieval (lakehouse) + las herramientas del servidor MCP."""
    from databricks_langchain import ChatDatabricks
    llm = ChatDatabricks(endpoint=llm_endpoint, temperature=0)
    fns = [f"{catalog}.{schema}.{f}" for f in ("obtener_alerta", "features_transaccionales")]
    herr_uc, modo_uc = herramientas_uc(w, fns, probar=(fns[0], {"alerta_id": "AT-001"}), spark_=spark_)
    herr_norm, modo_ret = herramienta_normativa(w, spark_, f"{catalog}.{schema}.chunks_index",
                                                f"`{catalog}`.{schema}.chunks")
    herr_mcp = herramientas_langchain(cliente_mcp)
    herramientas = herr_uc + [herr_norm] + herr_mcp
    info = {"modelo": llm_endpoint, "servidor_mcp": cliente_mcp.url,
            "herramientas_lakehouse": [h.name for h in herr_uc + [herr_norm]],
            "herramientas_mcp": [h.name for h in herr_mcp], "modo_funciones_uc": modo_uc, "modo_retrieval": modo_ret}
    return crear_agente(llm, herramientas, instrucciones), info


def _texto(contenido: Any) -> str:
    if isinstance(contenido, str):
        return contenido
    if isinstance(contenido, list):
        return "".join(b.get("text", "") if isinstance(b, dict) else str(b) for b in contenido)
    return str(contenido)


def extraer_dictamen(texto: str):
    if not texto:
        return None
    bloque = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", texto, re.S)
    candidato = bloque.group(1) if bloque else None
    if candidato is None:
        ini, fin = texto.find("{"), texto.rfind("}")
        candidato = texto[ini:fin + 1] if ini != -1 and fin > ini else None
    try:
        return json.loads(candidato) if candidato else None
    except json.JSONDecodeError:
        return None


ID_ALERTA = re.compile(r"\b(AT-\d{3}|A\d{5})\b")

def dictaminar(agente, pregunta: str, version: str = "s5-mcp", max_pasos: int = 30):
    """Ejecuta el agente y devuelve (dictamen, llamadas) donde llamadas = [(herramienta, argumentos, resultado)]."""
    @mlflow.trace(name="agente_alertas_mcp", span_type=SpanType.AGENT)
    def _correr(pregunta):
        m = ID_ALERTA.search(pregunta)
        mlflow.update_current_trace(tags={"version_agente": version, "alerta_id": m.group(1) if m else "ninguna"})
        out = agente.invoke({"messages": [{"role": "user", "content": pregunta}]}, config={"recursion_limit": max_pasos})
        return out["messages"]
    msgs = _correr(pregunta)
    pedidas = {tc["id"]: (tc["name"], tc["args"]) for m in msgs if isinstance(m, AIMessage) for tc in m.tool_calls}
    llamadas = [(pedidas.get(m.tool_call_id, (m.name, {}))[0], pedidas.get(m.tool_call_id, (m.name, {}))[1],
                 _texto(m.content)[:400]) for m in msgs if isinstance(m, ToolMessage)]
    texto = _texto(msgs[-1].content)
    return extraer_dictamen(texto) or {"error": "dictamen_no_es_json", "texto": texto[:1500]}, llamadas


def mostrar_llamadas(llamadas: list, origen_mcp: set):
    for i, (h, a, r) in enumerate(llamadas, 1):
        tag = "MCP " if h in origen_mcp else "UC  "
        print(f"{i:2d}. [{tag}] {h}({json.dumps(a, ensure_ascii=False)[:90]})\n        → {r[:160]}")

print("✅ agente_mcp_utils cargado: ClienteMCP, ClienteMCPLocal, mcp_rpc, crear_mcp_service, construir_agente_mcp, dictaminar")
