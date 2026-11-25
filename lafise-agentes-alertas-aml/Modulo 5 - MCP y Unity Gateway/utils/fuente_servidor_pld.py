# Databricks notebook source
# MAGIC %md
# MAGIC # Código fuente del servidor MCP «servicios PLD» (generado)
# MAGIC
# MAGIC **No edites este notebook a mano**: se genera desde `servidor-mcp-pld/` con
# MAGIC `build/generar_fuente_servidor.py`. Lo usa `facilitador/00_preparar_servicios_pld` para escribir
# MAGIC los archivos de la Databricks App en el workspace y desplegarla.

# COMMAND ----------

FUENTE_SERVIDOR_PLD = {}

# COMMAND ----------

FUENTE_SERVIDOR_PLD['servicios_pld.py'] = r'''"""Servicios PLD del banco (SIMULADOS) expuestos como herramientas MCP.

Simula dos sistemas que en la vida real viven FUERA del lakehouse:
  1. Un proveedor externo de screening de listas (sanciones OFAC / ONU / UAF y registro PEP).
  2. El sistema de gestión de casos de la Unidad PLD (crear, consultar y cerrar casos).

Todo es SINTÉTICO. Los nombres coinciden con las alertas testigo AT-001..013 del bootcamp.
Usa el SDK oficial de MCP para Python (`mcp` 1.x, clase FastMCP). Lo sirve app.py en Databricks Apps.
"""
import difflib
import itertools
import os
import threading
import unicodedata
from datetime import datetime, timezone

from mcp.server.fastmcp import Context, FastMCP

VERSION = "1.0.0"

# --------------------------------------------------------------------------------------------
# Datos sintéticos del "proveedor" de listas
# --------------------------------------------------------------------------------------------
LISTAS_SANCIONES = [
    {"nombre_listado": "Ramiro Quintanilla Rojas", "lista": "OFAC SDN", "pais": "Honduras",
     "fecha_inclusion": "2024-05-10", "motivo": "Facilitador de redes de lavado", "id_registro": "SDN-48211"},
    {"nombre_listado": "Inversiones Costa Azul S.A.", "lista": "OFAC SDN", "pais": "Panamá",
     "fecha_inclusion": "2023-09-01", "motivo": "Sociedad pantalla", "id_registro": "SDN-47390"},
    {"nombre_listado": "Héctor Iván Montalvo", "lista": "ONU CSNU", "pais": "Guatemala",
     "fecha_inclusion": "2022-02-14", "motivo": "Financiamiento al terrorismo", "id_registro": "QDi.512"},
    {"nombre_listado": "Grupo Comercial Tres Ríos", "lista": "Lista local UAF", "pais": "Nicaragua",
     "fecha_inclusion": "2025-01-20", "motivo": "Contrabando", "id_registro": "UAF-NI-0091"},
    {"nombre_listado": "Transportes del Istmo Ltda.", "lista": "OFAC SDN", "pais": "Panamá",
     "fecha_inclusion": "2025-06-03", "motivo": "Narcotráfico", "id_registro": "SDN-49902"},
]

REGISTRO_PEP = [
    {"nombre": "Gabriela Ortiz Salinas", "cargo": "Viceministra de Finanzas", "pais": "Panamá",
     "desde": "2021-07-01", "vigente": True},
    {"nombre": "Federico Arturo Bermúdez Mairena", "cargo": "Diputado de la Asamblea Nacional",
     "pais": "Nicaragua", "desde": "2017-01-10", "vigente": True},
    {"nombre": "Mariela Isabel Duarte Vega", "cargo": "Alcaldesa municipal (2014-2022)", "pais": "Nicaragua",
     "desde": "2014-01-15", "vigente": False},
]

# --------------------------------------------------------------------------------------------
# "Base de datos" del sistema de gestión de casos (en memoria; se reinicia con la app)
# --------------------------------------------------------------------------------------------
_CASOS_INICIALES = [
    {"caso_id": "CASO-2025-0412", "alerta_id": "A00318", "cliente_id": "CT-001", "estado": "CERRADO",
     "disposicion": "SOLICITAR_INFORMACION", "prioridad": "media", "fecha_apertura": "2025-08-12",
     "resumen": "Depósitos en efectivo fraccionados en dos sucursales; el cliente presentó soporte parcial.",
     "registrado_por": "analista.pld@banco.example"},
    {"caso_id": "CASO-2025-0577", "alerta_id": "A00402", "cliente_id": "CT-009", "estado": "ESCALADO",
     "disposicion": "ESCALAR_INVESTIGACION", "prioridad": "alta", "fecha_apertura": "2025-11-03",
     "resumen": "Transferencias desde sociedad sin actividad comercial verificable en Panamá.",
     "registrado_por": "analista.pld@banco.example"},
    {"caso_id": "CASO-2026-0108", "alerta_id": "A00977", "cliente_id": "CT-011", "estado": "ABIERTO",
     "disposicion": "ESCALAR_INVESTIGACION", "prioridad": "alta", "fecha_apertura": "2026-03-21",
     "resumen": "PEP con transferencias recurrentes hacia Islas Caimán sin soporte del origen de fondos.",
     "registrado_por": "analista.pld@banco.example"},
]
_CASOS = {c["caso_id"]: dict(c) for c in _CASOS_INICIALES}
_SECUENCIA = itertools.count(1001)
_LOCK = threading.Lock()

DISPOSICIONES = {"CERRAR_SIN_ACCION", "SOLICITAR_INFORMACION", "ESCALAR_INVESTIGACION", "REPORTAR_ROS"}
PRIORIDADES = {"baja", "media", "alta"}


def _norm(txt: str) -> str:
    txt = unicodedata.normalize("NFKD", str(txt or "").lower())
    return " ".join("".join(c for c in txt if not unicodedata.combining(c)).replace(".", " ").split())


def _similitud(a: str, b: str) -> float:
    return round(difflib.SequenceMatcher(None, _norm(a), _norm(b)).ratio(), 3)


def _quien(ctx) -> str:
    """Identidad que ve el servidor. En Databricks Apps el proxy la inyecta en el encabezado X-Forwarded-Email
    (detrás de Unity Gateway con conexión M2M es el service principal de la conexión)."""
    try:
        h = ctx.request_context.request.headers
        return h.get("x-forwarded-email") or h.get("x-forwarded-preferred-username") or "desconocido"
    except Exception:
        return "local (en memoria)"


def crear_servidor() -> FastMCP:
    """Crea el servidor MCP con las herramientas de los servicios PLD."""
    mcp = FastMCP(
        name="servicios-pld-banco",
        instructions=("Servicios internos SIMULADOS de la Unidad PLD: screening de listas de sanciones y PEP "
                      "(proveedor externo) y sistema de gestión de casos. Datos sintéticos."),
        stateless_http=True,          # cada solicitud es independiente (recomendado en Apps y detrás de Unity Gateway)
        json_response=True,           # respuestas JSON simples en lugar de un stream SSE
        streamable_http_path="/mcp",
        host="0.0.0.0",
        log_level="WARNING",          # sin logs INFO por cada solicitud (también en el modo en memoria del notebook)               # escucha detrás del proxy de Apps (desactiva el filtro de Host de localhost)
    )

    @mcp.tool()
    def screening_sanciones(nombre: str) -> dict:
        """Coteja el nombre de una persona o empresa contra las listas de sanciones del proveedor externo
        (OFAC SDN, Consejo de Seguridad de la ONU y lista local de la UAF). Tolera tildes y errores menores
        de escritura. Una coincidencia obliga a bloquear la operación y escalar; nunca se cierra sin acción."""
        coincidencias = []
        for r in LISTAS_SANCIONES:
            s = _similitud(nombre, r["nombre_listado"])
            if s >= 0.85:
                coincidencias.append({**r, "similitud": s,
                                      "tipo": "exacta" if s >= 0.99 else "aproximada"})
        return {"nombre_consultado": nombre, "listas_consultadas": ["OFAC SDN", "ONU CSNU", "Lista local UAF"],
                "hay_coincidencia": bool(coincidencias), "coincidencias": coincidencias,
                "fecha_consulta": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "proveedor": "Proveedor de listas (simulado)"}

    @mcp.tool()
    def verificar_pep(nombre: str) -> dict:
        """Consulta el registro regional de Personas Expuestas Políticamente (PEP). Devuelve el cargo, el país
        y si la condición de PEP está vigente. Una PEP (vigente o no) requiere debida diligencia reforzada."""
        hallados = [{**p, "similitud": _similitud(nombre, p["nombre"])} for p in REGISTRO_PEP
                    if _similitud(nombre, p["nombre"]) >= 0.85]
        return {"nombre_consultado": nombre, "es_pep": bool(hallados), "registros": hallados,
                "proveedor": "Registro PEP regional (simulado)"}

    @mcp.tool()
    def consultar_casos_cliente(cliente_id: str) -> dict:
        """Devuelve el historial de casos PLD de un cliente en el sistema de gestión de casos (abiertos,
        escalados y cerrados), con su disposición y un resumen. Útil para detectar reincidencia."""
        casos = sorted((c for c in _CASOS.values() if c["cliente_id"] == cliente_id),
                       key=lambda c: c["fecha_apertura"])
        return {"cliente_id": cliente_id, "n_casos": len(casos), "casos": casos}

    @mcp.tool()
    def crear_caso(alerta_id: str, cliente_id: str, disposicion_recomendada: str, resumen: str,
                   prioridad: str = "media", ctx: Context = None) -> dict:
        """Registra un caso nuevo en el sistema de gestión de casos PLD con la disposición que RECOMIENDA el
        agente. El caso queda en estado PENDIENTE_REVISION: siempre lo revisa y decide un analista humano.
        disposicion_recomendada: CERRAR_SIN_ACCION | SOLICITAR_INFORMACION | ESCALAR_INVESTIGACION | REPORTAR_ROS.
        prioridad: baja | media | alta."""
        if disposicion_recomendada not in DISPOSICIONES:
            return {"error": f"disposicion_recomendada inválida; use una de {sorted(DISPOSICIONES)}"}
        if prioridad not in PRIORIDADES:
            prioridad = "media"
        with _LOCK:
            caso_id = f"CASO-{datetime.now(timezone.utc):%Y}-{next(_SECUENCIA):04d}"
            caso = {"caso_id": caso_id, "alerta_id": alerta_id, "cliente_id": cliente_id,
                    "estado": "PENDIENTE_REVISION", "disposicion": disposicion_recomendada,
                    "prioridad": prioridad, "fecha_apertura": datetime.now(timezone.utc).date().isoformat(),
                    "resumen": resumen[:1500], "registrado_por": _quien(ctx)}
            _CASOS[caso_id] = caso
        return {"ok": True, "caso": caso}

    @mcp.tool()
    def cerrar_caso(caso_id: str, motivo: str, ctx: Context = None) -> dict:
        """Cierra DEFINITIVAMENTE un caso PLD en el sistema de gestión de casos. Acción sensible: solo debería
        poder ejecutarla un analista autorizado, nunca un agente automático."""
        with _LOCK:
            caso = _CASOS.get(caso_id)
            if not caso:
                return {"error": f"No existe el caso {caso_id}"}
            caso.update(estado="CERRADO", motivo_cierre=motivo[:500], cerrado_por=_quien(ctx))
        return {"ok": True, "caso": caso}

    @mcp.tool()
    def estado_servicio(ctx: Context = None) -> dict:
        """Verifica que el servicio responde y muestra qué identidad de Databricks ve el servidor."""
        return {"estado": "ok", "version": VERSION, "identidad_vista_por_el_servidor": _quien(ctx),
                "n_casos": len(_CASOS), "host": os.environ.get("DATABRICKS_APP_NAME", "local")}

    return mcp
'''

# COMMAND ----------

FUENTE_SERVIDOR_PLD['app.py'] = r'''"""Punto de entrada de la Databricks App: sirve el servidor MCP de servicios PLD en /mcp (Streamable HTTP)."""
import os

import uvicorn

from servicios_pld import crear_servidor

app = crear_servidor().streamable_http_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")))
'''

# COMMAND ----------

FUENTE_SERVIDOR_PLD['app.yaml'] = r'''command: ["python", "app.py"]
'''

# COMMAND ----------

FUENTE_SERVIDOR_PLD['requirements.txt'] = r'''mcp>=1.24,<2
uvicorn>=0.35
'''

# COMMAND ----------

print('Archivos del servidor:', ', '.join(f'{k} ({len(v)} bytes)' for k, v in FUENTE_SERVIDOR_PLD.items()))
