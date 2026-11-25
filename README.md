# Bootcamp de Agentes AML · Sesión 5: Integración vía MCP + Unity AI Gateway

Contenido práctico de la Sesión 5 del bootcamp de Agentes AML para el equipo de PLD de Grupo LAFISE: el **Agente de
Alertas AML** de las sesiones anteriores sale del lakehouse y se conecta por **MCP** (Model Context Protocol) a dos
sistemas del banco, un proveedor de listas de sanciones y PEP y el sistema de gestión de casos (ambos **simulados**),
y se **gobierna con Unity Gateway**: herramientas permitidas, permisos, rate limits, uso, auditoría y service policies.

> **Sobre los nombres (noviembre 2026).** **Unity Gateway** (también "Unity AI Gateway") está en GA. Los servidores
> MCP registrados son **MCP services** de Unity Catalog. MCP de Databricks: `system.ai.genie_one_mcp` (GA),
> `system.ai.dbsql` (Beta) y los endpoints MCP del workspace `/api/2.0/mcp/...` (Public Preview). Las **service
> policies** están en Beta y se adjuntan desde la interfaz.

> Todos los datos del laboratorio son **100% sintéticos**. Nunca uses PII real en un workspace de laboratorio.

## Contenido

| Ruta | Qué es |
|---|---|
| `lafise-agentes-alertas-aml/00 - Course Overview` | Agenda, arquitectura y requisitos de la sesión |
| `lafise-agentes-alertas-aml/Modulo 5 - MCP y Unity Gateway/` | Notebooks `00_setup` a `04` (código completo) y `05_reto` (tarea), con `solutions/`, `data/` y `utils/` |
| `lafise-agentes-alertas-aml/Modulo 5 - MCP y Unity Gateway/facilitador/00_preparar_servicios_pld` | Lo corre el facilitador **antes** de la sesión: despliega el servidor de servicios PLD y lo registra en Unity Gateway |
| `lafise-agentes-alertas-aml/requirements.txt` | Librerías con versiones fijas (para instalarlas como librerías de clúster si no hay salida a PyPI) |
| `servidor-mcp-pld/` | Código del servidor MCP de servicios PLD (Databricks App, SDK oficial `mcp`). También viaja embebido en los notebooks |
| `validacion/validacion_entorno_lafise_s5.py` | Notebook de validación de entorno |
| `build/lafise-agentes-alertas-aml-s5.dbc` | El mismo árbol de notebooks en un solo archivo, para importar en un clic |

## Requisitos

- Clúster clásico **DBR 17.3 LTS**, modo de acceso **Standard**, para ejecutar los notebooks.
- Acceso a **Foundation Model APIs** (modelo con tool calling) y a **Unity Gateway**.
- Permiso para crear tablas, funciones y MCP services (`CREATE SERVICE`) en el schema `pld_aml_<usuario>` del
  catálogo de las Sesiones 1 a 4.
- **Solo el facilitador:** Databricks Apps habilitado, un service principal con secreto OAuth guardado en un secret
  scope y `CREATE CONNECTION` en el schema compartido `pld_servicios_mcp`. Los comandos están al inicio de
  `facilitador/00_preparar_servicios_pld`.
- Sin Apps, el laboratorio corre igual con los servicios PLD **en memoria** (plan B): `00_setup` lo detecta solo.

## Cómo cargarlo en Databricks

**Opción A (recomendada): archivo `.dbc`.** Importa todo el árbol de notebooks en un clic.
1. Descargar el archivo desde este repositorio: abrir `build/lafise-agentes-alertas-aml-s5.dbc` y pulsar **Download raw file**
   (o usar el enlace directo
   `https://github.com/FerHrndz7/lafise-bootcamp-agentes-s5/raw/main/build/lafise-agentes-alertas-aml-s5.dbc`).
2. En el workspace: **Workspace → (carpeta destino) → Import → File** y subir el `.dbc`.
3. Abrir `00 - Course Overview`.

**Opción B: Git folder.** No requiere descargar archivos.
1. En el workspace: **Workspace → Create → Git folder**.
2. URL: `https://github.com/FerHrndz7/lafise-bootcamp-agentes-s5`, proveedor **GitHub** (repositorio público, no
   requiere credenciales).

**Opción C: descargar el ZIP.** En GitHub: **Code → Download ZIP**, descomprimir e importar la carpeta
`lafise-agentes-alertas-aml` al workspace.

## Antes de la sesión

1. **Validación (organizadores):** `validacion/validacion_entorno_lafise_s5` con una cuenta de asistente (no
   administrador), widget `catalog` = el catálogo de los asistentes, **Run all** (~6 min). Una FALLA en los puntos
   2, 3 o 7b es bloqueante; los puntos 6 y 7a son del facilitador.
2. **Servicios PLD (facilitador):** `Modulo 5 - MCP y Unity Gateway/facilitador/00_preparar_servicios_pld` con
   `catalog` = el del curso y `grupo` = el grupo de los asistentes (~10 min la primera vez). La mañana de la sesión,
   correrlo de nuevo con `redesplegar = si` (reinicia el sistema de casos, que vive en memoria).
3. **Prueba con una cuenta de asistente** (30 a 40 min): `00_setup` → `01` → `02` → `03` → `04`.

Resultado esperado: en `00_setup` los servicios PLD en modo gateway (o "plan B" si no hay Apps); en el `03`,
`cerrar_caso` bloqueado (error MCP -32003) y HTTP 429 al pasar el rate limit; en el `04`, la alerta AT-003 escalada
con un caso registrado y el intento de manipulación sin efecto (el caso sigue ABIERTO). Las tablas de sistema de uso
y auditoría llegan con hasta ~60 min de retraso. `05_reto` es la tarea para después de la sesión (su solución está
en `solutions/`).
