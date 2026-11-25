# Databricks notebook source
# MAGIC %md
# MAGIC # LAFISE — Bootcamp de Agentes AML · Sesión 5
# MAGIC ## Integración vía MCP + Unity AI Gateway: el agente se conecta a los sistemas del banco, con gobierno
# MAGIC
# MAGIC En la **Sesión 1** construimos la base de conocimiento, en la **Sesión 2** programamos el **Agente de Alertas**,
# MAGIC en la **Sesión 3** lo hicimos sin código y en la **Sesión 4** lo evaluamos contra alucinaciones. Hasta ahora todas
# MAGIC sus herramientas vivían **dentro del lakehouse**. En la vida real, el screening de sanciones lo hace un
# MAGIC **proveedor externo** y los casos viven en el **sistema de gestión de casos** de la Unidad PLD.
# MAGIC
# MAGIC Hoy conectamos el agente a esos sistemas con **MCP** (*Model Context Protocol*, el estándar abierto para que un
# MAGIC agente descubra y llame herramientas) y los **gobernamos con Unity AI Gateway**: quién puede llamar, qué
# MAGIC herramientas se exponen, cuántas llamadas por minuto, qué políticas se aplican y quién hizo qué.
# MAGIC
# MAGIC > **Sobre los nombres (noviembre 2026).** El producto se llama **Unity Gateway** (en el material también "Unity
# MAGIC > AI Gateway"; antes, *Mosaic AI Gateway*) y está en **GA**. Los servidores MCP registrados son **MCP services**:
# MAGIC > objetos de Unity Catalog `<catálogo>.<schema>.<servicio>`. Databricks ofrece MCP propios en `system.ai`
# MAGIC > (`genie_one_mcp` en GA; `dbsql` en Beta). Las **service policies** sobre MCP están en **Beta**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Aviso
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Solo laboratorio · datos sintéticos</strong>
# MAGIC <div style="color:#333;">
# MAGIC <ul>
# MAGIC <li>Ejecuta esta práctica únicamente en el <strong>workspace de laboratorio</strong> proporcionado.</li>
# MAGIC <li>Clientes, transacciones, alertas, listas de sanciones, registro PEP y casos son <strong>sintéticos</strong>. Los "servicios PLD" son una <strong>simulación</strong>, no un sistema real del banco. <strong>Nunca</strong> uses PII real de clientes de LAFISE.</li>
# MAGIC <li>El agente <strong>recomienda</strong> y registra casos <em>pendientes de revisión</em>; la decisión final siempre es del analista humano.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Recorrido de la práctica (~75 min)
# MAGIC
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 20px 24px; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC <style>.agenda-table td, .agenda-table th { font-size: 14pt !important; }</style>
# MAGIC <table class="agenda-table" style="width: 100%; border-collapse: collapse; line-height: 1.5;">
# MAGIC   <thead>
# MAGIC      <tr style="background: #1B5162; color: white;">
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 60px;">#</th>
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 90px;">Tiempo</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">Notebook</th>
# MAGIC      </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">00</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">8 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Preparación: datos de las sesiones anteriores y conexión con los servicios PLD</td></tr>
# MAGIC      <tr style="background: #FBFAF8;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">01</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">12 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">¿Qué es MCP? El protocolo en vivo con los MCP de Databricks: tus funciones de UC y <code>system.ai</code></td></tr>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">02</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">13 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Los servicios PLD del banco por MCP: screening de sanciones y PEP, historial y registro de casos</td></tr>
# MAGIC      <tr style="background: #FBFAF8;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">03</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">22 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Gobernar con Unity Gateway: tu MCP service, herramientas permitidas, rate limit, permisos, auditoría y políticas</td></tr>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">04</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">20 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">El Agente de Alertas conectado por MCP: dictamen + caso registrado, trazado y gobernado</td></tr>
# MAGIC      <tr style="background: #FBFAF8;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">05</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">Tarea</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Reto: un "agente de consulta" de solo lectura con su propio MCP service y su propio tope</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC Empieza en `Modulo 5 - MCP y Unity Gateway/00_setup`. Todos los notebooks traen el **código completo**; solo el
# MAGIC `05_reto` tiene espacios para completar (su solución está en `solutions/`).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. La arquitectura de hoy
# MAGIC
# MAGIC <div style="font-family: monospace; background:#1B3139; color:#F9F7F4; padding:16px 20px; border-radius:8px; line-height:1.55; font-size: 12.5pt;">
# MAGIC  Notebook (clúster clásico)                 Unity Gateway                         Sistemas del banco<br>
# MAGIC  ┌───────────────────────┐   tools/list   ┌──────────────────────────┐   OAuth   ┌────────────────────────────┐<br>
# MAGIC  │ Agente de Alertas AML │ ─────────────▶ │ MCP service (UC)         │ ────────▶ │ Servidor MCP "servicios PLD" │<br>
# MAGIC  │  LangGraph + LLM      │   tools/call   │ · EXECUTE (quién)        │   M2M     │ · screening_sanciones        │<br>
# MAGIC  │                       │ ◀───────────── │ · herramientas permitidas│ ◀──────── │ · verificar_pep              │<br>
# MAGIC  │  + funciones UC       │                │ · rate limit (QPM)       │           │ · consultar_casos_cliente    │<br>
# MAGIC  │  + AI Search (S1)     │                │ · service policies (Beta)│           │ · crear_caso / cerrar_caso   │<br>
# MAGIC  └───────────────────────┘                │ · uso + auditoría        │           └────────────────────────────┘<br>
# MAGIC                                           └──────────────────────────┘            (Databricks App, simulado)
# MAGIC </div>
# MAGIC
# MAGIC La credencial del sistema externo vive en una **conexión de Unity Catalog**: ni el agente ni los participantes la
# MAGIC ven. El gateway decide qué llega al servidor y deja registro de cada llamada.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Requisitos
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="color:#333;">
# MAGIC
# MAGIC - Conecta un **clúster clásico** (DBR **17.3 LTS**, modo de acceso *Standard*) para ejecutar los notebooks.
# MAGIC - En el widget **catalog** escribe el **mismo catálogo de las Sesiones 1 a 4**.
# MAGIC - Acceso a **Foundation Model APIs** (el agente) y permiso para crear tablas, funciones y **services** en tu schema.
# MAGIC - El facilitador ya preparó los **servicios PLD** (servidor MCP + conexión en Unity Catalog). Si en tu workspace no
# MAGIC   están disponibles, los notebooks usan automáticamente una copia **en memoria** (plan B) y el `03` se ve como demo.
# MAGIC - Usa **Chrome o Firefox** (no Safari), sin modo Incógnito.
# MAGIC - Todo se valida antes de la sesión con el notebook de validación de entorno.
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. Todos los derechos reservados.
