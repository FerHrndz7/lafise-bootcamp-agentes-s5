"""Punto de entrada de la Databricks App: sirve el servidor MCP de servicios PLD en /mcp (Streamable HTTP)."""
import os

import uvicorn

from servicios_pld import crear_servidor

app = crear_servidor().streamable_http_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")))
