import asyncio
import logging
import os
import uuid
from typing import AsyncGenerator

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

PORT = int(os.environ.get("PORT", "8000"))
MCP_CMD = ["alpaca-mcp-server", "serve"]

sessions: dict[str, asyncio.subprocess.Process] = {}

app = FastAPI(title="Alpaca MCP Gateway")


@app.get("/health")
async def health():
    return {"status": "ok", "active_sessions": len(sessions)}


@app.get("/sse")
async def sse_endpoint(request: Request):
    session_id = str(uuid.uuid4())
    logger.info(f"Nueva conexión SSE | session={session_id}")

    async def event_stream() -> AsyncGenerator[str, None]:
        # ── 1. Enviar el evento "endpoint" INMEDIATAMENTE ─────────────────
        # El cliente necesita esto antes de poder enviar mensajes.
        yield f"event: endpoint\ndata: /messages?sessionId={session_id}\n\n"

        # ── 2. Iniciar el subproceso MCP ──────────────────────────────────
        process: asyncio.subprocess.Process | None = None
        try:
            process = await asyncio.create_subprocess_exec(
                *MCP_CMD,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            sessions[session_id] = process
            logger.info(f"Proceso MCP iniciado | pid={process.pid} | session={session_id}")
        except FileNotFoundError:
            logger.error("alpaca-mcp-server no encontrado.")
            yield "event: error\ndata: alpaca-mcp-server not installed\n\n"
            return
        except Exception as e:
            logger.error(f"Error iniciando MCP: {e}")
            yield f"event: error\ndata: {e}\n\n"
            return

        # ── 3. Stream stdout del MCP con keepalive cada 15s ───────────────
        try:
            while True:
                if await request.is_disconnected():
                    logger.info(f"Cliente desconectado | session={session_id}")
                    break

                try:
                    line = await asyncio.wait_for(
                        process.stdout.readline(), timeout=15.0
                    )
                except asyncio.TimeoutError:
                    # Comentario SSE — mantiene la conexión viva sin ser un mensaje
                    yield ": ping\n\n"
                    continue

                if not line:
                    logger.info(f"Proceso MCP cerró stdout | session={session_id}")
                    break

                decoded = line.decode().strip()
                if decoded:
                    logger.debug(f"MCP→cliente: {decoded[:120]}")
                    yield f"data: {decoded}\n\n"

        except Exception as e:
            logger.error(f"Error en SSE stream | session={session_id} | {e}")
        finally:
            if process is not None:
                try:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), timeout=5.0)
                except Exception:
                    try:
                        process.kill()
                    except Exception:
                        pass
            sessions.pop(session_id, None)
            logger.info(f"Sesión limpiada | session={session_id}")

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Desactiva buffering en nginx/Railway
        },
    )


@app.post("/messages")
async def messages_endpoint(request: Request, sessionId: str):
    process = sessions.get(sessionId)
    if not process:
        logger.warning(f"Sesión no encontrada: {sessionId!r}")
        return JSONResponse({"error": "Session not found"}, status_code=404)

    body = await request.body()
    logger.debug(f"Cliente→MCP | session={sessionId} | {body[:120]}")

    try:
        process.stdin.write(body + b"\n")
        await process.stdin.drain()
        return Response(status_code=202)
    except Exception as e:
        logger.error(f"Error escribiendo a MCP | session={sessionId} | {e}")
        sessions.pop(sessionId, None)
        return JSONResponse({"error": str(e)}, status_code=500)


if __name__ == "__main__":
    logger.info(f"Alpaca MCP Gateway arrancando en puerto {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
