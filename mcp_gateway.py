import asyncio
import json
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
MCP_ENV = {**os.environ, "PYTHONUNBUFFERED": "1"}
MCP_CMD = ["alpaca-mcp-server", "serve"]

sessions: dict[str, asyncio.subprocess.Process] = {}

app = FastAPI(title="Alpaca MCP Gateway")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _log_msg(direction: str, sid: str, raw: str) -> None:
    """Log completo de cada mensaje MCP en ambas direcciones."""
    try:
        parsed = json.loads(raw)
        method = parsed.get("method", "")
        msg_id = parsed.get("id", "-")
        if "result" in parsed:
            # Respuesta: mostrar las keys del result
            result_keys = list(parsed["result"].keys()) if isinstance(parsed["result"], dict) else "scalar"
            logger.info(f"{'─'*4} {direction} [{sid[:8]}] id={msg_id} RESULT keys={result_keys}")
            logger.info(f"     FULL: {raw[:500]}")
        elif "error" in parsed:
            logger.error(f"{'─'*4} {direction} [{sid[:8]}] id={msg_id} ERROR: {parsed['error']}")
        else:
            logger.info(f"{'─'*4} {direction} [{sid[:8]}] id={msg_id} method={method!r}")
            logger.info(f"     FULL: {raw[:500]}")
    except json.JSONDecodeError:
        logger.warning(f"{'─'*4} {direction} [{sid[:8]}] RAW (no JSON): {raw[:200]}")


def _patch_initialize_response(raw: str, sid: str) -> str:
    """
    Si la respuesta del initialize no incluye 'tools' en capabilities,
    lo inyectamos. Sin esto, Claude nunca envía tools/list.
    """
    try:
        msg = json.loads(raw)
        result = msg.get("result", {})
        caps = result.get("capabilities")
        if caps is None:
            return raw  # No es una respuesta initialize

        modified = False
        if "tools" not in caps:
            caps["tools"] = {}
            modified = True
            logger.info(f"[PATCH] [{sid[:8]}] Injected 'tools:{{}}' en capabilities")

        if modified:
            return json.dumps(msg)
    except (json.JSONDecodeError, TypeError, AttributeError):
        pass
    return raw


async def _drain_stderr(process: asyncio.subprocess.Process, sid: str) -> None:
    """Lee stderr del subprocess en background y lo vuelca al log."""
    while True:
        line = await process.stderr.readline()
        if not line:
            break
        logger.warning(f"[STDERR] [{sid[:8]}] {line.decode().strip()}")


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "active_sessions": len(sessions)}


@app.get("/sse")
async def sse_endpoint(request: Request):
    sid = str(uuid.uuid4())
    logger.info(f"══ Nueva conexión SSE | session={sid}")
    logger.info(f"   ALPACA_API_KEY  : {'SET' if os.environ.get('ALPACA_API_KEY') else '*** MISSING ***'}")
    logger.info(f"   ALPACA_SECRET_KEY: {'SET' if os.environ.get('ALPACA_SECRET_KEY') else '*** MISSING ***'}")
    logger.info(f"   ALPACA_PAPER_TRADE: {os.environ.get('ALPACA_PAPER_TRADE', 'not set')}")

    async def event_stream() -> AsyncGenerator[str, None]:
        process: asyncio.subprocess.Process | None = None

        try:
            # ── 1. Arrancar subprocess y registrar sesión ──────────────────
            process = await asyncio.create_subprocess_exec(
                *MCP_CMD,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=MCP_ENV,
            )
            sessions[sid] = process
            logger.info(f"   MCP pid={process.pid} arrancado")

            # Dar 200ms para que el subprocess arranque y detectar crash inmediato
            await asyncio.sleep(0.2)
            if process.returncode is not None:
                logger.error(f"   MCP MURIÓ en arranque | exit={process.returncode}")
                yield f"event: error\ndata: MCP process crashed on startup (exit={process.returncode})\n\n"
                return

            # Drenar stderr en background
            asyncio.create_task(_drain_stderr(process, sid))

            # ── 2. Enviar endpoint: sesión ya registrada, sin race condition ─
            yield f"event: endpoint\ndata: /messages?sessionId={sid}\n\n"
            logger.info(f"   Endpoint enviado a Claude → POST /messages?sessionId={sid}")

            # ── 3. Proxy stdout → SSE (con intercepción de initialize) ──────
            while True:
                if await request.is_disconnected():
                    logger.info(f"   Cliente desconectó [{sid[:8]}]")
                    break

                try:
                    line = await asyncio.wait_for(
                        process.stdout.readline(), timeout=15.0
                    )
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
                    continue

                if not line:
                    exit_code = process.returncode
                    logger.error(f"   MCP cerró stdout [{sid[:8]}] | exit={exit_code}")
                    break

                raw = line.decode().strip()
                if not raw:
                    continue

                # Interceptar respuesta de initialize para garantizar tools en caps
                patched = _patch_initialize_response(raw, sid)
                _log_msg("MCP→Claude", sid, patched)
                yield f"data: {patched}\n\n"

        except FileNotFoundError:
            logger.error(f"   alpaca-mcp-server no encontrado en PATH [{sid[:8]}]")
            yield "event: error\ndata: alpaca-mcp-server not found in PATH\n\n"
        except Exception as exc:
            logger.error(f"   Error inesperado [{sid[:8]}]: {exc}", exc_info=True)
        finally:
            sessions.pop(sid, None)
            if process is not None:
                exit_code = process.returncode
                if exit_code is None:
                    try:
                        process.terminate()
                        await asyncio.wait_for(process.wait(), timeout=5.0)
                        exit_code = process.returncode
                    except Exception:
                        process.kill()
                logger.info(f"══ Sesión cerrada [{sid[:8]}] | MCP exit={exit_code}")

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/messages")
async def messages_endpoint(request: Request, sessionId: str):
    process = sessions.get(sessionId)
    if not process:
        logger.warning(f"POST a sesión inexistente: {sessionId!r}")
        return JSONResponse({"error": "Session not found"}, status_code=404)

    body = await request.body()
    _log_msg("Claude→MCP", sessionId, body.decode())

    try:
        process.stdin.write(body + b"\n")
        await process.stdin.drain()
        return Response(status_code=202)
    except BrokenPipeError:
        logger.error(f"Pipe rota — MCP muerto | session={sessionId[:8]}")
        sessions.pop(sessionId, None)
        return JSONResponse({"error": "MCP process died"}, status_code=500)
    except Exception as exc:
        logger.error(f"Error write stdin | session={sessionId[:8]} | {exc}")
        sessions.pop(sessionId, None)
        return JSONResponse({"error": str(exc)}, status_code=500)


if __name__ == "__main__":
    logger.info(f"Alpaca MCP Gateway en puerto {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
