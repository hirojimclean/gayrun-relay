"""gayrun 中継サーバー。

同じ合言葉（のハッシュ）で入ってきた2人をペアにして、
片方から届いたメッセージをもう片方へそのまま流すだけのサーバー。
Render などの無料サービスで動かす想定（ポートは環境変数 PORT）。
"""
import asyncio
import json
import logging
import os
from http import HTTPStatus

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed
from websockets.protocol import State

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("relay")

rooms: dict[str, list[ServerConnection]] = {}
peers: dict[ServerConnection, ServerConnection] = {}


def process_request(connection, request):
    """WebSocket 以外のアクセス（起こすためのアクセスや死活確認）には ok を返す。"""
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return connection.respond(HTTPStatus.OK, "gayrun relay ok\n")
    return None


async def handler(ws: ServerConnection):
    room = None
    try:
        first = json.loads(await asyncio.wait_for(ws.recv(), 15))
        if not isinstance(first, dict) or first.get("t") != "join":
            return
        room = str(first.get("room", ""))[:64]
        members = rooms.setdefault(room, [])
        members[:] = [m for m in members if m.state is State.OPEN]
        if len(members) >= 2:
            await ws.send(json.dumps({"t": "full"}))
            room = None
            return
        members.append(ws)
        if len(members) == 2:
            a, b = members
            peers[a], peers[b] = b, a
            await a.send(json.dumps({"t": "paired", "role": "host"}))
            await b.send(json.dumps({"t": "paired", "role": "guest"}))
            log.info("paired (%d rooms)", len(rooms))
        else:
            await ws.send(json.dumps({"t": "waiting"}))

        async for msg in ws:
            other = peers.get(ws)
            if other is not None:
                try:
                    await other.send(msg)
                except ConnectionClosed:
                    pass
    except (asyncio.TimeoutError, ValueError, ConnectionClosed):
        pass
    finally:
        other = peers.pop(ws, None)
        if other is not None:
            peers.pop(other, None)
            try:
                await other.send(json.dumps({"t": "peer_left"}))
                await other.close()
            except ConnectionClosed:
                pass
        if room is not None and room in rooms:
            if ws in rooms[room]:
                rooms[room].remove(ws)
            if not rooms[room]:
                del rooms[room]


async def main():
    port = int(os.environ.get("PORT", "8765"))
    async with serve(handler, "0.0.0.0", port, process_request=process_request,
                     max_size=65536, ping_interval=20, ping_timeout=20) as server:
        log.info("gayrun relay listening on %d", port)
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
