"""gayrun 中継サーバー。

同じ合言葉（のハッシュ）で入ってきた2人をペアにして、
片方から届いたメッセージをもう片方へそのまま流すだけのサーバー。
Render などの無料サービスで動かす想定（ポートは環境変数 PORT）。

Render の中継（プロキシ）は、ゲーム側が切断しても数秒〜十数秒サーバーに伝えないことがある。
そのため切断の検知だけに頼らず、
- "bye" が届いたらすぐに部屋を空ける
- しばらく何も送ってこない参加者は、次の人が来たときに追い出す
ようにしている（ゲーム側は待っている間も対戦中も、1〜2秒ごとに何か送ってくる）。
"""
import asyncio
import json
import logging
import os
import time
from http import HTTPStatus

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed
from websockets.protocol import State

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("relay")

# この秒数なにも届かない参加者は、いなくなったものとみなす
STALE_WAITING = 6.0   # 相手を待っている人（2秒ごとに送ってくる）
STALE_PAIRED = 10.0   # 対戦中の人（1秒ごとに送ってくる。ゲーム側は8秒で切断あつかい）

rooms: dict[str, list[ServerConnection]] = {}
room_of: dict[ServerConnection, str] = {}
peers: dict[ServerConnection, ServerConnection] = {}
last_seen: dict[ServerConnection, float] = {}


def process_request(connection, request):
    """WebSocket 以外のアクセス（起こすためのアクセスや死活確認）には ok を返す。"""
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return connection.respond(HTTPStatus.OK, "gayrun relay ok v2\n")
    return None


def _alive(ws: ServerConnection) -> bool:
    limit = STALE_PAIRED if ws in peers else STALE_WAITING
    return ws.state is State.OPEN and time.monotonic() - last_seen.get(ws, 0.0) < limit


def _detach(ws: ServerConnection) -> ServerConnection | None:
    """ws を部屋とペアから外す。ペアの相手がいれば返す。"""
    room = room_of.pop(ws, None)
    members = rooms.get(room) if room is not None else None
    if members is not None:
        if ws in members:
            members.remove(ws)
        if not members:
            del rooms[room]
    last_seen.pop(ws, None)
    other = peers.pop(ws, None)
    if other is not None:
        peers.pop(other, None)
    return other


async def _close(ws: ServerConnection):
    try:
        await ws.close()
    except Exception:
        pass


async def _end(ws: ServerConnection, notify_other: bool):
    """ws を退出させる。ペアの相手がいれば（必要なら知らせてから）相手も閉じる。"""
    other = _detach(ws)
    if other is not None:
        _detach(other)
        if notify_other:
            try:
                await other.send(json.dumps({"t": "peer_left"}))
            except ConnectionClosed:
                pass
        asyncio.create_task(_close(other))
    asyncio.create_task(_close(ws))


async def handler(ws: ServerConnection):
    try:
        first = json.loads(await asyncio.wait_for(ws.recv(), 15))
        if not isinstance(first, dict) or first.get("t") != "join":
            return
        room = str(first.get("room", ""))[:64]
        for m in list(rooms.get(room, [])):
            if not _alive(m):
                log.info("evict stale member")
                await _end(m, notify_other=True)
        members = rooms.setdefault(room, [])
        if len(members) >= 2:
            await ws.send(json.dumps({"t": "full"}))
            return
        members.append(ws)
        room_of[ws] = room
        last_seen[ws] = time.monotonic()
        if len(members) == 2:
            a, b = members
            peers[a], peers[b] = b, a
            await a.send(json.dumps({"t": "paired", "role": "host"}))
            await b.send(json.dumps({"t": "paired", "role": "guest"}))
            log.info("paired (%d rooms)", len(rooms))
        else:
            await ws.send(json.dumps({"t": "waiting"}))

        async for msg in ws:
            if ws not in room_of:
                break  # すでに退出あつかい
            last_seen[ws] = time.monotonic()
            other = peers.get(ws)
            if other is not None:
                try:
                    await other.send(msg)
                except ConnectionClosed:
                    pass
            if '"bye"' in msg:
                try:
                    if json.loads(msg).get("t") == "bye":
                        await _end(ws, notify_other=False)  # 相手には bye がそのまま届いている
                        break
                except ValueError:
                    pass
    except (asyncio.TimeoutError, ValueError, AttributeError, ConnectionClosed):
        pass
    finally:
        await _end(ws, notify_other=True)


async def main():
    port = int(os.environ.get("PORT", "8765"))
    async with serve(handler, "0.0.0.0", port, process_request=process_request,
                     max_size=65536, ping_interval=20, ping_timeout=20) as server:
        log.info("gayrun relay listening on %d", port)
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
