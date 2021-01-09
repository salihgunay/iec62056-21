import asyncio
import websockets
from itertools import count
from pickle import loads, dumps


class IPC:
    MAX_MSG_ID = 2 ** 32
    PING_INTERVAL = 10
    _listen_task = None
    _auto_ping_task = None

    def __init__(self, uri=None, mode="client", ws=None, cls=None):
        self.uri = uri
        self.mode = mode
        self._iter = count()
        self.tasks = {}
        if isinstance(ws, websockets.WebSocketServerProtocol):
            self.ws = ws
        if mode == "server":
            self.cls = cls
        if mode == "client":
            self.proxy = Proxy(self._send)

    async def connect(self):
        self.ws = await websockets.connect(self.uri, max_size=1_000_000_000, ping_interval=None)
        self._listen_task = asyncio.create_task(self.listen())
        self._auto_ping_task = asyncio.create_task(self._auto_ping())

    async def _auto_ping(self):
        while True:
            await self._send('ping_')
            await asyncio.sleep(self.PING_INTERVAL)

    async def disconnect(self):
        self._listen_task.cancel()
        self._auto_ping_task.cancel()
        await self.ws.close()

    async def listen(self):
        try:
            async for message in self.ws:
                res = loads(message)
                asyncio.ensure_future(self._on_message(res['msg_id'], res['function_name'], *res['args'],
                                                       result=res['result'], error=res.get('error'), **res['kwargs']))
        except websockets.ConnectionClosedError as e:
            print(f"Connection Closed Error in _listen: {e}")
        except Exception as e:
            print(f"Other Exception: {e}")

    async def _on_message(self, msg_id, function_name, *args, result=None, error=False, **kwargs):
        if self.mode == "client":
            if error:
                self.tasks.pop(msg_id).set_exception(result)
            else:
                self.tasks.pop(msg_id).set_result(result)
        else:
            message = {'msg_id': msg_id, 'function_name': function_name, 'args': (), 'kwargs': {}}
            if function_name == 'ping_':  # Special function name for auto ping
                message['result'] = True
            else:
                try:
                    func = getattr(self.cls, function_name)
                    message['result'] = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) \
                        else func(*args, **kwargs)
                except Exception as e:
                    message['error'] = True
                    message['result'] = e
            await self.ws.send(dumps(message))

    async def _send(self, function_name, *args, **kwargs):
        try:
            msg_id = self._get_next_msg_id()
            message = {
                'msg_id': msg_id,
                'function_name': function_name,
                'args': args,
                'kwargs': kwargs,
                'result': None,
                'error': False
            }
            fut = asyncio.Future()
            self.tasks[msg_id] = fut
            await self.ws.send(dumps(message))
            return await fut
        except websockets.ConnectionClosedError as e:
            print(f"Connection Closed Error in send: {e}")
        except KeyError as e:
            print(f"Key Error in send: {e}")
        except Exception as e:
            print(f"Other Exception: {e}")

    def _get_next_msg_id(self) -> int:
        i = next(self._iter)
        if i < self.MAX_MSG_ID:
            return i
        self._iter = count()
        return self._get_next_msg_id()


class Proxy:
    def __init__(self, send):
        self.send = send

    def __getattr__(self, function_name):
        async def func(*args, **kwargs):
            return await self.send(function_name, *args, **kwargs)
        return func
