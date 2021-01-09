import asyncio
import websockets
from src.ipc import IPC
import uvloop  # Optional
from iec62056_21.client_ import Iec6205621Client
from iec62056_21.messages import ProfileData
from typing import List
from datetime import date

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class MyClass:
    _lock = asyncio.locks.Lock()
    _serial_client = None

    async def connect_serial(self):
        await self._lock.acquire()
        self._serial_client = Iec6205621Client.with_serial_transport(port='/dev/ttyUSB0')
        self._serial_client.connect()

    async def get_profile(self, start_date: date, end_date: date) -> List[ProfileData]:
        await self.connect_serial()
        res = self._serial_client.read_profile(start_date, end_date)
        self._lock.release()
        return [data.to_json() for data in res]

    async def get_standard_data(self):
        """
        Change readout format
        """
        await self.connect_serial()
        res = self._serial_client.standard_readout()
        self._lock.release()
        return res.to_json()


async def echo(websocket, path):
    ipc_server = IPC(ws=websocket, cls=MyClass(), mode="server")
    await ipc_server.listen()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(websockets.serve(echo, '0.0.0.0', 5020, max_size=1_000_000_000))
    asyncio.get_event_loop().run_forever()
