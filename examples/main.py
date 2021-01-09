from iec62056_21.client_ import Iec6205621Client
from datetime import date, timedelta

client = Iec6205621Client.with_serial_transport(port='/dev/ttyUSB0')

client.connect()
start_date, end_date = date.today() - timedelta(days=1), date.today()

#res = client.standard_readout()
res = client.read_profile(start_date, end_date)
print("result", type(res))


