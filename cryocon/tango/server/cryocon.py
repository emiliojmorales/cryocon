import time
import inspect
import logging
import urllib.parse

from connio import connection_for_url

from tango import DevState, AttrQuality, GreenMode
from tango.server import Device, attribute, command, device_property

import cryocon


def attr(**kwargs):
    name = kwargs['name'].lower()
    func = ATTR_MAP[name]
    dtype = kwargs.setdefault('dtype', float)

    def get(self):
        value = self.last_values[name]
        if isinstance(value, Exception):
            raise value
        value = self.last_values[name]
        if value is None:
            value = float('nan') if dtype == float else ''
            return value, time.time(), AttrQuality.ATTR_INVALID
        return value

    attr = attribute(get, **kwargs)

    sig = inspect.signature(func)
    if len(sig.parameters) > 1:
        @attr.setter
        async def fset(self, value):
            await func(self.cryocon, value)
        fset.__name__ = 'write_' + name
        kwargs['fset'] = fset

    return attr


ATTR_MAP = {
    'idn': lambda cryo: cryo.idn(),
    'control': lambda cryo, v=None: cryo.control(v),
    'channela': lambda cryo: cryo['A'].temperature(),
    'channelb': lambda cryo: cryo['B'].temperature(),
    'channelc': lambda cryo: cryo['C'].temperature(),
    'channeld': lambda cryo: cryo['D'].temperature(),
    'loop1output': lambda cryo, v=None: cryo[1].output_power(v),
    'loop2output': lambda cryo, v=None: cryo[2].output_power(v),
    'loop3output': lambda cryo, v=None: cryo[3].output_power(v),
    'loop4output': lambda cryo, v=None: cryo[4].output_power(v),
    'loop1range': lambda cryo, v=None: cryo[1].range(v),
    'loop1rate': lambda cryo, v=None: cryo[1].rate(v),
    'loop2rate': lambda cryo, v=None: cryo[2].rate(v),
    'loop3rate': lambda cryo, v=None: cryo[3].rate(v),
    'loop4rate': lambda cryo, v=None: cryo[4].rate(v),
    'loop1ramp': lambda cryo, v=None: cryo[1].ramp(v),
    'loop2ramp': lambda cryo, v=None: cryo[2].ramp(v),
    'loop3ramp': lambda cryo, v=None: cryo[3].ramp(v),
    'loop4ramp': lambda cryo, v=None: cryo[4].ramp(v),
    'loop1type': lambda cryo, v=None: cryo[1].type(v),
    'loop2type': lambda cryo, v=None: cryo[2].type(v),
    'loop3type': lambda cryo, v=None: cryo[3].type(v),
    'loop4type': lambda cryo, v=None: cryo[4].type(v),
    'loop1setpoint': lambda cryo, v=None: cryo[1].set_point(v),
    'loop2setpoint': lambda cryo, v=None: cryo[2].set_point(v),
    'loop3setpoint': lambda cryo, v=None: cryo[3].set_point(v),
    'loop4setpoint': lambda cryo, v=None: cryo[4].set_point(v),
    'loop1pgain': lambda cryo: cryo[1].proportional_gain(),
    'loop2pgain': lambda cryo: cryo[2].proportional_gain(),
    'loop3pgain': lambda cryo: cryo[3].proportional_gain(),
    'loop4pgain': lambda cryo: cryo[4].proportional_gain(),
    'loop1igain': lambda cryo: cryo[1].integrator_gain(),
    'loop2igain': lambda cryo: cryo[2].integrator_gain(),
    'loop3igain': lambda cryo: cryo[3].integrator_gain(),
    'loop4igain': lambda cryo: cryo[4].integrator_gain(),
    'loop1dgain': lambda cryo: cryo[1].differentiator_gain(),
    'loop2dgain': lambda cryo: cryo[2].differentiator_gain(),
    'loop3dgain': lambda cryo: cryo[3].differentiator_gain(),
    'loop4dgain': lambda cryo: cryo[4].differentiator_gain(),
}


class CryoCon(Device):

    green_mode = GreenMode.Asyncio

    url = device_property(str)
    baudrate = device_property(dtype=int, default_value=9600)
    bytesize = device_property(dtype=int, default_value=8)
    parity = device_property(dtype=str, default_value='N')

    UsedChannels = device_property([str], default_value='ABCD')
    UsedLoops = device_property([int], default_value=[1, 2, 3, 4])
    ReadValidityPeriod = device_property(float, default_value=0.1)
    AutoLockFrontPanel = device_property(bool, default_value=False)

    channelA = None
    channelB = None
    channelC = None
    channelD = None
    channel_attrs = [channelA, channelB, channelC, channelD]

    loop1output = None
    loop1range = None
    loop1ramp = None
    loop1type = None
    loop1setpoint = None
    loop1pgain = None
    loop1igain = None
    loop1dgain = None
    loop1_attrs = [loop1output, loop1range, loop1ramp, loop1type,
                   loop1setpoint, loop1pgain, loop1igain, loop1dgain]

    loop2output = None
    loop2range = None
    loop2ramp = None
    loop2type = None
    loop2setpoint = None
    loop2pgain = None
    loop2igain = None
    loop2dgain = None
    loop2_attrs = [loop2output, loop2range, loop2ramp, loop2type,
                   loop2setpoint, loop2pgain, loop2igain, loop2dgain]

    loop3output = None
    loop3range = None
    loop3ramp = None
    loop3type = None
    loop3setpoint = None
    loop3pgain = None
    loop3igain = None
    loop3dgain = None
    loop3_attrs = [loop3output, loop3range, loop3ramp, loop3type,
                   loop3setpoint, loop3pgain, loop3igain, loop3dgain]

    loop4output = None
    loop4range = None
    loop4ramp = None
    loop4type = None
    loop4setpoint = None
    loop4pgain = None
    loop4igain = None
    loop4dgain = None
    loop4_attrs = [loop4output, loop4range, loop4ramp, loop4type,
                   loop4setpoint, loop4pgain, loop4igain, loop4dgain]

    all_loop_attrs = [loop1_attrs, loop2_attrs, loop3_attrs, loop4_attrs]

    def delete_loops(self):
        for item in self.all_loop_attrs:
            for l in item:
                item = None
        return True

    def delete_channels(self):
        for item in self.channel_attrs:
            item = None

    def url_to_connection_args(self):
        url = self.url
        res = urllib.parse.urlparse(url)
        kwargs = dict(concurrency="async")
        if res.scheme in {"serial", "rfc2217"}:
            kwargs.update(dict(baudrate=self.baudrate, bytesize=self.bytesize,
                               parity=self.parity))
        elif res.scheme == "tcp":
            if res.port is None:
                url += ":5000"
            kwargs["timeout"] = 0.5
            kwargs["connection_timeout"] = 1
        return url, kwargs

    async def init_device(self):
        await super().init_device()

        channels = ''.join(self.UsedChannels)
        loops = self.UsedLoops

        url, kwargs = self.url_to_connection_args()
        conn = connection_for_url(url, **kwargs)
        self.cryocon = cryocon.CryoCon(conn, channels=channels, loops=loops)

        self.create_channels(channels)
        self.create_loops(loops)

        self.last_values = {}
        self.last_state_ts = 0

    async def delete_device(self):
        super().delete_device()
        try:
            self.delete_channels()
            self.delete_loops()
            await self.cryocon._conn.close()
        except Exception:
            logging.exception('Error closing cryocon')

    async def read_attr_hardware(self, indexes):
        multi = self.get_device_attr()
        names = [
            multi.get_attr_by_ind(index).get_name().lower()
            for index in sorted(indexes)
        ]
        funcs = [ATTR_MAP[name] for name in names]
        async with self.cryocon as group:
            names.insert(0, "control")
            self.cryocon.control()
            for func in funcs:
                func(self.cryocon)
        values = group.replies
        self.last_values = dict(zip(names, values))
        await self._update_state_status(self.last_values['control'])

    async def _update_state_status(self, value=None):
        if value is None:
            ts = time.time()
            if ts < (self.last_state_ts + 1):
                return self.get_state(), self.get_status()
            try:
                value = await self.cryocon.control()
            except Exception as error:
                value = error
        ts = time.time()
        if isinstance(value, Exception):
            state, status = DevState.FAULT, 'Error: {!r}'.format(value)
        else:
            state = DevState.ON if value else DevState.OFF
            status = 'Control is {}'.format('On' if value else 'Off')
        self.set_state(state)
        self.set_status(status)
        self.last_state_ts = ts
        self.__local_status = status  # prevent deallocation by keeping reference
        return state, status

    async def dev_state(self):
        state, status = await self._update_state_status()
        return state

    async def dev_status(self):
        state, status = await self._update_state_status()
        return status

    idn = attr(name='idn', label='ID', dtype=str)

    def create_channels(self, channels):

        channels = [x.upper() for x in channels]
        if 'A' in channels:
            self.channelA = attr(name='channelA', label='Channel A', unit='K')
        if 'B' in channels:
            self.channelB = attr(name='channelB', label='Channel B', unit='K')
        if 'C' in channels:
            self.channelC = attr(name='channelC', label='Channel C', unit='K')
        if 'D' in channels:
            self.channelD = attr(name='channelD', label='Channel D', unit='K')
        if not channels:
            raise ValueError("Please fill device property 'UsedChannels'.")

    def create_loops(self, loops):

        if '1' in loops:
            self.loop1output = attr(name='loop1output', label='Loop 1 Output', unit='%')
            self.loop1range = attr(name='loop1range', label='Loop 1 Range', dtype='str')
            self.loop1ramp = attr(name='loop1ramp', label='Loop 1 Ramp', dtype=bool)
            self.loop1type = attr(name='loop1type', label='Loop 1 Type', dtype=str)
            self.loop1setpoint = attr(name='loop1setpoint', label='Loop 1 SetPoint', unit='K')
            self.loop1pgain = attr(name='loop1pgain', label='Loop 1 P gain')
            self.loop1igain = attr(name='loop1igain', label='Loop 1 I gain', unit='s')
            self.loop1dgain = attr(name='loop1dgain', label='Loop 1 D gain', unit='Hz')

        if '2' in loops:
            self.loop2output = attr(name='loop2output', label='Loop 2 Output', unit='%')
            self.loop2range = attr(name='loop2range', label='Loop 2 Range', dtype='str')
            self.loop2ramp = attr(name='loop2ramp', label='Loop 2 Ramp', dtype=bool)
            self.loop2type = attr(name='loop2type', label='Loop 2 Type', dtype=str)
            self.loop2setpoint = attr(name='loop2setpoint', label='Loop 2 SetPoint', unit='K')
            self.loop2pgain = attr(name='loop2pgain', label='Loop 2 P gain')
            self.loop2igain = attr(name='loop2igain', label='Loop 2 I gain', unit='s')
            self.loop2dgain = attr(name='loop2dgain', label='Loop 2 D gain', unit='Hz')

        if '3' in loops:
            self.loop3output = attr(name='loop3output', label='Loop 3 Output', unit='%')
            self.loop3range = attr(name='loop3range', label='Loop 3 Range', dtype='str')
            self.loop3ramp = attr(name='loop3ramp', label='Loop 3 Ramp', dtype=bool)
            self.loop3type = attr(name='loop3type', label='Loop 3 Type', dtype=str)
            self.loop3setpoint = attr(name='loop3setpoint', label='Loop 3 SetPoint', unit='K')
            self.loop3pgain = attr(name='loop3pgain', label='Loop 3 P gain')
            self.loop3igain = attr(name='loop3igain', label='Loop 3 I gain', unit='s')
            self.loop3dgain = attr(name='loop3dgain', label='Loop 3 D gain', unit='Hz')

        if '4' in loops:
            self.loop4output = attr(name='loop4output', label='Loop 3 Output', unit='%')
            self.loop4range = attr(name='loop4range', label='Loop 3 Range', dtype='str')
            self.loop4ramp = attr(name='loop4ramp', label='Loop 3 Ramp', dtype=bool)
            self.loop4type = attr(name='loop4type', label='Loop 3 Type', dtype=str)
            self.loop4setpoint = attr(name='loop4setpoint', label='Loop 3 SetPoint', unit='K')
            self.loop4pgain = attr(name='loop4pgain', label='Loop 3 P gain')
            self.loop4igain = attr(name='loop4igain', label='Loop 3 I gain', unit='s')
            self.loop4dgain = attr(name='loop4dgain', label='Loop 3 D gain', unit='Hz')

        if not loops:
            raise ValueError("Please fill device property 'UsedLoops'.")

    @command
    def on(self):
        return self.cryocon.control(True)

    @command
    def off(self):
        return self.cryocon.control(False)

    @command(dtype_in=str, dtype_out=str)
    async def run(self, cmd):
        r = await self.cryocon._ask(cmd)
        return r or ''

    @command(dtype_in=[str])
    def setchannelunit(self, unit):
        raise NotImplementedError


def main():
    import logging
    fmt = '%(asctime)s %(levelname)s %(threadName)s %(message)s'
    logging.basicConfig(level=logging.WARNING, format=fmt)
    CryoCon.run_server()


if __name__ == '__main__':
    main()
