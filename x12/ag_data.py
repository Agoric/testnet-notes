# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:hydrogen
#     text_representation:
#       extension: .py
#       format_name: hydrogen
#       format_version: '1.3'
#       jupytext_version: 1.11.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Colophon: Notebook and Module
#
# This is both a jupyter notebook and a python module, sync'd using [jupytext][].
#
# [jupytext]: https://github.com/mwouts/jupytex

# %% [markdown]
# ### OCap Discipline in Python
#
# When loaded as a module, 
# in order to support patterns of cooperation without vulnerability,
# we maintain [OCap Discipline](https://github.com/dckc/awesome-ocap/wiki/DisciplinedPython)
# by avoiding access to IO and other ambient authority,
# except when loaded in the [top-level code environment](https://docs.python.org/3/library/__main__.html#what-is-the-top-level-code-environment), which we can test with `__name__ == '__main__'`.
# Note that jupyter notebooks run in the top-level code environment.

# %%
import logging

log = logging.getLogger(__name__)

IO_TESTING = False

if __name__ == '__main__':
    from sys import version as sys_version, stderr

    IO_TESTING = True
    logging.basicConfig(level=logging.INFO, stream=stderr)
    log.info('python version: %s', dict(
        python=sys_version,
    ))

# %%
from urllib.request import Request, HTTPError, BaseHandler
from io import StringIO

if IO_TESTING:
    # Use leading _ to remind ourselves of ambient authorities
    # that should not be available when loaded as a module.
    def _build_opener():
        from urllib.request import build_opener
        return build_opener()

    _theWeb = _build_opener()
    _theWeb.cache = {}

# %% [markdown]
# ## Cosmos / Tendermint RPC
#
# The Agoric blockchain is based on Cosmos-SDK, which uses Tendermint consensus.
# Each layer has an OpenAPI / Swagger / REST API with interactive explorer style documentation:
#
#  - [Tendermint RPC](https://docs.tendermint.com/master/rpc/)
#  - [Cosmos RPC](https://v1.cosmos.network/rpc/v0.44.5)
#
#
# ### Agoric xnet RPC server
#
# The hostname of the RPC server we're exploring is:

# %%
RPC_HOST = 'xnet.rpc.agoric.net'

# %% [markdown]
# But if we try to access it using the python standard library defaults, we are forbidden:

# %%
if IO_TESTING:
    try:
        _theWeb.open(f'https://{RPC_HOST}/status?')
    except HTTPError as err:
        log.error(err)

# %% [markdown]
# So let's tell a little bit about who we are and what business we are about using `User-Agent` :

# %%
import json

def ua_req(url, user_agent='agoric.com test exploration'):
    return Request(url, headers={'User-Agent': user_agent})

def get_json(url, ua):
    return json.loads(ua.open(ua_req(url)).read())

IO_TESTING and get_json('http://httpbin.org/headers', _theWeb)

# %% [markdown]
# Now we can start exploring:

# %%
IO_TESTING and get_json(f'https://{RPC_HOST}/status?', _theWeb)


# %% [markdown]
# ### JSON Helper

# %% [markdown]
# Navigating JSON structured in python is a bit tedious using `obj['headers']` syntax.
# So let's borrow `obj.headers` style from JavaScript.

# %%
class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

def json_parse(s):
    return json.loads(s, object_hook=AttrDict)

def get_json(url, ua):
    cache = hasattr(ua, 'cache') and ua.cache
    if cache is not None and url in ua.cache:
        return ua.cache[url]
    # log.info('cache miss %s %s', not not cache, url)
    obj = json_parse(ua.open(ua_req(url)).read())
    if cache is not None:
        cache[url] = obj
    return obj

json_parse('{"headers": {"Host": "httpbin.org"}}').headers

# %% [markdown]
# ## What Pools are on the Agoric AMM?

# %% [markdown]
# The AMM, like all Inter Protocol contracts, publishes metrics using via an `x/vstorage` module.

# %%
from base64 import b64decode


class StorageNode:
    """Query Agoric x/vstorage module"""
    def __init__(self, ua, path, base=f'https://{RPC_HOST}'):
        self._path = path
        self._base = base
        self.__ua = ua

    def __repr__(self):
        return self._path

    def _url(self, data=False):
        kind = 'data' if data else 'children'
        return f'{self._base}/abci_query?path=%22/custom/vstorage/{kind}/{self._path}%22&height=0'

    @property
    def _keys_url(self):
        return self._url()
            
    @property
    def _data_url(self):
        return self._url(True)

    def keys(self):
        tendermint_response = get_json(self._keys_url, self.__ua)
        # log.debug('tendermint response %s', tendermint_response)
        return json_parse(b64decode(tendermint_response.result.response.value)).children

    def __call__(self):
        tendermint_response = get_json(self._data_url, self.__ua)
        # log.debug('tendermint response %s', tendermint_response)
        return json_parse(b64decode(tendermint_response.result.response.value)).value

    def __getattr__(self, name):
        return self.__class__(self.__ua, f'{self._path}.{name}', self._base)

IO_TESTING and StorageNode(_theWeb, 'published').keys()

# %%
IO_TESTING and StorageNode(_theWeb, 'published').amm.keys()

# %%
IO_TESTING and StorageNode(_theWeb, 'published').amm.metrics()


# %% [markdown]
# The format follows Agoric distributed objects conventions (specifically, `CapData` from `@endo/marshal`);
# typically, it uses Agoric board IDs for slots:

# %%
class ObjectNode(StorageNode):
    """Query storage nodes using Distribute Objects conventions"""
    
    @classmethod
    def root(cls, ua):
        return cls(path='published', ua=ua)

    @classmethod
    def cleanup(cls, capData):
        return AttrDict({'body': json_parse(capData.body), 'slots': capData.slots})

    def __call__(self, marshaller=None):
        capData = json_parse(StorageNode.__call__(self))
        return marshaller.unserialize(capData) if marshaller else self.cleanup(capData)

if IO_TESTING:
    _amm = ObjectNode.root(_theWeb).amm

IO_TESTING and _amm.metrics()


# %%
class Marshal:
    def __init__(self, convertSlotToVal=lambda slot, iface: slot):
        self.convertSlotToVal = convertSlotToVal

    def unserialize(self, capData):
        s2v = self.convertSlotToVal
        slots = capData.slots
        def object_hook(obj):
            qclass = obj.get('@qclass')
            if qclass == 'slot':
                # TODO? hilbert hotel
                index = obj['index']
                return s2v(slots[index], obj.get('iface'))
            elif qclass == 'bigint':
                return int(obj['digits'])
            return AttrDict(obj)
        return json.loads(capData.body, object_hook=object_hook)


class SlotCache:
    def __init__(self, slotKey):
        self._cache = {}
        self.slotKey = slotKey

    def convertSlotToVal(self, slot, iface):
        cache = self._cache
        if slot in cache:
            return cache[slot]
        obj = AttrDict({self.slotKey: slot, 'iface': iface})
        cache[slot] = obj
        return obj

if IO_TESTING:
    # Preserve identities when deserializing using boardIds
    _fromBoard = Marshal(SlotCache('boardId').convertSlotToVal)

IO_TESTING and _amm.metrics(_fromBoard)

# %% [markdown]
# The `XYK` member of the AMM metrics is a list of brands.
# The format carries their _alleged_ name, for debugging,
# but take care not to rely on it for anything else:

# %%
IO_TESTING and [brand.iface for brand in _amm.metrics(_fromBoard).XYK]

# %% [markdown]
# ### AMM Governance

# %%
IO_TESTING and _amm.governance(_fromBoard)


# %% [markdown]
# ### Brands from the `agoricNames` distinguished name hub
#
# Agoric provides an authoritative collection of brands via the `agoricNames` name hub:

# %%
def fromEntries(entries):
    return AttrDict({key: val for key, val in entries})

class AgoricNames:
    # ISSUE: this treats contents as static when in theory, they could change
    def __init__(self, ua, marshaller):
        root = ObjectNode.root(ua)
        agoricNames = root.agoricNames
        log.info('agoricNames keys %s', agoricNames.keys())
        self.brand = fromEntries(agoricNames.brand(marshaller))
        self.oracleBrand = fromEntries(agoricNames.oracleBrand(marshaller))
        self.issuer = fromEntries(agoricNames.issuer(marshaller))
        self.installation = fromEntries(agoricNames.installation(marshaller))
        self.instance = fromEntries(agoricNames.instance(marshaller))

if IO_TESTING:
    _agoricNames = AgoricNames(_theWeb, _fromBoard)

IO_TESTING and _agoricNames.brand

# %%
IO_TESTING and _amm.metrics(_fromBoard).XYK[0] is _agoricNames.brand.IbcATOM

# %% [markdown]
# ### PyData tools: Pandas DataFrames

# %%
import pandas as pd

log.info(dict(pandas=pd.__version__))

# %%
IO_TESTING and pd.DataFrame.from_dict(_agoricNames.brand, orient='index').rename_axis('keyword')

# %% [markdown]
# ## Status of 1st AMM pool (`pool0`)

# %%
IO_TESTING and _amm.pool0.metrics(_fromBoard)

# %%
from decimal import Decimal

def amt(nameHub):
    def aux(obj, unit=1000000):
        found = [n for n, v in nameHub.items() if v is obj.brand]
        if found:
            [brand] = found
        else:
            log.warning('unknown brand %s', obj.brand)
            brand = obj.brand.iface
        amount = Decimal(obj.value) / unit
        return pd.Series(dict(amount=amount, brand=brand))
    return aux

if IO_TESTING:
    _amt = amt(_agoricNames.brand)

IO_TESTING and _amt(_amm.pool0.metrics(_fromBoard).centralAmount)

# %%
IO_TESTING and pd.DataFrame([pd.Series(_amt(a), name=col)
                             for col, a in _amm.pool0.metrics(_fromBoard).items()])
