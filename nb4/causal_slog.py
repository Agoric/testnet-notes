# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# ## Preface: PyData

import pandas as pd
import numpy as np
dict(pandas=pd.__version__,
     numpy=np.__version__)

bk1 = !/home/customer/projects/gztool/gztool -v 0 -L 12160222 -R 15432 slogfiles/Agoric_by_QuantNode/Agoric_by_QuantNode-agorictest16.slog.gz

# causal:
# vat 1: result promise from send...
# vat 2: deliver result promise kpid
#
# vat 2: resolve same kpid
# vat 1: notify

# +
import json
import itertools


def cranks(lines):
    crankNum = None
    delivery = None
    deliveryNode = None
    syscall = None
    vatDelivery = {}
    prevSyscall = None
    d = 0
    for line in lines:
        entry = json.loads(line)
        ty = entry['type']
        # if ty not in ['deliver', 'deliver-result', 'syscall', 'syscall-result', 'clist']:
        #    log.info('entry %s %s', ty, entry.get('vatID'))
        if ty == 'deliver':
            prevSyscall = None
            kd = entry['kd']
            vatID = entry['vatID']
            if vatID in []: # comms 'v13', , vattp 'v14' are kinda boring
                continue
            deliveryNum = entry['deliveryNum']
            delivery = entry
            if kd[0] == 'message':
                [_, target, msg] = kd
                method = msg['method']
                slots = msg['args']['slots']
                result = msg['result']
                node = f'D:{target}@{vatID}#{deliveryNum}\n.{method}'
                deliveryNode = node
                prev = vatDelivery.get(vatID)
                if prev:
                    yield (prev, node, {'style': 'dotted'})  # next
                vatDelivery[vatID] = node
                #@@ yield (target, node, {'label': 'targetOf'})
                if result:
                    yield (result, node, {}) # 'label': 'resultOf'
                #for slot in slots:
                #    yield (slot, node, {'label': 'slotOfD'})
            elif kd[0] == 'notify':
                primarykp = kd[1][0][0]
                node = f'DN:{primarykp}@{vatID}#{deliveryNum}({len(kd[1])})'
                deliveryNode = node
                # prev = vatDelivery.get(vatID)
                # if prev:
                #     yield (prev, node, {'style': 'dotted'})  # next
                vatDelivery[vatID] = node
                for [kp, info] in kd[1]:
                    yield (f'R:{kp}', node, {}) # 'label': 'notifies'
                    if info['state'] == 'fulfilled':
                        slots = info['data']['slots']
                        for slot in slots:
                            pass
                    else:
                        raise NotImplementedError(info)
            elif kd[0] in ['dropExports']:
                pass
            else:
                raise NotImplementedError(kd)
            # log.info('delivery %s', json.dumps(entry, indent=2))
            #     body = json.loads(kd[2]['args']['body'])
            #     log.info('body: %s', json.dumps(body, indent=2))
            d += 1
            #if d > 100:
            #    break
        elif ty == 'deliver-result':
            delivery = None
        elif ty == 'syscall':
            if not delivery:
                continue
            # log.info('syscall %s', entry['ksc'][0])
            # log.info('syscall %s', entry['vsc'][0])
            # log.info('syscall %s', entry)
            syscall = entry
            ksc = entry['ksc']
            syscallNum = entry['syscallNum']
            if ksc[0] == 'resolve':
                node = f'S:{vatID}:{deliveryNum}:{syscallNum}\n#{ksc[0]}'
                if syscallNum > 0 and prevSyscall:
                    yield (prevSyscall, node, {'style': 'dotted', 'color': 'blue'})
                prevSyscall = node
                yield (deliveryNode, node, {})  # 'label': 'syscall'
                for kp, _b, data in ksc[2]:
                    yield (node, f'R:{kp}', {})  # 'label': 'resolve'
                    for slot in data['slots']:
                        pass
                        # yield (node, slot, {'label': 'slotOfR'})
            elif ksc[0] == 'send':
                node = f'S:{vatID}:{deliveryNum}:{syscallNum}\n#{ksc[0]}\n.{ksc[2]["method"]}'
                if syscallNum > 0 and prevSyscall:
                    yield (prevSyscall, node, {'style': 'dotted', 'color': 'blue'})
                prevSyscall = node
                yield (deliveryNode, node, {})  # 'label': 'syscall'
                target = ksc[1]
                method = ksc[2]['method']
                slots = ksc[2]['args']['slots']
                result = ksc[2]['result']
                if result:
                    yield (node, result, {})  # 'label': 'result'
                # for slot in data['slots']:
                #    pass
                # yield (slot, node, {'label': 'slotOfSend'})
            elif ksc[0] in ['subscribe', 'invoke', 'dropImports', 'retireImports', 'retireExports',
                            'vatstoreGet', 'vatstoreSet', 'vatstoreDelete']:
                pass
            else:
                raise NotImplementedError(ksc)
        elif ty == 'syscall-result':
            syscall = None


arcs = list(cranks(bk1))
arcs[:3]
# entries = [json.loads(line) for line in bk1]
# df = pd.DataFrame.from_records(entries)
# df[df.type.isin(['deliver', 'deliver-result', 'syscall', 'syscall-result'])]

# +
import sys
import re

colors = {'1': '#ccffcc', # bank
          '5': '#aaffaa', # mints
          '10': '#ffcccc', # zoe
          '11': '#bbffbb', # bootstrap
          '13': '#f8fff8', #comms
          '14': '#ffeeee', # vattp
          '16': 'orange', # treasury
          '18': '#ffffcc', # contract vat
         }


def tographviz(arcs, out):
    lit = json.dumps
    out.write('digraph G {\n')
    for (src, dest, attrs) in arcs:
        out.write(f'{lit(src)} -> {lit(dest)}')
        if attrs:
            out.write(' [')
            sep = ''
            for name, value in attrs.items():
                out.write(f'{sep}{name}={lit(value)}')
            out.write(']')
        out.write('\n')
        for n in [src, dest]:
            color = 'white'
            if n.startswith('D'):
                vid = re.search(r'v(\d+)', n).group(1)
                color = colors.get(vid, 'white')
            if re.search(r'^DN:', n):
                out.write(f'{lit(n)} [fillcolor="{color}" style="filled"]\n')
            elif re.search(r'^D:', n):
                out.write(f'{lit(n)} [shape="box" fillcolor="{color}" style="filled"]\n')
            #if re.match(r'^ko\d+$', n):
            #    out.write(f'{lit(n)} [fillcolor=red]\n')
    out.write('}\n')

with open('bk1.gv', 'w') as out:
    tographviz(arcs, out)
# -

# ## MySQL

TOP = __name__ == '__main__'

# +
import logging
from sys import stderr

logging.basicConfig(level=logging.INFO, stream=stderr,
                    format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger(__name__)
if TOP:
    log.info('notebook start')

# +
from slogdata import mysql_socket, show_times

if TOP:
    def _slog4db(database='slog4'):
        from sqlalchemy import create_engine
        return create_engine(mysql_socket(database, create_engine))

    _db4 = _slog4db()

TOP and _db4.execute('show tables').fetchall()


# +
def _exec(sql, db=_db4):
    log.info('%s', sql)
    db.execute(sql)
    log.info('done')

_exec('''
create index if not exists slog_entry_ty on slog_entry(type)
''')
_exec('''
create index if not exists slog_entry_bk on slog_entry(blockHeight)
''')
