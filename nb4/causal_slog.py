"""causal_slog - Swingset Log (slogfile) causal visualization

Swingset logs (slogfiles) record events in the Agoric smart contract
platform, which provides asynchronous messaging between JavaScript
event loops. For example, this deliver event (formatted for readability):

{
  "time": 1625269779.8321261,
  "type": "deliver",
  "crankNum": 293340,
  "vatID": "v1",
  "deliveryNum": 36694,
  "kd": [
    "message",
    "ko571",
    {
      "method": "getCurrentAmount",
      "args": {
        "body": "[]",
        "slots": []
      },
      "result": "kp116645"
    }
  ],
  "vd": [ ... ]
}

comes from running code such as

  const p = E(o).getCurrentAmount()

which calls the `getCurrentAmount` method on an object `o` that the
swingset kernel tracks as `ko571`, where the result should be sent to
promise `p`, tracked in the kernel as `kp116645`.

 - "vatID": "v1" tells which vat, i.e. which JavaScript
   event loop, the object `o` lives in.
 - "deliveryNum": 36694 indicates the that this is 36694th
   delivery to this vat
 - "crankNum": 293340 indicates that this is the 293340th
   delivery that the swingset kernel has made across all vats.

We visualize this as a box labelled:

  D:ko571.getCurrentAmount
  @v1#36694

with an arc coming in from a node labelled `kp116645`.

In response to this message, a vat may make "syscalls" to queue
messages (shown `S:v13:69955:53 #send .transmit`) or resolve promises
(shown `S:v1:36694:0 #resolve`).

See also
https://github.com/Agoric/agoric-sdk/blob/master/packages/SwingSet/docs/delivery.md # noqa

"""

import json
import logging
import re

log = logging.getLogger(__name__)


def main(stdin, stdout):
    log.info('to get block 78840: %s', ' '.join(bk1()))
    arcs = AgViz().slog_arcs(stdin)
    GraphViz.format_graph(arcs, stdout, style=AgViz)


def bk1(L=12160222, R=15432,
        root='slogfiles',
        parent='Agoric_by_QuantNode',
        name='Agoric_by_QuantNode-agorictest16.slog.gz',
        bin='/home/customer/projects/gztool/gztool'):
    cmd = [bin, '-v', '0', '-L', str(L), '-R', str(R),
           f'{root}/{parent}/{name}']
    return cmd


class AgViz:
    colors = {
        '1': '#ccffcc',   # bank
        '5': '#aaffaa',   # mints
        '10': '#ffcccc',  # zoe
        '11': '#bbffbb',  # bootstrap
        '13': '#f8fff8',  # comms
        '14': '#ffeeee',  # vattp
        '16': 'orange',   # treasury
        '18': '#ffffcc',  # contract vat
    }

    @classmethod
    def node_style(cls, n):
        color = 'white'
        if n.startswith('D'):
            vid = re.search(r'v(\d+)', n).group(1)
            color = cls.colors.get(vid, 'white')
        if re.search(r'^DN:', n):
            return dict(fillcolor=color, style='filled')
        elif re.search(r'^D:', n):
            return dict(shape='box', fillcolor=color, style='filled')
        return None

    def __init__(self):
        self.vatID = None
        self.deliveryNum = None
        self.deliveryNode = None
        self.vatDelivery = {}
        self.prevSyscall = None

    def slog_arcs(self, lines,
                  # comms 'v13', , vattp 'v14' are kinda boring
                  exclude_vats=[]):
        delivery = None

        for line in lines:
            entry = json.loads(line)
            ty = entry['type']
            if ty == 'deliver':
                self.prevSyscall = None
                self.vatID = entry['vatID']
                if self.vatID in exclude_vats:
                    continue
                delivery = entry
                self.deliveryNum = entry['deliveryNum']
                for arc in self.delivery_arcs(entry):
                    yield arc
            elif ty == 'deliver-result':
                delivery = None
            elif ty == 'syscall':
                if not delivery:
                    continue
                for arc in self.syscall_arcs(entry):
                    yield arc
            elif ty == 'syscall-result':
                pass

    def delivery_arcs(self, entry):
        vatID, deliveryNum = self.vatID, self.deliveryNum
        kd = entry['kd']
        if kd[0] == 'message':
            [_, target, msg] = kd
            method = msg['method']
            result = msg['result']
            node = f'D:{target}.{method}\n@{vatID}#{deliveryNum}'
            self.deliveryNode = node
            prev = self.vatDelivery.get(vatID)
            if prev:
                yield (prev, node, {'style': 'dotted'})  # next
            self.vatDelivery[vatID] = node
            # yield (target, node, {'label': 'targetOf'})
            if result:
                yield (result, node, {})  # 'label': 'resultOf'
            # slots = msg['args']['slots']
            # for slot in slots:
            #    yield (slot, node, {'label': 'slotOfD'})
        elif kd[0] == 'notify':
            primarykp = kd[1][0][0]
            node = f'DN:{primarykp}({len(kd[1])})\n@{vatID}#{deliveryNum}'
            self.deliveryNode = node
            # prev = vatDelivery.get(vatID)
            # if prev:
            #     yield (prev, node, {'style': 'dotted'})  # next
            self.vatDelivery[vatID] = node
            for [kp, info] in kd[1]:
                yield (f'R:{kp}', node, {})  # 'label': 'notifies'
                if info['state'] == 'fulfilled':
                    slots = info['data']['slots']
                    for slot in slots:
                        pass
                else:
                    raise NotImplementedError(info)
        elif kd[0] in ['dropExports', 'retireImports', 'retireExports']:
            pass
        else:
            raise NotImplementedError(kd)
        # log.info('delivery %s', json.dumps(entry, indent=2))
        #     body = json.loads(kd[2]['args']['body'])
        #     log.info('body: %s', json.dumps(body, indent=2))

    def syscall_arcs(self, entry):
        vatID, deliveryNum = self.vatID, self.deliveryNum
        # log.info('syscall %s', entry['ksc'][0])
        # log.info('syscall %s', entry['vsc'][0])
        # log.info('syscall %s', entry)
        ksc = entry['ksc']
        syscallNum = entry['syscallNum']

        style = {'style': 'dotted', 'color': 'blue'}

        if ksc[0] == 'resolve':
            node = f'S:{vatID}:{deliveryNum}:{syscallNum}\n#{ksc[0]}'
            if syscallNum > 0 and self.prevSyscall:
                yield (self.prevSyscall, node, style)
            self.prevSyscall = node
            yield (self.deliveryNode, node, {})  # 'label': 'syscall'
            for kp, _b, _data in ksc[2]:
                yield (node, f'R:{kp}', {})  # 'label': 'resolve'
                # for slot in data['slots']:
                #    pass
                #    yield (node, slot, {'label': 'slotOfR'})
        elif ksc[0] == 'send':
            node = (f'S:{vatID}:{deliveryNum}:{syscallNum}\n' +
                    f'#{ksc[0]}\n.{ksc[2]["method"]}')
            if syscallNum > 0 and self.prevSyscall:
                yield (self.prevSyscall, node, style)
            self.prevSyscall = node
            yield (self.deliveryNode, node, {})  # 'label': 'syscall'
            result = ksc[2]['result']
            if result:
                yield (node, result, {})  # 'label': 'result'
            # target = ksc[1]
            # method = ksc[2]['method']
            # slots = ksc[2]['args']['slots']
            # for slot in data['slots']:
            #    pass
            # yield (slot, node, {'label': 'slotOfSend'})
        elif ksc[0] in ['subscribe', 'invoke',
                        'dropImports', 'retireImports', 'retireExports',
                        'vatstoreGet', 'vatstoreSet', 'vatstoreDelete']:
            pass
        else:
            raise NotImplementedError(ksc)


"""TODO:

subgraph cluster_key {
 label="Legend";
 v1 -> v10 -> v11 -> v13 -> v14 -> v16 -> v18 [style="invisible"]
 v1 [shape="box" style="filled" color="#ccffcc" label="v1: bank"]
 v10 [shape="box" style="filled" color="#ffcccc" label="v10: zoe"]
 v11 [shape="box" style="filled" color="#bbffbb" label="v11: bootstrap"]
 v13 [shape="box" style="filled" color="#f8fff8" label="v13: comms"]
 v14 [shape="box" style="filled" color="#ffeeee" label="v14: vattp"]
 v16 [shape="box" style="filled" color="orange" label="v16: treasury"]
 v18 [shape="box" style="filled" color="#ffffcc" label="v18: loadgen"]
}
"""


class GraphViz:
    @classmethod
    def format_graph(cls, arcs, out,
                     style=None):
        lit = json.dumps

        def attrfmt(attrs):
            parts = [f'{name}={lit(value)}'
                     for name, value in attrs.items()]
            return f'[{" ".join(parts)}]'

        nodes = set()
        out.write('digraph G {\n')
        for (src, dest, attrs) in arcs:
            out.write(f'{lit(src)} -> {lit(dest)}')
            if attrs:
                out.write(' ')
                out.write(attrfmt(attrs))
            out.write('\n')
            nodes |= set([src, dest])

        if style:
            for n in nodes:
                attrs = style.node_style(n)
                if attrs:
                    out.write(f'{lit(n)} {attrfmt(attrs)}\n')
        out.write('}\n')


if __name__ == '__main__':
    def _script():
        from sys import stdin, stdout, stderr

        logging.basicConfig(
            level=logging.INFO, stream=stderr,
            format='%(asctime)s %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')
        main(stdin, stdout)

    _script()
