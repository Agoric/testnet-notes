# causal:
# vat 1: result promise from send...
# vat 2: deliver result promise kpid
#
# vat 2: resolve same kpid
# vat 1: notify


import json
import logging
import re

log = logging.getLogger(__name__)


def main(stdin, stdout):
    log.info('to get block 78840: %s', ' '.join(bk1()))
    arcs = cranks(stdin)
    tographviz(arcs, stdout)


def bk1(L=12160222, R=15432,
        root='slogfiles',
        parent='Agoric_by_QuantNode',
        name='Agoric_by_QuantNode-agorictest16.slog.gz',
        bin='/home/customer/projects/gztool/gztool'):
    cmd = [bin, '-v', '0', '-L', str(L), '-R', str(R),
           f'{root}/{parent}/{name}']
    return cmd


def cranks(lines,
           # comms 'v13', , vattp 'v14' are kinda boring
           exclude_vats=[]):
    delivery = None
    deliveryNode = None
    vatDelivery = {}
    prevSyscall = None

    for line in lines:
        entry = json.loads(line)
        ty = entry['type']
        # if ty not in ['deliver', 'deliver-result',
        #               'syscall', 'syscall-result', 'clist']:
        #    log.info('entry %s %s', ty, entry.get('vatID'))
        if ty == 'deliver':
            prevSyscall = None
            kd = entry['kd']
            vatID = entry['vatID']
            if vatID in exclude_vats:
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
                # yield (target, node, {'label': 'targetOf'})
                if result:
                    yield (result, node, {})  # 'label': 'resultOf'
                # for slot in slots:
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
                    yield (f'R:{kp}', node, {})  # 'label': 'notifies'
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
        elif ty == 'deliver-result':
            delivery = None
        elif ty == 'syscall':
            if not delivery:
                continue
            # log.info('syscall %s', entry['ksc'][0])
            # log.info('syscall %s', entry['vsc'][0])
            # log.info('syscall %s', entry)
            ksc = entry['ksc']
            syscallNum = entry['syscallNum']
            if ksc[0] == 'resolve':
                node = f'S:{vatID}:{deliveryNum}:{syscallNum}\n#{ksc[0]}'
                if syscallNum > 0 and prevSyscall:
                    yield (prevSyscall, node,
                           {'style': 'dotted', 'color': 'blue'})
                prevSyscall = node
                yield (deliveryNode, node, {})  # 'label': 'syscall'
                for kp, _b, data in ksc[2]:
                    yield (node, f'R:{kp}', {})  # 'label': 'resolve'
                    for slot in data['slots']:
                        pass
                        # yield (node, slot, {'label': 'slotOfR'})
            elif ksc[0] == 'send':
                node = (f'S:{vatID}:{deliveryNum}:{syscallNum}\n' +
                        f'#{ksc[0]}\n.{ksc[2]["method"]}')
                if syscallNum > 0 and prevSyscall:
                    yield (prevSyscall, node,
                           {'style': 'dotted', 'color': 'blue'})
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
            elif ksc[0] in ['subscribe', 'invoke',
                            'dropImports', 'retireImports', 'retireExports',
                            'vatstoreGet', 'vatstoreSet', 'vatstoreDelete']:
                pass
            else:
                raise NotImplementedError(ksc)
        elif ty == 'syscall-result':
            pass


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


def tographviz(arcs, out):
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

    for n in nodes:
        color = 'white'
        if n.startswith('D'):
            vid = re.search(r'v(\d+)', n).group(1)
            color = colors.get(vid, 'white')
        if re.search(r'^DN:', n):
            style = dict(fillcolor=color, style='filled')
            out.write(f'{lit(n)} [fillcolor="{color}" style="filled"]\n')
        elif re.search(r'^D:', n):
            style = dict(shape='box', fillcolor=color, style='filled')
            out.write(f'{lit(n)} {attrfmt(style)}')
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
