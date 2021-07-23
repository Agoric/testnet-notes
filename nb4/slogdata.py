from contextlib import contextmanager
from json import JSONDecodeError
from subprocess import PIPE
from zlib import crc32
import json
import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


def main(argv, dirAccess, create_engine,
         run, Popen,
         debug=True):
    [dbURI, slogfiles, gztoolBin] = argv[1:4]
    gztool = CLI(gztoolBin, run, Popen, debug=debug)
    sa = SlogAccess(dirAccess(slogfiles), gztool)
    db = create_engine(dbURI)
    build_deliveries_table(sa, db)


def build_deliveries_table(sa, con,
                           table='delrun',
                           table_run='run',
                           table_br='blockrun16',
                           json_cols=['kd', 'dr'],
                           # 2021-07-01 19:00:00
                           gen16=1625166000):
    if_exists = 'replace'
    runs = pd.read_sql_table(table_run, con)
    runs['chain'] = np.where(runs.time >= gen16, 16, 15)
    log.info('runs:\n%s', runs.tail())
    blockrun = pd.read_sql(f'select * from {table_br}', con)
    log.info('blockrun:\n%s', blockrun.tail())
    for run_ix, run in runs.iterrows():
        heights = blockrun[blockrun.run == run_ix].blockHeight.unique()
        log.info('run %s %-3d blocks %.16s %s', run_ix, len(heights),
                 pd.to_datetime(run.time, unit='s'), run['name'])
        for blockHeight in heights:
            br = blockrun[(blockrun.run == run.name) &
                          (blockrun.blockHeight == blockHeight)]
            if not len(br):
                continue
            df = sa.provide_deliveries(blockHeight, run, br)
            if not len(df):
                continue
            for col in json_cols:
                df[col] = df[col].apply(json.dumps)
            log.info('inserting %d records into %s', len(df), table)
            df.to_sql(table, con, if_exists=if_exists)
            if_exists = 'append'


def show_times(df, cols=['time', 'blockTime']):
    return df.assign(**{col: pd.to_datetime(df[col], unit='s')
                        for col in df.columns
                        if col in cols})


class CLI:
    def __init__(self, bin, run, popen, debug=False):
        self.bin = bin
        self.__run = run
        self.__popen = popen
        self.debug = debug

    def run(self, *args):
        cmd = [self.bin] + [str(a) for a in args]
        return self.__run(cmd, capture_output=True)

    @contextmanager
    def pipe(self, *args):
        cmd = [self.bin] + [str(a) for a in args]
        if self.debug:
            log.info('pipe: %s', ' '.join(cmd))
        with self.__popen(cmd, stdout=PIPE) as proc:
            yield proc.stdout


def file_size_id(p):
    """file_id index a stable function of each file
    (relative pathname and file size)
    """
    st_size = p.stat().st_size
    refb = (f'{p.parent.name}/{p.name}').encode()
    return st_size, crc32(refb) * 1000000 + st_size


no_deliveries = pd.DataFrame.from_records([
    {'time': 1625198620.6265895,
     'type': 'deliver-result',
     'crankNum': 1291,
     'vatID': 'v11',
     'deliveryNum': 124,
     'kd': object(),
     'line': 1673077,
     'dr': object(),
     'syscalls': 2,
     'method': 'inbound',
     'compute': 119496.0,  # missing compute is possible... from replay.
     'dur': 0.1912224292755127,
     }]).iloc[:0]


def block_cranks(records):
    deliveries = []
    syscalls = 0
    deliver = None
    for record in records:
        ty = record['type']
        if ty == 'deliver':
            deliver = record
            syscalls = 0
        elif ty == 'syscall-result':
            syscalls += 1
        elif ty == 'deliver-result':
            dur = record['time'] - deliver['time']
            method = (deliver['kd'][2]['method']
                      if deliver['kd'][0] == 'message' else None)
            compute = (record['dr'][2]['compute']
                       if type(record['dr'][2]) is type({}) else np.nan)
            detail = dict(record,
                          syscalls=syscalls,
                          kd=deliver['kd'],
                          method=method,
                          compute=compute,
                          dur=dur)
            deliveries.append(detail)
    if deliveries:
        return pd.DataFrame.from_records(deliveries)
    else:
        return no_deliveries


class SlogAccess:
    def __init__(self, slogdir, gztool):
        self.__gztool = gztool
        self.__slogdir = slogdir

    def files_by_size(self,
                      glob='**/*.slog.gz'):
        slogdir = self.__slogdir
        return pd.DataFrame.from_records([
            dict(
                file_id=file_id,
                parent=p.parent.name,
                name=p.name,
                st_size=st_size,
            )
            for p in slogdir.glob(glob)
            for (st_size, file_id) in [file_size_id(p)]
        ]).sort_values('st_size').reset_index(drop=True)

    def line_count(self, parent, name):
        """count lines using binary search
        """
        # log.info('line count: %s', path)
        lo = 0
        hi = 1024
        gztool = self.__gztool
        path = self.__slogdir / parent / name

        while True:
            # log.info('finding upper bound: %d to %d', lo, hi)
            line = gztool.run(str(path), '-L', hi, '-R', 1).stdout
            if not line:
                break
            lo = hi
            hi *= 2
        log.info('%s/%s <= %d', path.parent.name, path.name, hi)
        while hi > lo + 1:
            mid = (hi + lo) // 2
            line = gztool.run(str(path), '-L', mid, '-R', 1).stdout
            # log.info('narrowing: %d (%d to %d) %s', mid, lo, hi, len(line))
            if line:
                lo = mid
            else:
                hi = mid
        log.info('%s/%s = %d', path.parent.name, path.name, lo)
        return lo

    no_runs = pd.DataFrame(dict(time=[0.0],
                                type=[''],
                                line=[0])).iloc[:0]

    def provide_runs(self, parent, name, start, qty):
        return self.provide_data(parent, name, start, qty,
                                 'runs', self.no_runs, self.get_runs)

    def provide_data(self, parent, name, start, qty,
                     kind, none, get, compress=None):
        path = self.__slogdir / parent / name
        gz = '.gz' if compress == 'gzip' else ''
        dest = self.__slogdir / parent / f'{path.stem}-{start}-{kind}.csv{gz}'
        if dest.exists():
            # dtype is important for empty CSV files
            dtype = dict(none.assign(file_id=0).dtypes)
            return pd.read_csv(dest, dtype=dtype)
        df = get(f'{parent}/{name}', start, qty)
        _, file_id = file_size_id(path)
        df = df.assign(file_id=file_id)
        df.to_csv(dest, index=False)
        return df

    def get_runs(self, ref, start, qty):
        df = self.get_records(ref, start, qty,
                              target='import-kernel-finish',
                              include=['import-kernel-finish'])
        df = df if len(df) > 0 else self.no_runs
        return df

    no_blocks = pd.DataFrame(dict(time=[0.0],
                                  type=[''],
                                  blockHeight=[0],
                                  blockTime=[0],
                                  line=[0])).iloc[:0]

    def provide_blocks(self, parent, name, start, qty):
        df = self.provide_data(parent, name, start, qty,
                               'blocks', self.no_blocks, self.get_blocks)
        df = df.assign(sign=np.where(
            df.type == 'cosmic-swingset-end-block-start', -1, 1))
        df = df.drop(columns=['type'])
        return df

    def get_blocks(self, ref, start, qty):
        df = self.get_records(ref, start, qty,
                              target='cosmic-swingset-end-block',
                              include=['cosmic-swingset-end-block-start',
                                       'cosmic-swingset-end-block-finish'])
        return df if len(df) > 0 else self.no_blocks

    def get_records(self, ref, start, qty,
                    target=None, include=None, exclude=[]):
        """
        :param ref: 'parent/slogname.slog.gz'
        :param start: line **1-based**
        :param qty: of lines to examine
        """
        slogdir, gztool = self.__slogdir, self.__gztool
        records = []
        error = {'time': -1, 'type': 'error'}
        loads = json.loads

        targetb = target.encode('utf-8') if target else None
        records = []
        with gztool.pipe(slogdir / ref, '-v', 0,
                         '-L', start, '-R', qty) as lines:
            # misnomer: text is actually bytes
            for offset, text in enumerate(lines):
                # log.info('line: %d %s', lo + offset, text)
                if target and targetb not in text:
                    continue
                try:
                    record = loads(text)
                except (JSONDecodeError, UnicodeDecodeError):
                    record = error
                ty = record['type']
                if ty in exclude:
                    continue
                if include and ty not in include:
                    continue
                record = dict(record, line=start + offset)
                # log.info('record: %s', record)
                records.append(record)
        return pd.DataFrame.from_records(records)

    def get_deliveries(self, ref, start, qty):
        df = self.get_records(
            ref, int(start), int(qty),
            target=None,
            include=['deliver', 'deliver-result', 'syscall-result'])
        if len(df) > 0 and 'syscallNum' in df.columns:
            df = df.drop(columns=['syscallNum', 'ksr', 'vsr', 'vd'])
            return block_cranks(df.to_dict('records'))
        else:
            return no_deliveries

    def provide_deliveries(self, blockHeight, run, blockrun):
        br = blockrun[(blockrun.run == run.name) &
                      (blockrun.blockHeight == blockHeight)]
        if len(br) < 2:
            return no_deliveries.assign(
                file_id=-1, chain=-1, blockHeight=blockHeight, run=run.name)
        block_start = br.iloc[0]  # assert sign == -1?
        block_end = br.iloc[1]
        length = int(block_end.line - block_start.line + 1)
        df = self.provide_data(run.parent, run['name'],
                               int(block_start.line), length,
                               f'deliveries-{blockHeight}', no_deliveries,
                               self.get_deliveries,
                               'gzip')
        df = df.assign(chain=run.chain, blockHeight=blockHeight, run=run.name)
        if (df.dtypes['chain'] != 'int64'
            or 'vatID' not in df.columns
            or 'vd' in df.columns):
            raise NotImplementedError(
                f'cols: {df.columns} dtypes: {df.dtypes} '
                f'block {blockHeight, int(block_start.line)}, run\n{run}')
        return df


if __name__ == '__main__':
    def _script():
        from pathlib import Path
        from subprocess import run, Popen
        from sys import argv, stderr
        from sqlalchemy import create_engine

        logging.basicConfig(
            level=logging.INFO, stream=stderr,
            format='%(asctime)s %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')

        main(argv[:],
             dirAccess=lambda d: Path(d),
             create_engine=create_engine,
             run=run, Popen=Popen)

    _script()
