from contextlib import contextmanager
from json import JSONDecodeError
from subprocess import PIPE
from zlib import crc32
import json
import logging

import pandas as pd

log = logging.getLogger(__name__)


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
        path = self.__slogdir / parent / name
        dest = self.__slogdir / parent / f'{path.stem}-{start}-runs.csv'
        if dest.exists():
            # dtype is important for empty CSV files
            dtype = dict(self.no_runs.assign(file_id=0).dtypes)
            return pd.read_csv(dest, dtype=dtype)
        df = self.get_runs(f'{parent}/{name}', start, qty)
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
