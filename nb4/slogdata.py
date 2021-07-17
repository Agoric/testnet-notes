from contextlib import contextmanager
from json import JSONDecodeError
from subprocess import PIPE
import json


import pandas as pd


def show_times(df, cols=['time', 'blockTime']):
    return df.assign(**{col: pd.to_datetime(df[col], unit='s')
                        for col in df.columns
                        if col in cols})


class CLI:
    def __init__(self, bin, run, popen):
        self.bin = bin
        self.__run = run
        self.__popen = popen

    def run(self, *args):
        cmd = [self.bin] + [str(a) for a in args]
        return self.__run(cmd, capture_output=True)

    @contextmanager
    def pipe(self, *args):
        cmd = [self.bin] + [str(a) for a in args]
        with self.__popen(cmd, stdout=PIPE) as proc:
            yield proc.stdout


def files_by_size(files):
    return pd.DataFrame.from_records([
        dict(
            path=p,
            parent=p.parent.name,
            name=p.name,
            st_size=p.stat().st_size
        )
        for p in files
    ]).sort_values('st_size').reset_index(drop=True)


class SlogAccess:
    def __init__(self, gztool, slogdf):
        self.__gztool = gztool
        self.__slogdf = slogdf
        self.slogdf = slogdf.drop('path', axis=1)

    @classmethod
    def make(cls, slogdir, gztool):
        slogdf = files_by_size(slogdir.glob('**/*.slog.gz'))
        return cls(gztool, slogdf)

    def extract_lines(self, p, include=['import-kernel-finish',
                                        'cosmic-swingset-end-block-start',
                                        'cosmic-swingset-end-block-finish'],
                      exclude=[]):
        """
        :param p: 4-tuple (slogfile_index, start_line, line_qty, _ignored)
        note start_line is **1-based**
        """
        slogdf, gztool = self.__slogdf, self.__gztool

        records = []
        error = {'time': -1, 'type': 'error'}
        loads = json.loads
        s, lo, r, _tot = p
        # ISSUE: gztool is ambient
        with gztool.pipe(slogdf.path[s], '-L', lo, '-R', r) as lines:
            for offset, txt in enumerate(lines):
                try:
                    record = loads(txt)
                except (JSONDecodeError, UnicodeDecodeError):
                    record = error
                ty = record['type']
                if ty in exclude:
                    continue
                if include and ty not in include:
                    continue
                records.append(dict(record, slogfile=s, line=lo + offset))
        return pd.DataFrame.from_records(records)
