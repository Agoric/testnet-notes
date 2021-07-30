import logging
import json
from zlib import crc32
import gzip
import datetime

from sqlalchemy.engine.url import URL
from sqlalchemy import (MetaData, Table, Column,
                        select, and_,
                        Integer, BIGINT, String)
from sqlalchemy.dialects.mysql import JSON, TINYINT, DOUBLE
# from sqlalchemy.sql import func

log = logging.getLogger(__name__)


def main(argv, cwd, create_engine):
    [fn] = argv[1:2]
    db = create_engine(dburl())
    meta = schema()
    if '--drop' in argv:
        log.warning('dropping %s all in %s', meta.tables.keys(), db.name)
        meta.drop_all(bind=db)
    meta.create_all(db)
    with db.connect() as conn:
        shred(cwd / fn, conn, meta)


def schema():
    metadata = MetaData()
    Table('file_info', metadata,
          Column('file_id', BIGINT, primary_key=True),
          Column('parent', String(64)),
          Column('name', String(128)),
          Column('st_size', BIGINT),
          Column('line_loaded', Integer),
          Column('line_count', Integer))
    Table('slog_run', metadata,
          Column('file_id', BIGINT, primary_key=True),
          Column('parent', String(64)),
          Column('name', String(128)),
          Column('line_lo', Integer, primary_key=True),
          Column('line_hi', Integer),
          Column('line_count', Integer),
          Column('blockHeight_lo', Integer),
          Column('blockHeight_hi', Integer),
          Column('blockTime_lo', Integer),
          Column('blockTime_hi', Integer))
    Table('slog_block', metadata,
          Column('file_id', BIGINT, primary_key=True),
          Column('line', Integer, primary_key=True),
          Column('run_line_lo', Integer),
          Column('blockHeight', Integer),
          Column('blockTime', Integer),
          Column('sign', TINYINT))
    Table('slog_entry', metadata,
          Column('file_id', BIGINT),
          Column('run_line_lo', Integer),
          Column('blockTime', Integer),
          Column('blockHeight', Integer),
          Column('line', Integer),
          Column('time', DOUBLE),
          Column('type', String(64)),
          Column('record', JSON))
    return metadata


def shred(src, conn, meta,
          delete=False,
          chunk_size=10000):
    st_size, file_id = file_size_id(src)
    chunk = []

    entry = meta.tables['slog_entry']
    file_info = meta.tables['file_info']
    slog_run = meta.tables['slog_run']
    slog_block = meta.tables['slog_block']

    if delete:
        done = conn.execute(entry.delete().where(entry.c.file_id == file_id))
        log.info('%s: deleted %d records', entry.name, done.rowcount)

    line_loaded = conn.execute(select(file_info.c.line_loaded).where(
        file_info.c.file_id == file_id)).scalar() or 0
    log.info('%s: %d existing records', entry.name, line_loaded)
    if not line_loaded:
        conn.execute(file_info.insert(), [dict(
            parent=src.parent.name,
            name=src.name,
            st_size=st_size,
            file_id=file_id,
            line_loaded=0)])

    run_line_lo = None
    blockHeight = None
    blockTime = None

    def add(chunk):
        log.info('%s += %d = %d from %s',
                 entry.name, len(chunk), line_loaded, src.name)
        conn.execute(entry.insert(), chunk)
        loaded = line_loaded + len(chunk)
        conn.execute(file_info.update().where(
            file_info.c.file_id == file_id).values(line_loaded=line_loaded))
        return loaded

    def add_run(line):
        log.info('%s += %s, %d', slog_run.name, src.name, line)
        conn.execute(slog_run.insert(), [dict(
            file_id=file_id,
            parent=src.parent.name,
            name=src.name,
            line_lo=line,
        )])
        return line

    def end_run(line):
        log.info('%s: %s %d - %d', slog_run.name, src.name, run_line_lo, line)
        conn.execute(slog_run.update().where(
            and_(slog_run.c.file_id == file_id,
                 slog_run.c.line_lo == run_line_lo)).values(
                     line_hi=line,
                     line_count=line - run_line_lo + 1,
                     blockHeight_hi=blockHeight,
                     blockTime_hi=blockTime,
                 ))

    def add_block_mark(line, ht, t, sign):
        if blockHeight is None:
            conn.execute(slog_run.update().where(
                and_(slog_run.c.file_id == file_id,
                     slog_run.c.line_lo == run_line_lo)).values(
                         blockHeight_lo=ht,
                         blockTime_lo=t))
        if ht % 100 == 0:
            log.info('%s += %s: %d, %s', slog_block.name, src.name,
                     ht, datetime.datetime.fromtimestamp(t))
        conn.execute(slog_block.insert(), [dict(
            file_id=file_id,
            line=line,
            run_line_lo=run_line_lo,
            blockHeight=ht,
            blockTime=t,
            sign=sign,
        )])
        return ht, t

    with gzip.open(src.open('rb')) as lines:
        for ix, line in enumerate(lines):
            if ix + 1 <= line_loaded:
                continue
            try:
                record = json.loads(line)
            except Exception as ex:
                log.warn('%s:%d: cannot parse %s', line, exc_info=ex)
            if len(chunk) >= chunk_size:
                line_loaded = add(chunk)
                chunk = []
            time = record['time']
            ty = record['type']
            if ty == 'import-kernel-start':
                if run_line_lo is not None:
                    end_run(ix - 1)
                run_line_lo = add_run(ix + 1)
                blockHeight = None
                blockTime = None
            elif ty in ['cosmic-swingset-end-block-start',
                        'cosmic-swingset-end-block-finish']:
                sign = -1 if ty == 'cosmic-swingset-end-block-start' else 1
                blockHeight, blockTime = add_block_mark(
                    ix + 1, record['blockHeight'], record['blockTime'], sign)
            chunk.append(dict(file_id=file_id,
                              line=ix + 1,
                              time=time,
                              type=ty,
                              run_line_lo=run_line_lo,
                              blockHeight=blockHeight,
                              blockTime=blockTime,
                              record=record))
    if chunk:
        line_loaded = add(chunk)
    end_run(line_loaded)
    conn.execute(file_info.update().where(
        file_info.c.file_id == file_id).values(line_count=line_loaded))


def dburl(database='slog4',
          socket='/var/run/mysqld/mysqld.sock'):
    url = URL.create(
        drivername='mysql+pymysql',
        host=None,
        username=None,
        password=None,
        database=database,
        query={
            'unix_socket': socket,
            'charset': 'utf8mb4',
        }
    )
    return url


def file_size_id(p):
    """file_id index a stable function of each file
    (relative pathname and file size)
    """
    st_size = p.stat().st_size
    refb = (f'{p.parent.name}/{p.name}').encode()
    return st_size, crc32(refb) * 1000000 + st_size


if __name__ == '__main__':
    def _script():
        from sys import argv, stderr
        from pathlib import Path
        from sqlalchemy import create_engine

        logging.basicConfig(
            level=logging.INFO, stream=stderr,
            format='%(asctime)s %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')

        main(argv[:], cwd=Path('.'), create_engine=create_engine)

    _script()
