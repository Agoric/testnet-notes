import logging
import json
from zlib import crc32
import gzip

from sqlalchemy.engine.url import URL
from sqlalchemy import Table, Column, Integer, BIGINT, Numeric, String, MetaData
from sqlalchemy.dialects.mysql import JSON

log = logging.getLogger(__name__)


def main(argv, cwd, create_engine):
    [fn] = argv[1:2]
    db = create_engine(dburl())
    meta, entry = schema()
    if '--drop' in argv:
        entry.drop(bind=db)
    meta.create_all(db)
    with db.connect() as conn:
        shred(cwd / fn, conn, entry)


def schema():
    metadata = MetaData()
    entry = Table('slog_entry', metadata,
                  Column('file_id', BIGINT),
                  Column('line', Integer),
                  Column('time', Numeric),
                  Column('type', String(64)),
                  Column('record', JSON))
    return metadata, entry


def shred(src, conn, entry,
          chunk_size=100):
    _size, file_id = file_size_id(src)
    chunk = []

    done = conn.execute(entry.delete().where(entry.c.file_id == file_id))
    log.info('%s: deleted %d records', entry.name, done.rowcount)

    def add(chunk):
        log.info('%s += %d records', entry.name, len(chunk))
        conn.execute(entry.insert(), chunk)

    with gzip.open(src.open('rb')) as lines:
        for ix, line in enumerate(lines):
            try:
                record = json.loads(line)
            except Exception as ex:
                log.warn('%s:%d: cannot parse %s', line, exc_info=ex)
            time = record['time']
            ty = record['type']
            if len(chunk) >= chunk_size:
                add(chunk)
                chunk = []
            chunk.append(dict(file_id=file_id,
                              line=ix,
                              time=time,
                              type=ty,
                              record=record))
    if chunk:
        add(chunk)


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
