# see https://pydoit.org/

from pathlib import Path  # WARNING: ambient
import re
import itertools

from doit import create_after

LIST = 'slogwk/slogfile-upload-5.ls'
SLOG5 = 'gs://slogfile-upload-5/'

def task_list_slogfiles():
    return {
        'actions': [f'gsutil ls gs://slogfile-upload-5/ >{LIST}'],
        'targets': [LIST],
        'uptodate': [True],
        'clean': True,
    }


def slogfiles(cwd):
    for line in itertools.islice((cwd / LIST).open(), 0, 3):
        url = line.strip()
        if 'Z-' not in url:
            continue
        fn = url.replace(SLOG5, '')
        [fresh, _d] = fn.split('Z-', 1)
        yield url, fn, fresh


@create_after(executed='list_slogfiles', target_regex='slogwk/.*\.json')
def task_extract_blocks():
    cwd = Path('.')
    slogwk = cwd / 'slogwk'

    for url, fn, fresh in slogfiles(cwd):
        data = slogwk / f'{fresh}-blocks.json'

        def check_json(data):
            line1 = data.open().readline()
            if not line1.startswith('{'):
                raise ValueError(line1)

        split_records = r"'s/}{/}\n{/'"
        yield {
            'name': str(data),
            'actions': [
                f'gsutil cat "{url}" | zcat | sed {split_records} | egrep \'"type":"cosmic-swingset-(begin|end|bootstrap)|import-kernel\' >{data}',
                (check_json, [data]),
            ],
            'targets': [data],
            'uptodate': [True],
            'clean': True,
        }


@create_after(executed='extract_blocks', target_regex='slogwk/.*\.table')
def task_load_blocks():
    cwd = Path('.')
    slogwk = cwd / 'slogwk'

    for url, fn, fresh in slogfiles(cwd):
        data = slogwk / f'{fresh}-blocks.json'
        marker = slogwk / f'{fresh}-blocks.table'

        table = 'blocks_' + re.sub(r'[^A-Z0-9]', '_', fresh)
        sql = [
            f'''CREATE TABLE IF NOT EXISTS slog45.slog_blocks AS
            SELECT current_timestamp() fresh, t.* FROM slog45.{table} t WHERE 1 = 0''',
            f'''insert into slog45.slog_blocks
            select timestamp(\'{fresh}\') fresh, t.* from slog45.{table} t''',
            f'drop table slog45.{table}'
        ]
        script = slogwk / f'load_blocks_{fresh}.sql'
        script.open('w').write(';\n'.join(sql))

        yield {
            'name': str(script),
            'file_dep': [data],
            'actions': [
                f'bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect slog45.{table} {data}',
                f'bq query --nouse_legacy_sql <{script}',
                f'touch {marker}',
            ],
            'verbosity': 2,
            'clean': True,
        }
