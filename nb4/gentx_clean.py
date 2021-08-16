#!/usr/bin/python3
"""cleaning gentx data
"""

import json
import logging

# ## Preface: python data tools
import pandas as pd

log = logging.getLogger(__name__)


def main(argv, cwd):
    log.info('versions: %s', dict(pandas=pd.__version__))
    [portal_export] = argv[1:2]

    # ## Sumitted Tasks
    #
    # exported from the portal

    task_export = pd.read_csv(cwd / portal_export,
                              parse_dates=['Last Date Updated'])

    log.info('portal update: %s', task_export.dtypes)

    # one completed task per participant
    tasks = dedup(
        task_export[task_export.Status == 'Completed'].set_index('TaskBoardID')
    )

    log.info('tasks: %s',
             (dict(submissions_all=len(task_export), deduped=len(tasks),
                   dups=len(task_export) - len(tasks))))
    log.info('tasks:\n%s',
             tasks.drop(['Status', 'Verified', 'Task Type', 'Event',
                         'Submission Link'], axis=1).head())

    # ## Clean up markup
    #
    # The portal exports with newline as `<br>`.
    log.info(
        tasks[tasks['Submission Link'].str.contains('<br />')
              .fillna(False)][['Submission Link']].head(8))


# +
def dedup(df,
          key='Discord ID',
          boring=['Verified', 'Task Type', 'Event', 'Submission Link']):
    """one per participant"""

    dups = df[df.duplicated([key], keep='last')].reset_index(drop=True).drop(
        ['Verified', 'Task Type', 'Event', 'Submission Link'], axis=1)
    log.info('dups by %s:\n%s', key, dups)

    return df.drop_duplicates([key], keep='last')


# +
def nobr(data):
    return data.str.replace('<br />', '')


def tryjson(txt):
    try:
        return json.loads(txt)
    except Exception as ex:
        return ex

def _x():
    tasks['gentx'] = nobr(tasks['Submission Link']).apply(lambda txt: tryjson(txt))
    tasks['jsonErr'] = tasks.gentx.apply(lambda v: isinstance(v, Exception))
    tasks[['jsonErr', 'gentx', 'Submission Link']][tasks.jsonErr]
    # -

    # ### Irreparable submissions

    # +
    tasks.loc[tasks.index.isin(links.index), 'gentx'] = links.gentx
    tasks.loc[tasks.index.isin(links.index), 'jsonErr'] = False

    tasks[['jsonErr', 'gentx', 'Submission Link']][tasks.jsonErr]
    # -

    # ## TODO: prune duplicates

    # Duplicates are all fixing erros except `null#2319`, whose entries are all the same:

    tasks[~tasks.jsonErr].loc[tasks[tasks.index.duplicated()].index.unique()]

    len(set(json.dumps(v) for v in tasks.loc['null#2319'].gentx.values))

    # ## cleaned data

    ok = tasks[~tasks.jsonErr]
    ok = ok[~ok.index.duplicated()].copy()
    ok

    # ## No duplicate Monikers

    (ok.Moniker.value_counts() > 1).any()

    # ## No gentx with >50 BLD

    ok['amount'] = ok.gentx.apply(lambda g: g['body']['messages'][0]['value']['amount']).astype('float') / 1000000.0
    ok.amount.max()

    ok.amount.describe()

    # ## Results

    len(ok)

    alljson = json.dumps([tx for tx in ok.gentx.values], indent=2)
    (_home() / 'Desktop' / 'genesis.json').open('w').write(alljson)

    # ## separate files

    for ix, info in ok[['gentx']].reset_index().iterrows():
        path = _home() / 'Desktop' / 'gentx3' / f'gentx{ix}.json'
        json.dump(info.gentx, path.open('w'))

    # ## duplicate pubkeys

    filestuff = [json.load(p.open()) for p in (_home() / 'Desktop' / 'gentx3').iterdir()]
    len(gentxs)

    df = pd.DataFrame(pd.Series(filestuff), columns=['gentx'])
    df['pubkey'] = df.gentx.apply(lambda g: g['body']['messages'][0]['pubkey']['key'])
    df['moniker'] = df.gentx.apply(lambda g: g['body']['messages'][0]['description']['moniker'])
    df = df.set_index('pubkey')
    df.head()

    df.loc[df.index.duplicated()]

    df['rate'] = df.gentx.apply(lambda g: g['body']['messages'][0]['commission']['rate'])
    df['max_rate'] = df.gentx.apply(lambda g: g['body']['messages'][0]['commission']['max_rate'])
    df[['moniker', 'rate', 'max_rate']]

    df[['moniker', 'rate', 'max_rate']][df.max_rate <= df.rate]


if __name__ == '__main__':
    def _script():
        from sys import argv, stderr
        from pathlib import Path

        logging.basicConfig(
            level=logging.INFO, stream=stderr,
            format='%(asctime)s %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')

        main(argv[:], cwd=Path('.'))

    _script()
