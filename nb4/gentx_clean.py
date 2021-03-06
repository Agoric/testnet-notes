#!/usr/bin/python3
"""cleaning gentx data
"""

import json
import logging

# ## Preface: python data tools
import pandas as pd

log = logging.getLogger(__name__)
BORING = ['Verified', 'Task Type', 'Event', 'Submission Link']


def main(argv, stdout, cwd):
    log.info('versions: %s', dict(pandas=pd.__version__))
    [portal_export, dest] = argv[1:3]

    tasks = load(cwd / portal_export)
    tasks = extract(tasks)
    save(tasks, cwd / dest, stdout)


def load(path):
    # ## Sumitted Tasks
    #
    # exported from the portal

    task_export = pd.read_csv(path,
                              parse_dates=['Last Date Updated'])
    task_export = task_export[
        task_export.Task == 'Create and submit gentx - Metering ']

    # log.info('portal update: %s', task_export.dtypes)

    # one completed task per participant
    tasks = mark_dups(task_export.set_index('TaskBoardID'))

    log.info('tasks:\n%s',
             tasks.drop(['Status', 'Verified', 'Task Type', 'Event',
                         'Submission Link'], axis=1).sort_values(
                             'Last Date Updated').tail())

    # ## Clean up markup
    #
    # The portal exports with newline as `<br>`.
    # log.info(
    #     tasks[tasks['Submission Link'].str.contains('<br />')
    #           .fillna(False)][['Submission Link']].head(8))
    return tasks


# +
def mark_dups(df,
              key='Discord ID'):
    """one per participant"""

    df = df.sort_values([key, 'Last Date Updated'])
    dupd = df.duplicated([key], keep='last')
    df.loc[dupd, 'Status'] = 'Obsolete'
    dups = df[df.Status == 'Obsolete'].reset_index().drop(
        BORING, axis=1)

    log.warning('dropping dups by %s:\n%s', key,
                dups[['TaskBoardID', 'Discord ID', 'Moniker']])

    log.info('tasks: %s',
             (dict(submissions_all=len(df), deduped=len(df) - len(dups),
                   dups=len(dups))))

    return df


# +
def nobr(data):
    return data.str.replace('<br />', '')


def tryjson(txt):
    try:
        return json.loads(txt)
    except Exception as ex:
        return ex


def extract(tasks):
    tasks['gentx'] = nobr(tasks['Submission Link']).apply(
        lambda txt: tryjson(txt))
    tasks['jsonErr'] = tasks.gentx.apply(lambda v: isinstance(v, Exception))
    # print(json.dumps(tasks.gentx.iloc[0], indent=2))
    tasks['moniker'] = tasks.gentx.apply(
        lambda v: None if isinstance(v, Exception)
        else v['body']['messages'][0]['description']['moniker'])
    tasks['delegator_address'] = tasks.gentx.apply(
        lambda v: None if isinstance(v, Exception)
        else v['body']['messages'][0]['delegator_address'])

    log.warning('JSON errors:\n%s',
                tasks[['Discord ID', 'Moniker', 'jsonErr']][tasks.jsonErr])

    dup_moniker = tasks[tasks.Status == 'Completed'].sort_values('Moniker')
    dup_moniker = dup_moniker[dup_moniker.duplicated('Moniker')]
    dup_moniker = dup_moniker.drop(columns=BORING)
    if len(dup_moniker):
        log.warning('Duplicate Monikers?\n%s', dup_moniker)

    # ## No gentx with >50 BLD

    tasks['amount'] = tasks[~tasks.jsonErr].gentx.apply(
        lambda g: g['body']['messages'][0]['value']['amount']
    ).astype('float') / 1000000.0

    over50 = tasks[tasks.amount > 50]
    if len(over50):
        log.warning('No gentx with >50 BLD\n%s', over50)

    return tasks


def save(tasks, dest, stdout):
    # alljson = json.dumps([tx for tx in tasks.gentx.values], indent=2)
    # (_home() / 'Desktop' / 'genesis.json').open('w').write(alljson)

    dest.mkdir(parents=True, exist_ok=True)

    # ## separate files

    ok = tasks[(tasks.Status == 'Completed') &
               ~tasks.jsonErr]
    for ix, info in ok[['gentx']].reset_index().iterrows():
        path = dest / f'gentx{ix}.json'
        json.dump(info.gentx, path.open('w'))

    tasks = tasks.sort_values(['Discord ID', 'Last Date Updated'])
    tasks[['Discord ID', 'Moniker', 'Status', 'jsonErr',
           'Last Date Updated', 'moniker', 'delegator_address']].reset_index().to_csv(stdout)


def _more_checks():
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
        from sys import argv, stdout, stderr
        from pathlib import Path

        logging.basicConfig(
            level=logging.INFO, stream=stderr,
            format='%(asctime)s %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')

        main(argv[:], stdout, cwd=Path('.'))

    _script()
