# pip install --user jupytext
JUPYTEXT=$(HOME)/.local/bin/jupytext

%.py: %.ipynb
	$(JUPYTEXT) --to .py $<


all: slogfile-node-task.py block-explore-uptime.py uptime-tasks.py

# Cloud Storage Bucket
CSB=slogfile-upload-5

# largest slogfile that doesn't exceed the 4GB bigquery load limit
SLOG0=gs://slogfile-upload-5/2021-08-27T12:00:56.385Z-mirxl\#0530.slog.gz

slogwk:
	mkdir -p $@

slogwk/slog0-head.slog: slogwk
	gsutil cat -r 0-10000000 "$(SLOG0)" | zcat >$@

slogwk/slog0-shared.slog.gz: check-shared.jq
	gsutil cat "$(SLOG0)" | pv -s 3925214742 | zcat | jq -c -f check-shared.jq | gzip >$@

# btsniik 2308177920 bytes
SLOG1=gs://slogfile-upload-5/2021-08-26T18:22:25.567Z-Vitalii\#9679.slog.gz

slogwk/slog1-shared.slog.gz: check-shared.jq
	gsutil cat "$(SLOG1)" | pv -s 2308177920 | zcat | jq -c -f check-shared.jq | gzip >$@

slogwk/slog01.diff: slogwk/slog0-shared.slog.gz slogwk/slog1-shared.slog.gz
	zdiff -u slogwk/slog0-shared.slog.gz slogwk/slog1-shared.slog.gz >$@

