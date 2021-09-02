# pip install --user jupytext
JUPYTEXT=$(HOME)/.local/bin/jupytext

%.py: %.ipynb
	$(JUPYTEXT) --to .py $<


all: slogfile-node-task.py block-explore-uptime.py uptime-tasks.py

