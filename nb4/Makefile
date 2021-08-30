# pip install --user jupytext
JUPYTEXT=$(HOME)/.local/bin/jupytext

%.py: %.ipynb
	$(JUPYTEXT) --to .py $<


all: slogfile-node-task.py block-explore-uptime.py

slogfile-node-task.py: slogfile-node-task.ipynb


block-explore-uptime.py: block-explore-uptime.ipynb
