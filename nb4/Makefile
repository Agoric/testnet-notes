# pip install --user jupytext
JUPYTEXT=$(HOME)/.local/bin/jupytext

%.py: %.ipynb
	$(JUPYTEXT) --to .py $<

slogfile-node-task.py: slogfile-node-task.ipynb

