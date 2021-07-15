# Contributing: Design, Development notes

## Keeping .py and .ipynb in sync with jupytext

To avoid mixing the notebook output with version controlled source,
we use `jupytext` to keep the `.ipynb` in sync with a `.py` file.

To configure it:

```sh
jupyter nbextension install --py jupytext --user
jupyter nbextension enable --py jupytext --user
jupyter notebook --generate-config
```

Then append to `.jupyter/jupyter_notebook_config.py`:
```py
c.NotebookApp.contents_manager_class = "jupytext.TextFileContentsManager"
```
