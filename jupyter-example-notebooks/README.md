
# Setup Jupyter
* Check out this repo and cd to jupyter-example-notebooks folder.
* You can install jupyter using conda. First install [conda](https://conda.io/projects/conda/en/latest/user-guide/install/index.html)
* Install jupyter in a conda enviroment, easiest way is to get enviroment file in this repo and run below command
`conda env create -f environment-jupyter.yml`. This will create a conda environment name jupyter3.6, you can see it listed by 
running `conda env list`
* Activate conda environment `source activate jupyter3.6`
* Other option if you have docker is to get this [docker image](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-scipy-notebook) and run it and upload the notebook. (you probably want to mount folder of jupyter-example-notebooks so that you do not lose changes)
* if your conda enviroment does not have jupyter installed already use `pip install jupyter` to install it.
* Run jupyter notebook by running `jupyter notebook`
* It will launch browser with current directy listing, select ExplanationConfidenceLRSExample.ipynb notebook and launch it.
* Specify api_host, account_id and token
* Run through each cell to see the output.

