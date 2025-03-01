# Choose your desired base image
FROM jupyter/pyspark-notebook:latest

# name your environment and choose the python version
ARG conda_env=pyspark_env
ARG py_ver=3.10

# os
USER root
COPY .devcontainer/workspace/packages.txt /tmp/os-tmp/
RUN apt update -y && apt upgrade -y
RUN xargs apt-get -y install < /tmp/os-tmp/packages.txt && rm -rf /tmp/os-tmp
USER ${NB_UID}

# you can add additional libraries you want mamba to install by listing them below the first line and ending with "&& \"
RUN mamba create --yes -p "${CONDA_DIR}/envs/${conda_env}" python=${py_ver} ipython ipykernel && \
    mamba clean --all -f -y

# COPY --chown=${NB_UID}:${NB_GID} source dest


# create Python kernel and link it to jupyter
RUN "${CONDA_DIR}/envs/${conda_env}/bin/python" -m ipykernel install --user --name="${conda_env}" && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

# any additional pip installs can be added by uncommenting the following line
COPY --chown=${NB_UID}:${NB_GID} requirements.txt /tmp/pip-tmp/
RUN "${CONDA_DIR}/envs/${conda_env}/bin/pip" install --no-cache-dir -r /tmp/pip-tmp/requirements.txt && rm -rf /tmp/pip-tmp

# if you want this environment to be the default one, uncomment the following line:
RUN echo "conda activate ${conda_env}" >> "${HOME}/.bashrc"