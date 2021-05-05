# Hacking Dask: Diving into Dask's Internals

[![Build](https://github.com/jrbourbeau/hacking-dask/actions/workflows/build.yml/badge.svg)](https://github.com/jrbourbeau/hacking-dask/actions/workflows/build.yml)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/jrbourbeau/hacking-dask/main?urlpath=lab)

## 🚨🚨 The materials here are still being actively developed. Please check back later. 🚨🚨

This repository contains materials for the "Hacking Dask: Diving Into Dask’s Internals" tutorial at PyCon 2021.

## Running the tutorial

There are two different ways in which you can set up and go through the tutorial materials. Both of which are outlined in the table below.

|     Method    | Setup | Description |
| :-----------: | :-----------: | ----------- |
| Binder        | [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/jrbourbeau/hacking-dask/main?urlpath=lab)         | Run the tutorial notebooks on mybinder.org without installing anything locally.       |
| Local install | [Instructions](#Local-installation-instructions)          | Download the tutorial notebooks and install the necessary packages (via `conda`) locally. Setting things up locally can take a few minutes, so we recommend going through the installation steps prior to the tutorial.    |


## Local installation instructions

### 1. Clone the repository

First clone this repository to your local machine via:

```
git clone https://github.com/jrbourbeau/hacking-dask
```

### 2. Download conda (if you haven't already)

If you do not already have the conda package manager installed, please follow the instructions [here](https://docs.conda.io/en/latest/miniconda.html). 

### 3. Create a conda environment

Navigate to the `hacking-dask/` directory and create a new conda environment with the required
packages via:

```terminal
cd hacking-dask
conda env create --file binder/environment.yml
```

This will create a new conda environment named "hacking-dask".

### 4. Activate the environment

Next, activate the environment:

```
conda activate hacking-dask
```

### 5. Launch JupyterLab

Finally, launch JupyterLab with:

```
jupyter lab
```