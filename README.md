# Hacking Dask @ PyCon 2021

[![Build](https://github.com/jrbourbeau/hacking-dask/actions/workflows/build.yml/badge.svg)](https://github.com/jrbourbeau/hacking-dask/actions/workflows/build.yml)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/jrbourbeau/hacking-dask/main?urlpath=lab)

This repository contains materials for the "Hacking Dask: Diving Into Dask’s Internals" tutorial at PyCon 2021.

---

## 🚨🚨 The materials here are still being actively developed. Please check back later. 🚨🚨

---

## Outline

- **Introduction [10 minutes]**
  - Provide an overview of what will be covered in the tutorial.
  - Ensure everyone is set up with tutorial materials.

- **An overview of Dask [30 minutes]**
  - Review Dask’s delayed, array, and DataFrame interfaces.
  - Recap the components of Dask’s distributed scheduler.
  - Participants will recap the basics of how to use the high-level Dask interfaces to accomplish well supported tasks and parallelize custom algorithms.

*10 minute break*

- **Advanced Dask Collections [50 minutes]**
  - Discuss applying custom operations on Dask arrays and DataFrames.
  - Discuss Dask’s graph optimization system.
  - Review the Dask collection interface and implement our own custom collection.
  - Participants will gain a deeper insight into the task graph system underlying Dask collections.

*10 minute break*

- **Hacking the distributed scheduler [50 minutes]**
  - Highlight built in coordination primitives like `Lock`s, `Event`s, and `Semaphore`s.
  - Demonstrate how to customize worker and scheduler behavior using Dask’s plugin system.
  - Learn how to inspect the internal state of a cluster’s scheduler and workers.
  - Participants will gain a better understanding of Dask internals and how to troubleshoot common pitfalls of distributed computing.

- **Conclusion [10 minutes]**
  - Recap what we learned.
  - Provide references to links to additional community resources.
