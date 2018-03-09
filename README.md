rFirm
=====

rFirm does firm synthesis and is partially implemented as a series of R scripts and as a Python package, 
using the [ActivitySim](https://github.com/activitysim) framework.  The plan is for rFirm to be 
eventually entirely implemented as a standalone Python package.

## Running the program
  - Run the R programs via ``create_inputs/NFM_MainSynthesis.R``
  - Copy ``outputs/cbp.establishments_final.csv`` to ``example/data/firms.csv``
  - Ensure ``firms_file_name`` in ``example\configs\settings.yaml`` references ``example/data/firms.csv``
  - Run the Python program via ``example/python run_simulation.py``
  - Running in debug / test mode (since the final R script takes days to run)
    - set ``DEBUG <- TRUE`` in ``NFM_MainSynthesis.R``
    - Run the R programs via ``create_inputs/NFM_MainSynthesis.R``
    - Copy ``outputs/firms_test.csv`` to ``example/data/firms_test.csv``
    - Ensure ``firms_file_name`` in ``example\configs\settings.yaml`` references ``example/data/firms_test.csv``
    - Run the Python program via ``example/python run_simulation.py``

## Python package

[![Build Status](https://travis-ci.org/RSGInc/rFirm.svg?branch=master)](https://travis-ci.org/RSGInc/rFirm) [![Coverage Status](https://coveralls.io/repos/RSGInc/rFirm/badge.png?branch=master)](https://coveralls.io/r/RSGInc/rFirm?branch=master)

## Documentation

https://rsginc.github.io/rFirm/
