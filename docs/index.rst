.. aFreight documentation master file
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

aFreight
========

Python version of Behavioral-based National Freight Demand Model.

Additional information about the aFreight development effort is on the
`GitHub project wiki <https://github.com/rsg/afreight/wiki>`__.

Software Design
---------------

aFreight is
implemented in Python, and makes heavy use of the vectorized backend C/C++ libraries in 
`pandas <http://pandas.pydata.org>`__  and `numpy <http://numpy.org>`__.  The core design 
principle of the system is vectorization of for loops, and this principle 
is woven into the system wherever reasonable.  As a result, the Python portions of the software 
can be thought of as more of an orchestrator, data processor, etc. that integrates a series of 
C/C++ vectorized data table and matrix operations.  The model system formulates 
each simulation as a series of vectorized table operations and the Python layer 
is responsible for setting up and providing expressions to operate on these large data tables.

Contents
--------

.. toctree::
   :maxdepth: 2

   gettingstarted


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
