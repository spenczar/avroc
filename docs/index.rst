.. avroc documentation master file, created by
   sphinx-quickstart on Thu Mar 18 12:37:20 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

avroc: Avro schemas, compiled at runtime
========================================

Avroc is a library for reading and writing Avro data. It's unusual because it
compiles Avro schemas into Python code during runtime. You pay an upfront cost
once to compile a schema, but then encoding and decoding data using that schema
is extremely efficient.


.. toctree::
   :maxdepth: 3
   :caption: Contents:

   usage
   api



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
