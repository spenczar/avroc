===
API
===

.. py:module:: avroc
   :synopsis: The main entrypoint

.. py:function:: message_encoder(schema)

   Construct a callable which encodes Python objects to bytes according to an
   Avro schema.

   :param schema: The schema to use when encoding data. Usually, this is a ``dict``.
   :type schema: dict (see :ref:`schema-types`)

.. py:module:: avroc.runtime
   :synopsis: Code called at runtime during avroc's reading and writing.

.. _schema-types:

Schemas
-------

The schemas passed in to ``avroc`` APIs are the plain old dictionaries (or
strings or lists) you'd get from JSON-decoding an Avro Schema. For example, this is a record schema:

.. code-block:: python

   schema = {
       "type": "record",
       "name": "WeatherData",
       "fields": [
           {"name": "temperature", "type": "float"},
           {"name": "location", "type": {
               "type": "record",
               "name": "Location",
               "fields": [
                   {"name": "latitude", "type": "float"},
                   {"name": "longitude", "type": "float"},
               ]
           }},

       ]
   }

That schema can be compiled by ``avroc``. The associated messages that ``avroc``
expects when writing, and that it will output when reading, will be dictionaries
of a similar shape:

.. code-block:: python

   msg = {
       "temperature": 71.4,
       "location": {
           "latitude": 40.213,
           "longitude": 45.231,
       },
   }
