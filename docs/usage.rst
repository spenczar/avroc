=======
 Usage
=======

explain usage here


Types
=====

Basic Types
-----------

Each of the Avro types is mapped to and from Python types according to this table:

+--------------------+--------------+
|         Avro Type  |Python Type   |
+==========+=========+==============+
|          |null     |None          |
|          +---------+--------------+
|primitive |int      |int           |
|          +---------+--------------+
|          |long     |int           |
|          +---------+--------------+
|          |boolean  |bool          |
|          +---------+--------------+
|          |float    |float         |
|          +---------+--------------+
|          |double   |float         |
|          +---------+--------------+
|          |string   |string        |
|          +---------+--------------+
|          |bytes    |bytes         |
+----------+---------+--------------+
|          |map      |dict          |
|          +---------+--------------+
|          |array    |list          |
|          +---------+--------------+
| complex  |record   |dict          |
|          +---------+--------------+
|          |fixed    |bytes         |
|          +---------+--------------+
|          |enum     |string        |
+----------+---------+--------------+

Unions
------

Unions are implemented transparently: when you're reading union-typed Avro data,
you'll just get the actual concretely typed value that was stored. To put it
another way, you won't explicitly know which branch of the union was stored, but
it shouldn't matter.

When you're writing a message with a union-typed schema, avroc will attempt to
infer the type to use. It does this greedily: it will encode the data with the
**first** schema in the union that appears to be "valid."

Validity is checked using the code found in the :py:mod:`avroc.runtime` module.

This can be easier to understand by looking at some of the generated code for
unions. Let's take a very simple record schema with just one field: a union of
"int", "float", and "string":

.. code-block:: json
   :name: schema.avsc

   {
     "name": "ExampleRecord",
     "type": "record",
     "fields": [
       {
         "type": ["int", "float", "string"],
         "name": "example_union_field",
       },
     ]
   }

The reader will produce a dictionary with one set key, `example_union_field`. It
will hold either an int, a float, or a string, depending on the bytes being
read. Here's what the generated code looks like:

.. code-block:: py
   :name: generated_reader.py

   import datetime
   import decimal
   import uuid
   from avroc.runtime.encoding import *
   from avroc.runtime.blocks import decode_block

   def decoder(src):
       ExampleRecord = {}
       union_choice = decode_long(src)
       if union_choice == 0:
           ExampleRecord['example_union_field'] = decode_int(src)
       elif union_choice == 1:
           ExampleRecord['example_union_field'] = decode_float(src)
       elif union_choice == 2:
           ExampleRecord['example_union_field'] = decode_string(src)
       result = ExampleRecord
       return result

And the writer will take in a dictionary, and decide how to encode based on type
tests. The ``writer`` function here expects a ``msg`` shapedc like
``{"example_union_field": 8}``.

.. code-block:: py
   :name: generated_writer.py

   import numbers
   from avroc.runtime.encoding import *
   from avroc.runtime.typetest import *

   def writer(msg):
       buf = bytes()
       if is_int(msg['example_union_field']):
           buf += encode_long(0)
           buf += encode_int(msg['example_union_field'])
       elif is_float(msg['example_union_field']):
           buf += encode_long(1)
           buf += encode_float(msg['example_union_field'])
       elif is_string(msg['example_union_field']):
           buf += encode_long(2)
           buf += encode_string(msg['example_union_field'])
       else:
           raise ValueError("message type doesn't match any options in the union")
       return buf

These cases are relatively straightforward. But type matching can be more
complicated for record types. If multiple record types are possible in a union,
the Avro specification leaves it up to the implementation to decide what to do.

Avroc decides to pick the **first** record type with field names that match the
dictionary keys for the input record, in this case. Another example may be
useful. Here's a schema which represents a union over three possible record
types:

.. code-block:: json

   [
        {
            "type": "record",
            "name": "CelsiusTemperature",
            "fields": [
                {"name": "temperature", "type": "double"},
                {"name": "measurement_error", "type": "double"}
            ]
        },
        {
            "type": "record",
            "name": "WindSpeed",
            "fields": [
                {"name": "speed", "type": "double"},
                {"name": "measurement_error", "type": "double"}
            ]
        },
        {
            "type": "record",
            "name": "FahrenheitTemperature",
            "fields": [
                {"name": "temperature", "type": "double"},
                {"name": "measurement_error", "type": "double"}
            ]
        }
    ]

Here's the generated writer code:

.. code-block:: py

 import numbers
 from avroc.runtime.encoding import *
 from avroc.runtime.typetest import *

 def writer(msg):
     buf = bytes()
     if is_record(msg, {'temperature', 'measurement_error'}):
         buf += encode_long(0)
         buf += encode_double(msg['temperature'])
         buf += encode_double(msg['measurement_error'])
     elif is_record(msg, {'speed', 'measurement_error'}):
         buf += encode_long(1)
         buf += encode_double(msg['speed'])
         buf += encode_double(msg['measurement_error'])
     elif is_record(msg, {'temperature', 'measurement_error'}):
         buf += encode_long(2)
         buf += encode_double(msg['temperature'])
         buf += encode_double(msg['measurement_error'])
     else:
         raise ValueError("message type doesn't match any options in the union")
     return buf


Using that code, any of the following are valid:

.. code-block:: py

   # Write a Celsius temperature measurement:
   writer({"temperature": 21.5, "measurement_error": 0.4})

   # Write a Windspeed measurement:
   writer({"speed": 3.21, "measurement_error": 0.04})

   # Write a Fahrenheit measurement - BUT this actually writes as "CelsiusTemperature"
   writer({"temperatuire": 73.2, "measurement_error": 2.1})

Note that, since the ``CelsiusTemperature`` and the ``FahrenheitTemperature``
record types in the schema have exactly the same field names, the writer can't
tell which one is intended. In this case, it just takes the first one which
matches.

If you need to disambiguate in cases like this, you might want to either add a
field name to act as a flag, or store an additional ``enum``-typed value to help
out.

Logical Types
-------------

Avro supports `"logical types." <https://avro.apache.org/docs/current/spec.html#Logical+Types>`_ These are annotations on types which indicate the
semantic intent of a field. Avroc uses ``logicalType`` annotations to encode and
decode values into certain types provided by the Python standard library.
Specifically:

+----------------------------------------+----------------------------------------+
| logicalType                            | Python type                            |
+========================================+========================================+
| "decimal"                              | decimal.Decimal                        |
+----------------------------------------+----------------------------------------+
| "uuid"                                 | uuid.UUID                              |
+----------------------------------------+----------------------------------------+
| "date"                                 | datetime.Date                          |
+----------------------------------------+----------------------------------------+
| "time-millis", "time-micros"           | datetime.time                          |
+----------------------------------------+----------------------------------------+
| "timestamp-millis", "timestamp-micros" | datetime.datetime                      |
+----------------------------------------+----------------------------------------+

If a ``logicalType`` is not recognized, or its arguments are invalid, then it
will be encoded or decoded as the underlying type.
