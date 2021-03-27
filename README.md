# avroc #

`avroc` is a Python just-in-time compiler for reading and writing Avro data.

It aims to be:
 - Completely correct to the Avro specification.
 - Very, very fast when compiled. Our goal is to be the fastest library
   available in Python for dealing with Avro.
 - Ergonomic and simple to use, with a small API that's clear.

## Installation

Install with pip:
```
pip install avroc
```

## Basic usage

```python
import avroc

# Make up a schema
schema = {
    "type": "record",
    "name": "Weather",
    "fields": [
        {"name": "temperature", "type": "double"},
        {"name": "wind_speed", "type": "double"},
        {"name": "location", "type": "string"},
    ]
}

# Make up some records
records = [
    {"temperature": 71.2, "wind_speed": 0.5, "location": "San Diego"},
    {"temperature": 8.2, "wind_speed": 13.4, "location": "North Pole"},
    {"temperature": -66.0, "wind_speed": 14.4, "location": "Mars"},
]

# Write records to a file
with open("data.avro, "wb") as f:
    avroc.write_file(f, schema, records)

# Read records from a file
with open("data.avro", "rb") as f:
    for msg in avroc.read_file(f):
        print(f'The temperature in {msg["location"]} is {msg["temperature"]}')

# Encode a single record as raw bytes
encoder = avroc.compile_encoder(schema)
raw_bytes = encoder(records[0])

# Decode a raw bytes as a single record
decoder = avroc.compile_decoder(schema)
rec = decoder(io.BytesIO(raw_bytes))
```

For a lot more detail, see the documentation: [avroc.readthedocs.io](https://avroc.readthedocs.io/)
