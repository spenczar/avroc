# avroc #

`avroc` is a Python just-in-time compiler for reading and writing Avro data.

Benchmarks: [google sheet](https://docs.google.com/spreadsheets/d/1SBQimDUuxekJ04bfRvIvDTpOBc79YiOZJ6YN-tpdVPw)


## Misc notes on variations from the Avro spec

 - Writers use field defaults when a message does not include a given field.
 - When writing a field with a type which is a union that includes 'null', the
   writer is permissive - a "default": "null" is assumed.
