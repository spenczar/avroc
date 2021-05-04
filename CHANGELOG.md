# Changelog

## unreleased

- Permit defaults for union-valued fields to match any of the union's types,
  rather than just the first type in the union. This makes avroc a little more
  permissive than the Avro specification requires, but it's pragmatically useful
  because so many schemas in the wild violate this detail of the spec.

## 0.2.2

- Fixed writing of data with a union which includes `double` as one of the
  possible values.
