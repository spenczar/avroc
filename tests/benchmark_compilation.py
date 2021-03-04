import timeit
import io
import json
import sys
from fastavro import schemaless_reader, schemaless_writer
from avro.schema import parse
from avro.io import DatumReader, DatumWriter, BinaryDecoder, BinaryEncoder
from avroc.codegen.read import ReaderCompiler
from avroc.codegen.write import WriterCompiler
from tests.test_compiled import testcases

machine_output = True


def main():
    if machine_output:
        print(
            "\t".join(
                [
                    "name",
                    "avro_read",
                    "fastavro_read",
                    "avroc_read",
                    "avro_write",
                    "fastavro_write",
                    "avroc_write",
                ]
            )
        )
    for tc in testcases:
        compare(tc.messages[0], tc.schema, tc.label)


def prepare_read_buffer(message, schema):
    buf = io.BytesIO()
    schemaless_writer(buf, schema, message)
    buf.seek(0)
    return buf


def compare(message, schema, name):
    if not machine_output:
        print(f"benchmarking '{name}'", file=sys.stderr)
    buf = prepare_read_buffer(message, schema)

    def read_schemaless():
        buf.seek(0)
        return schemaless_reader(buf, schema)

    compiled_reader = ReaderCompiler(schema).compile()

    def read_compiled():
        buf.seek(0)
        return compiled_reader(buf)

    avro_schema = parse(json.dumps(schema))
    avro_reader = DatumReader(avro_schema)
    avro_decoder = BinaryDecoder(buf)

    def read_avro():
        buf.seek(0)
        return avro_reader.read(avro_decoder)

    assert read_schemaless() == read_compiled()

    def write_schemaless():
        buf.seek(0)
        schemaless_writer(buf, schema, message)

    compiled_writer = WriterCompiler(schema).compile()

    def write_compiled():
        buf.seek(0)
        encoded = compiled_writer(message)
        buf.write(encoded)

    avro_writer = DatumWriter(avro_schema)
    avro_encoder = BinaryEncoder(buf)

    def write_avro():
        if "logical" in name or "illegal" in name:
            return
        buf.seek(0)
        avro_writer.write(message, avro_encoder)

    t = timeit.Timer(stmt="read_compiled()", globals=locals())
    avroc_read = time_and_print(t, "read_compiled")
    t = timeit.Timer(stmt="read_schemaless()", globals=locals())
    fastavro_read = time_and_print(t, "read_schemaless")
    t = timeit.Timer(stmt="read_avro()", globals=locals())
    avro_read = time_and_print(t, "read_avro")

    t = timeit.Timer(stmt="write_compiled()", globals=locals())
    avroc_write = time_and_print(t, "write_compiled")
    t = timeit.Timer(stmt="write_schemaless()", globals=locals())
    fastavro_write = time_and_print(t, "write_schemaless")
    t = timeit.Timer(stmt="write_avro()", globals=locals())
    avro_write = time_and_print(t, "write_avro")

    if machine_output:
        output(
            name,
            [
                avro_read,
                fastavro_read,
                avroc_read,
                avro_write,
                fastavro_write,
                avroc_write,
            ],
        )


def format_time(dt):
    units = {"nsec": 1e-9, "usec": 1e-6, "msec": 1e-3, "sec": 1.0}
    scales = [(scale, unit) for unit, scale in units.items()]
    scales.sort(reverse=True)
    for scale, unit in scales:
        if dt >= scale:
            break

    return "%.*g %s" % (3, dt / scale, unit)


def time_and_print(timer, label):
    n = 1000
    timings = [dt / n for dt in timer.repeat(repeat=3, number=n)]
    best = min(timings)
    print(
        f"\t{label}:  {n} iterations, best of 7: {format_time(best)} / iteration",
        file=sys.stderr,
    )
    return best


def output(name, timings):
    print(name + "\t" + "\t".join(f"{f*100000:.2f}" for f in timings))


if __name__ == "__main__":
    main()
