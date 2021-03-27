import argparse
import json
from avroc.codegen import read, write


def main():
    parser = argparse.ArgumentParser(
        description="Generate Python module for an Avro schema."
    )
    parser.add_argument(
        "schema", type=str, help="an Avro schema document to generate from"
    )
    parser.add_argument(
        "--writer", action="store_true", help="generate a writer module"
    )
    parser.add_argument(
        "--reader", action="store_true", help="generate a reader module"
    )

    args = parser.parse_args()

    if (not (args.writer or args.reader)) or (args.writer and args.reader):
        raise ValueError("exactly one of --writer or --reader must be chosen")

    dec = json.JSONDecoder()
    schema = dec.decode(args.schema)
    if args.writer:
        compiler = write.WriterCompiler(schema)
    else:
        compiler = read.ReaderCompiler(schema)

    code = compiler.generate_source_code()
    print(code)


if __name__ == "__main__":
    main()
