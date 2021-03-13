import pytest
import io
import avroc.codegen.read
import avroc.codegen.write
import avroc.codegen.resolution


class roundtrip_testcase:
    def __init__(self, schema, records, reader_schema=None):
        self.schema = schema
        self.reader_schema = reader_schema
        self.records = records

    def assert_roundtrip(self):
        buf = io.BytesIO()

        write_func = avroc.codegen.write.WriterCompiler(self.schema).compile()
        for r in records:
            write_func(buf, r)
        buf.seek(0)
