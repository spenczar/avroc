import pytest
import glob
import os.path
import io

import avroc.files

data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "testdata")

NO_DATA = {
    "class org.apache.avro.tool.TestDataFileTools.zerojsonvalues.avro",
    "testDataFileMeta.avro",
}

def _test_files():
    for filename in glob.iglob(os.path.join(data_dir, "*.avro")):
        yield filename

@pytest.mark.parametrize("filename", _test_files())
def test_file(filename):
    # Read from disk
    with open(filename, "rb") as fo:
        reader = avroc.files.AvroFileReader(fo)
        records = [r for r in reader]
        if os.path.basename(filename) in NO_DATA:
            return

        assert len(records) > 0, "no records found"

    # Rewrite
    write_buf = io.BytesIO()
    writer = avroc.files.AvroFileWriter(write_buf, reader.writer_schema, reader.codec)
    for r in records:
        writer.write(r)
    writer.flush()
    read_buf = io.BytesIO(write_buf.getvalue())
    writer.close()

    # Reread
    new_reader = avroc.files.AvroFileReader(read_buf)
    assert new_reader.writer_schema == reader.writer_schema
    assert new_reader.codec.id() == reader.codec.id()
    new_records = [r for r in new_reader]

    assert new_records == records

    read_buf.seek(0)
    # Reread, with schema migration with same schema
    new_reader = avroc.files.AvroFileReader(read_buf, reader.writer_schema)
    assert new_reader.writer_schema == reader.writer_schema
    assert new_reader.codec.id() == reader.codec.id()
    new_records = [r for r in new_reader]

    assert new_records == records
