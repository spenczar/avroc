from typing import NoReturn, Dict, TypedDict
import random
import json


from avroc.codegen.read import ReaderCompiler
from avroc.codegen.write import WriterCompiler
from avroc.util import SchemaType
from avroc.codec import Codec, DeflateCodec, NullCodec, code_by_id
from avroc.runtime import encoding

AvroFileHeaderSchema = {
    "type": "record",
    "name": "org.apache.avro.file.Header",
    "fields": [
        {"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
        {"name": "meta", "type": {"type": "map", "values": "bytes"}},
        {"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}},
    ]
}

class AvroFileHeader(TypedDict):
    magic: bytes
    meta: Dict[str, bytes]
    sync: bytes

avro_file_header_write = WriterCompiler(AvroFileHeaderSchema).compile()
avro_file_header_read = ReaderCompiler(AvroFileHeaderSchema).compile()


def write_header(fo: Bytes[IO], meta: Dict[str, bytes]) -> bytes:
    """
    Write the Avro Object Container File header to a file-like output. Returns a
    randomly-generated 16-byte sync marker, which should be appended after data
    blocks.
    """
    sync_marker = random.randbytes(16)
    obj = {
        "magic": b'Obj\x01',
        "meta": meta,
        "sync": sync_marker,
    }
    avro_file_header_write(fo, obj)
    return sync_marker


def read_header(fo: Bytes[IO]) -> AvroFileHeader:
    """
    Read an Avro Object Container File Format header from a file-like input.
    """
    return avro_file_header_read(fo)


class AvroFileWriter:
    def __init__(self, fo: Bytes[IO], schema: SchemaType, codec: Codec, block_size: int=1000):
        self.fo = fo
        self.codec = codec
        self.schema = schema

        self.block_size = block_size
        self.current_block_size = 0
        self.buf = io.BytesIO()

        self._write = WriterCompiler(schema).compile()

    def _write_header(self) -> bytes:
        meta = {
            "avro.schema": json.dumps(self.schema).encode(),
            "avro.codec": self.codec.id().encode(),
        }
        self.sync_marker = write_header(self.fo, meta)

    def write(self, msg: Any) -> NoReturn:
        self._write(self.buf, msg)
        self.current_block_size += 1
        if self.current_block_size >= self.block_size:
            self.flush()

    def flush(self):
        # Write the number of records.
        encoding.encode_long(self.fo, self.current_block_size)

        # Write the encoded record data.
        self.buf.seek(0)
        raw_bytes = self.buf.read()
        encoded_bytes = self.codec.encode(raw_bytes)
        encoding.encode_bytes(self.fo, encoded_bytes)

        # Write the sync marker.
        fo.write(self.sync_marker)

        # Reset counter.
        self.current_block_size = 0


    def close(self):
        self.flush()
        self.fo.close()


class AvroFileReader:
    def __init__(self, fo: Bytes[IO], schema: Optional[SchemaType]=None):
        self.fo = fo
        self._read_header()
        if schema is None:
            self._read = avroc.codegen.read.ReaderCompiler(self.writer_schema).compile()
        else:
            self._read = avroc.codegen.resolution.ResolvedReaderCompiler(self.writer_schema, schema).compile()
        self._iterator = self._read_blocks()

    def _read_blocks(self):
        while True:
            try:
                num_records = encoding.decode_long(self.fo)
            except IndexError:
                return
            raw_bytes = encoding.decode_bytes(self.fo)
            decoded_bytes = self.codec.decode(raw_bytes)
            byte_buffer = io.BytesIO(decoded_bytes)
            for _ in range(num_records):
                yield self._read(byte_buffer)

    def __iter__(self):
        return self._iterator

    def __next__(self):
        return next(self._iterator)

    def _read_header(self):
        header = read_header(self.fo)
        if header["magic"] != b'Obj\x01':
            raise ValueError("incorrect magic byte prefix, is this an Avro object container file?")
        self.sync = header["sync"]
        self.writer_schema = json.loads(header["meta"]["avro.schema"].decode())

        codec_id = header["meta"]["avro.codec"]
        if codec_id not in code_by_id:
            raise ValueError(f"unknown codec: {codec_id}")
        self.codec = codec_by_id.get(codec_id)
