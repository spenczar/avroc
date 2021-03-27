from typing import Dict, IO, Any, Optional, Iterable
import secrets
import json
import io

from avroc.codegen.read import ReaderCompiler
from avroc.codegen.write import WriterCompiler
from avroc.codegen.resolution import ResolvedReaderCompiler
from avroc.util import SchemaType
from avroc.codec import Codec, NullCodec, codec_by_id
from avroc.runtime import encoding

try:
    from typing import TypedDict
except ImportError:
    TypedDict = Dict


AvroFileHeaderSchema = {
    "type": "record",
    "name": "org.apache.avro.file.Header",
    "fields": [
        {"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
        {"name": "meta", "type": {"type": "map", "values": "bytes"}},
        {"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}},
    ],
}


class AvroFileHeader(TypedDict):
    magic: bytes
    meta: Dict[str, bytes]
    sync: bytes


avro_file_header_write = WriterCompiler(AvroFileHeaderSchema).compile()
avro_file_header_read = ReaderCompiler(AvroFileHeaderSchema).compile()


def write_header(fo: IO[bytes], meta: Dict[str, bytes]) -> bytes:
    """
    Write the Avro Object Container File header to a file-like output. Returns a
    randomly-generated 16-byte sync marker, which should be appended after data
    blocks.
    """
    sync_marker = secrets.token_bytes(16)
    obj = {
        "magic": b"Obj\x01",
        "meta": meta,
        "sync": sync_marker,
    }
    fo.write(avro_file_header_write(obj))
    return sync_marker


def read_header(fo: IO[bytes]) -> AvroFileHeader:
    """
    Read an Avro Object Container File Format header from a file-like input.
    """
    return avro_file_header_read(fo)


class AvroFileWriter:
    def __init__(
        self,
        fo: IO[bytes],
        schema: SchemaType,
        codec: Codec = NullCodec(),
        block_size: int = 1000,
    ):
        self.fo = fo
        self.codec = codec
        self.schema = schema

        self.block_size = block_size
        self.current_block_size = 0
        self.buf = io.BytesIO()

        self._write = WriterCompiler(schema).compile()

        if is_appendable(self.fo):
            self._read_header()
        else:
            self._write_header()

    def _read_header(self) -> None:
        self.fo.seek(0)
        header = read_header(self.fo)
        existing_schema = json.loads(header["meta"]["avro.schema"].decode())
        if existing_schema != self.schema:
            raise ValueError(
                f"provided schema {self.schema} does not match file writer"
                + f"schema {existing_schema}"
            )

        codec_id = header["meta"].get("avro.codec", b"null")
        codec_cls = codec_by_id.get(codec_id)
        if codec_cls is None:
            raise ValueError(f"unknown codec: {codec_id!r}")
        self.codec = codec_cls()
        self.sync_marker = header["sync"]
        self.fo.seek(0, 2)

    def _write_header(self) -> None:
        meta = {
            "avro.schema": json.dumps(self.schema).encode(),
            "avro.codec": self.codec.id(),
        }
        self.sync_marker = write_header(self.fo, meta)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.flush()

    def write(self, msg: Any) -> None:
        self.buf.write(self._write(msg))
        self.current_block_size += 1
        if self.current_block_size >= self.block_size:
            self.flush()

    def flush(self):
        if self.current_block_size == 0:
            # No data to be flushed.
            return

        # Write the number of records.
        self.fo.write(encoding.encode_long(self.current_block_size))

        # Write the encoded record data.
        raw_bytes = self.buf.getvalue()
        encoded_bytes = self.codec.encode(raw_bytes)
        self.fo.write(encoding.encode_bytes(encoded_bytes))

        # Write the sync marker.
        self.fo.write(self.sync_marker)

        # Reset counter.
        self.current_block_size = 0

        # Reset buffer.
        self.buf = io.BytesIO()


class AvroFileReader:
    def __init__(self, fo: IO[bytes], schema: Optional[SchemaType] = None):
        self.fo = fo
        self._read_header()
        if schema is None:
            self._read = ReaderCompiler(self.writer_schema).compile()
        else:
            self._read = ResolvedReaderCompiler(self.writer_schema, schema).compile()
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
            marker = self.fo.read(16)
            assert marker == self.sync_marker, "file is corrupted"

    def __iter__(self):
        return self._iterator

    def __next__(self):
        return next(self._iterator)

    def _read_header(self):
        header = read_header(self.fo)
        if header["magic"] != b"Obj\x01":
            raise ValueError(
                "incorrect magic byte prefix, is this an Avro object container file?"
            )
        self.sync_marker = header["sync"]
        self.writer_schema = json.loads(header["meta"]["avro.schema"].decode())

        codec_id = header["meta"].get("avro.codec", b"null")
        if codec_id not in codec_by_id:

            raise ValueError(f"unknown codec: {codec_id}")
        self.codec = codec_by_id.get(codec_id)()


def write_file(fo: IO[bytes], schema: SchemaType, messages: Iterable[Any]):
    w = AvroFileWriter(fo, schema)
    for m in messages:
        w.write(m)
    w.flush()


def read_file(fo: IO[bytes], schema: Optional[SchemaType] = None) -> Iterable[Any]:
    r = AvroFileReader(fo, schema)
    for msg in r:
        yield msg


def is_appendable(fo):
    if fo.seekable() and fo.tell() != 0:
        if "<stdout>" == getattr(fo, "name", ""):
            # In OSX, sys.stdout is seekable and has a non-zero tell() but
            # we wouldn't want to append to a stdout. In the python REPL,
            # sys.stdout is named `<stdout>`
            return False
        if fo.readable():
            return True
        else:
            raise ValueError(
                "When appending to an avro file you must use the "
                + "'a+' mode, not just 'a'"
            )
    else:
        return False
