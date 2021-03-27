from typing import Any, Callable, IO

from avroc.codegen.write import WriterCompiler
from avroc.codegen.read import ReaderCompiler
from avroc.codegen.resolution import ResolvedReaderCompiler


def compile_encoder(schema) -> Callable[[Any], bytes]:
    return WriterCompiler(schema).compile()


def compile_decoder(writer_schema, reader_schema=None) -> Callable[[IO[bytes]], Any]:
    if reader_schema is None:
        return ReaderCompiler(writer_schema).compile()
    else:
        return ResolvedReaderCompiler(writer_schema, reader_schema).compile()
