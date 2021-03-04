from typing import IO, Generator

from .encoding import decode_long, skip_long


def decode_block(src: IO[bytes]) -> Generator:
    blocksize: int = decode_long(src)
    while blocksize != 0:
        if blocksize < 0:
            blocksize = -blocksize
            skip_long(src)
        for _ in range(blocksize):
            yield
        blocksize = decode_long(src)
