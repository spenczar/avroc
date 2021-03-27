from typing import Optional

import zlib
import bz2
import snappy
import lzma
import zstandard


class Codec:
    def encode(self, data: bytes) -> bytes:
        raise NotImplementedError("abstract base is not implemented")

    def decode(self, source: bytes) -> bytes:
        raise NotImplementedError("abstract base is not implemented")

    def id(self) -> bytes:
        raise NotImplementedError("abstract base is not implemented")


class NullCodec(Codec):
    def id(self) -> bytes:
        return b"null"

    def encode(self, data: bytes) -> bytes:
        return data

    def decode(self, source: bytes) -> bytes:
        return source


class DeflateCodec(Codec):
    def __init__(self, compression_level: Optional[int] = None):
        self.compression_level = compression_level

    def id(self) -> bytes:
        return b"deflate"

    def encode(self, data: bytes) -> bytes:
        if self.compression_level is not None:
            compressed = zlib.compress(data, self.compression_level)
        else:
            compressed = zlib.compress(data)
        return compressed[2:-1]

    def decode(self, source: bytes) -> bytes:
        return zlib.decompress(source, -15)


class SnappyCodec(Codec):
    def id(self) -> bytes:
        return b"snappy"

    def encode(self, data: bytes) -> bytes:
        encoded = snappy.compress(data)
        encoded += zlib.crc32(encoded).to_bytes(4, "big")
        return encoded

    def decode(self, source: bytes) -> bytes:
        data = source[:-4]
        _ = source[-4:]  # CRC
        return snappy.decompress(data)


class Bzip2Codec(Codec):
    def id(self) -> bytes:
        return b"bzip2"

    def encode(self, data: bytes) -> bytes:
        return bz2.compress(data)

    def decode(self, source: bytes) -> bytes:
        return bz2.decompress(source)


class XZCodec(Codec):
    def id(self) -> bytes:
        return b"xz"

    def encode(self, data: bytes) -> bytes:
        return lzma.compress(data)

    def decode(self, source: bytes) -> bytes:
        return lzma.decompress(source)


class ZstandardCodec(Codec):
    def __init__(self, compressor: Optional[zstandard.ZstdCompressor] = None):
        if compressor is None:
            compressor = zstandard.ZstdCompressor()
        self.compressor = compressor
        self.decompressor = zstandard.ZstdDecompressor()

    def id(self) -> bytes:
        return b"zstandard"

    def encode(self, data: bytes) -> bytes:
        return self.compressor.compress(data)

    def decode(self, source: bytes) -> bytes:
        return self.decompressor.decompress(source)


codec_by_id = {
    b"null": NullCodec,
    b"deflate": DeflateCodec,
    b"snappy": SnappyCodec,
    b"bzip2": Bzip2Codec,
    b"xz": XZCodec,
    b"zstandard": ZstandardCodec,
}
