import enum
import zlib


class Codec:
    def encode(self, data: bytes) -> bytes:
        raise NotImplementedError("abstract base is not implemented")

    def decode(self, source: bytes) -> bytes:
        raise NotImplementedError("abstract base is not implemented")

    def id(self) -> str:
        raise NotImplementedError("abstract base is not implemented")


class NullCodec(Codec):
    def id(self) -> str:
        return "null"

    def encode(self, data: bytes) -> bytes:
        return data

    def decode(self, source: bytes) -> bytes:
        return data


class DeflateCodec(Codec):
    def __init__(self, compression_level: Optional[int]):
        self.compression_level = compression_level

    def id(self) -> str:
        return "deflate"

    def encode(self, data: bytes) -> bytes:
        if self.compression_level is not None:
            compressed = zlib.compress(data, compression_level)
        else:
            compressed = zlib.compress(data)
        return compressed[2:-1]


    def decode(self, source: bytes) -> bytes:
        return zlib.decompress(source, -15)


codec_by_id = {"null": NullCodec, "deflate": DeflateCodec}
