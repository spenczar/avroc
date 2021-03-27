===
API
===

.. py:module:: avroc
   :synopsis: The main entrypoint

``avroc``
---------

This module holds the main public API.

.. py:function:: compile_encoder(schema)

   Construct a callable which encodes Python objects to bytes according to an
   Avro schema.

   :param schema: The schema to use when encoding data. Usually, this is a
                  ``dict``.
   :type schema: dict (see :ref:`schema-types`)
   :rtype: function encoder(msg) -> bytes

.. py:function:: compile_decoder(writer_schema, reader_schema=None)

   Construct a callable which decodes Python objects from a bytes reader.

   :param writer_schema: The schema used by the writer when encoding data.
                         Usually, this is a ``dict``.
   :type writer_schema: dict (see :ref:`schema-types`)
   :param reader_schema: An optional schema to transform messages into when
                         decoding data. The schema must be compatible with the
                         writer's schema, in the sense described by the Avro
                         spec; see :ref:`schema-resolution` for details.
   :type reader_schema: Optional[dict] (see :ref:`schema-types`)
   :rtype: function decoder(fp) -> msg

.. py:function:: read_file(fo, schema)

   Read a file containing Avro messages. The file should already be opened, and
   should be opened in binary mode (like ``open(path, "rb")``, for example).

   The messages are provided as an iterator. To get all the messages in a list,
   you can use ``list(read_file(fp))``, for example.

   The optional ``schema`` parameter can be used to read data into a different
   shape than the writer used; see :ref:`schema-resolution` for more.

   Note that the writer's schema is always included in an Avro data file, so the
   schema is purely optional - you only need to pass it if you want to use a
   *different* schema than the writer used during encoding.

   :param fo: A handle to a file-like bytes source to read.
   :type fo: IO[bytes]
   :param schema: An optional schema to transform messages into when decoding
                  data. The schema must be compatible with the writer's schema,
                  in the sense described by the Avro spec; see
                  :ref:`schema-resolution` for details. If no schema is
                  provided, then the writer's schema is used.
   :type schema: Optional[dict] (see :ref:`schema-types`)
   :returns: An iterator of the messages in the file. The messages' type depend
             on the schema used when decoding, as laid out in :ref:`message-types`.
   :rtype: Iterable[msg]

.. py:function:: write_file(fo, schema, messages)

   Write messages to an open file according under a given Avro schema.

   All messages in the iterable will be consumed and written.

   :param fo: A handle to a file-like bytes destination to write to.
   :type fo: IO[bytes]
   :param schema: The schema to use when encoding data.
   :type schema: dict (see :ref:`schema-types`)
   :param messages: An iterable of the messages to write into the file. The
                 messages must be encodable under the given schema; see
                 :ref:`message-types` for details.

   :type messages: Iterable[msg]

.. py:class:: AvroFileWriter(fo, schema, codec=NullCodec, block_size=1000)

   A low-level class for writing Avro data to a file, complete with all
   persnickety details. Most users should use :py:obj:`write_file`.

   AvroFileWriter provides these additional capabilities on top of :py:obj:`write_file`:
    - You can write messages one-by-one, rather than passing an entire iterator
      of messages.
    - You can choose a compression codec to apply to all data bytes written to
      the file; the codec is stored in the Avro header so other readers will
      know how to read the data automatically.
    - You can pick a block size and choose exactly when flushes occur.

   Writes are buffered, and written in `blocks` of the given ``block-size``. As
   a result, it is important to call :py:meth:`flush` to be ensure that all
   writes are actually persisted to the underlying file.

   This can be done by using the AvroFileWriter as a context manager. For example, like this:

   .. code-block:: py

      with open("data.avro", "wb") as f:
          with AvroFileWriter(f, schema) as w:
              w.write(msg1)
              w.write(msg2)
              w.write(msg3)

      # When the 'with' block is exited, all writes will be
      # flushed, so this is safe.

   :param fo: A file-like object that can be written to in binary mode.
   :type fo: File-like in bytes mode
   :param schema: The schema to use when encoding data.
   :type schema: dict (see :ref:`schema-types`)
   :param codec: A compression codec to use when encoding data. The valid
                 options are all the classes in :py:mod:`avroc.codec`. Make sure
                 to pass an instantiated instance, not a class.
   :type codec: avroc.codec.Codec

   .. py:method:: write(msg)

      Write a single message to the Avro file. Writes are batched into large
      blocks; call :py:meth:`flush` to flush the current block.

      :param msg: A message conforming to the writer's schema.

   .. py:method:: flush()

      Flush any outstanding writes to the underlying file.

   .. py:method:: __enter__()

      Returns self, allowing the writer to be used as a context manager.

   .. py:method:: __exit__(exc_type, exc_value, exc_traceback)

      Flushes any buffered writes and exits the context-managed block.



.. py:module:: avroc.codec
   :synopsis: Compression codecs which can be used when writing Avro files

``avroc.codec``
---------------

Avro has some officially-endorsed codecs which can be used when writing files
(and are automatically selected when reading encoded files). Using these can
help you save some space, at the cost of a bit of CPU time for compression and
decompression.

Avroc implements all the codecs `from the Avro specification <http://avro.apache.org/docs/1.10.2/spec.html#Required+Codecs>`_.

.. py:class:: Codec()

   Abstract base class, implemented by the other classes in this module. Those classes are:

   +---------------------------+------------------------------+
   | Class                     | Description                  |
   +===========================+==============================+
   | :py:obj:`NullCodec`       |No compression                |
   +---------------------------+------------------------------+
   | :py:obj:`DeflateCodec`    |Compress with DEFLATE, similar|
   |                           |to gzip                       |
   +---------------------------+------------------------------+
   | :py:obj:`SnappyCodec`     |Compress with snappy          |
   +---------------------------+------------------------------+
   | :py:obj:`Bzip2Codec`      |Compress with bzip2           |
   +---------------------------+------------------------------+
   | :py:obj:`XZCodec`         |Compress with xz, from the    |
   |                           |lzma family                   |
   +---------------------------+------------------------------+
   | :py:obj:`ZstandardCodec`  |Compress with zstandard       |
   +---------------------------+------------------------------+


.. py:class:: NullCodec()

   A NullCodec does no compression. It just passes data through.

.. py:class:: DeflateCodec(compression_level=None)

   A DeflateCodec uses the deflate algorithm from `RFC 1951
   <https://www.isi.edu/in-notes/rfc1951.txt>`_.

   :param compression_level: The Deflate compression level to use. Higher is
                             more compressed. 0 is no compression, and 9 is max
                             compression. Defaults to the default compression
                             level of Python's :py:obj:`zlib` (currently 6).
   :type compression_level: int

.. py:class:: SnappyCodec()

   A SnappyCodec uses Google's `snappy <https://code.google.com/p/snappy/>`_
   compression algorithm, followed by a 4-byte CRC32 checksum.

.. py:class:: Bzip2Codec()

   A Bzip2Codec uses the `bzip2 <https://docs.python.org/3/library/bz2.html>`_
   compression algorithm.

.. py:class:: XZCodec()

   A XZCodec uses the `lzma <https://docs.python.org/3/library/lzma.html>`_
   compression algorithm.

.. py:class:: ZstandardCodec(compressor=None)

   A ZstandardCodec uses the `zstandard <https://facebook.github.io/zstd/>`_
   compression algorithm.

   :param compressor: A compressor, possibly which has already been trained on
                      other data, which should be used when compressing data. If
                      unset, then a compressor with all the default values is
                      used.
   :type compressor: :py:obj:`zstandard.ZstdCompressor`
