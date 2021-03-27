import json


class SchemaResolutionError(ValueError):
    def __init__(self, writer, reader, *args, **kwargs):
        self.writer_schema = writer
        self.reader_schema = reader
        super(SchemaResolutionError, self).__init__(*args, **kwargs)

    def __str__(self):
        ws = json.dumps(self.writer_schema)
        rs = json.dumps(self.reader_schema)
        return f"{self.args[0]}: writer={ws}, reader={rs}"

    def __repr__(self):
        return (
            f"SchemaResolutionError({repr(self.writer_schema)}, "
            + f"{repr(self.reader_schema)}, {repr(self.args[0])})"
        )
