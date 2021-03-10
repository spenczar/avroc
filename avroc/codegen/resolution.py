from typing import List, Optional

from avroc.avro_common import PRIMITIVES, is_primitive_schema, schema_type
from avroc.codegen.read import ReaderCompiler
from avroc.codegen.astutil import call_decoder
from avroc.util import SchemaType

from ast import (
    Add,
    AST,
    Assign,
    Attribute,
    BinOp,
    Call,
    Compare,
    Constant,
    Dict as DictLiteral,
    Eq,
    Expr,
    For,
    FloorDiv,
    FunctionDef,
    If,
    IfExp,
    Import,
    ImportFrom,
    Index,
    List as ListLiteral,
    Load,
    Lt,
    Mod,
    Module,
    Mult,
    Name,
    NotEq,
    Raise,
    Return,
    Store,
    Subscript,
    USub,
    UnaryOp,
    While,
    alias,
    arg,
    arguments,
    fix_missing_locations,
    keyword,
    stmt,
)


class ResolvedReaderCompiler(ReaderCompiler):
    def __init__(self, writer_schema: SchemaType, reader_schema: SchemaType):
        if not schemas_match(writer_schema, reader_schema):
            raise ValueError("schemas do not match")
        self.reader_schema = reader_schema
        self.writer_schema = writer_schema
        super(ResolvedReaderCompiler, self).__init__(writer_schema)

    def generate_module(self) -> Module:
        body: List[stmt] = [
            Import(names=[alias(name="datetime")]),
            Import(names=[alias(name="decimal")]),
            Import(names=[alias(name="uuid")]),
        ]

        # Add import statements of low-level decode functions
        body.append(
            ImportFrom(
                module="avroc.runtime.encoding",
                names=[alias(name="*")],
                level=0,
            )
        )
        body.append(
            ImportFrom(
                module="avroc.runtime.blocks",
                names=[alias("decode_block")],
                level=0,
            )
        )
        body.append(self.generate_decoder_func(self.writer_schema, self.reader_schema, self.entrypoint_name))

        # # Identify recursively-defined schemas. For each one, create a named
        # # decoder function.
        # for recursive_type in self.recursive_types:
        #     body.append(
        #         self.generate_decoder_func(
        #             name=self._decoder_name(recursive_type["name"]),
        #             schema=recursive_type,
        #         )
        #     )

        module = Module(
            body=body,
            type_ignores=[],
        )
        module = fix_missing_locations(module)
        return module

    def generate_decoder_func(self, writer_schema: SchemaType, reader_schema: SchemaType, name: str) -> FunctionDef:
        """
        Returns an AST describing a function which can decode an Avro message from a
        IO[bytes] source. The data is decoded from the writer's schema and into
        a message shaped according to the reader's schema.
        """
        src_var = Name(id="src", ctx=Load())
        result_var = Name(id=self.new_variable("result"), ctx=Store())
        func = FunctionDef(
            name=name,
            args=arguments(
                args=[arg(arg="src")],
                posonlyargs=[],
                kwonlyargs=[],
                kw_defaults=[],
                defaults=[],
            ),
            body=[],
            decorator_list=[],
        )

        func.body.extend(self._gen_resolved_decode(writer_schema, reader_schema, src_var, result_var))
        func.body.append(Return(value=Name(id=result_var.id, ctx=Load())))
        return func

    def _gen_resolved_decode(self, writer_schema: SchemaType, reader_schema: SchemaType, src: Name, dest: AST) -> List[stmt]:
        """
        It is an error if the two schemas do not match.

        To match, one of the following must hold:
            both schemas are arrays whose item types match
            both schemas are maps whose value types match
            both schemas are enums whose (unqualified) names match
            both schemas are fixed whose sizes and (unqualified) names match
            both schemas are records with the same (unqualified) name
            either schema is a union
            both schemas have same primitive type
            the writer's schema may be promoted to the reader's as follows:
                int is promotable to long, float, or double
                long is promotable to float or double
                float is promotable to double
                string is promotable to bytes
                bytes is promotable to string

        if both are records:

            the ordering of fields may be different: fields are matched by name.

            schemas for fields with the same name in both records are resolved
            recursively.

            if the writer's record contains a field with a name not present in
            the reader's record, the writer's value for that field is ignored.

            if the reader's record schema has a field that contains a default
            value, and writer's schema does not have a field with the same name,
            then the reader should use the default value from its field.

            if the reader's record schema has a field with no default value, and
            writer's schema does not have a field with the same name, an error
            is signalled.

        if both are enums:
            if the writer's symbol is not present in the reader's enum and the
            reader has a default value, then that value is used, otherwise an
            error is signalled.

        if both are arrays:

            This resolution algorithm is applied recursively to the reader's and
            writer's array item schemas.

        if both are maps:

            This resolution algorithm is applied recursively to the reader's and
            writer's value schemas.

        if both are unions:

            The first schema in the reader's union that matches the selected
            writer's union schema is recursively resolved against it. if none
            match, an error is signalled.

        if reader's is a union, but writer's is not

            The first schema in the reader's union that matches the writer's
            schema is recursively resolved against it. If none match, an error
            is signalled.

        if writer's is a union, but reader's is not

            If the reader's schema matches the selected writer's schema, it is
            recursively resolved against it. If they do not match, an error is
            signalled.
        """
        if isinstance(writer_schema, dict) and "logicalType" in writer_schema:
            raise NotImplementedError("logical types not implemented")
        if isinstance(reader_schema, dict) and "logicalType" in reader_schema:
            raise NotImplementedError("logical types not implemented")

        writer_type = schema_type(writer_schema)
        reader_type = schema_type(reader_schema)

        # Both are primitive types:
        if writer_type in PRIMITIVES and reader_type in PRIMITIVES:
            return self._gen_type_promoting_primitive_decode(writer_type, reader_type, src, dest)

        if writer_type == "union" and reader_type == "union":
            assert isinstance(writer_schema, list)
            assert isinstance(reader_schema, list)
            pass
        if writer_type == "union":
            assert isinstance(writer_schema, list)
            return self._gen_read_from_union(writer_schema, reader_schema, src, dest)
        if reader_type == "union":
            assert isinstance(reader_schema, list)
            pass

        # At this point, we're sure the schemas are dictionaries.
        assert isinstance(writer_schema, dict) and isinstance(reader_schema, dict)
        if writer_type == "enum" and reader_type == "enum":
            return self._gen_enum_decode(writer_schema["symbols"], reader_schema["symbols"], reader_schema.get("default"), src, dest)


    def _gen_type_promoting_primitive_decode(self, writer_schema: str, reader_schema: str, src: Name, dest: AST) -> List[stmt]:
        """
        Generate a series of statements that will read a value according to the
        writer's schema, and then promote it into the reader's schema type.
        """
        # Simple case: identical primitives.
        if writer_schema == reader_schema:
            return self._gen_primitive_decode(writer_schema, src, dest)

        # Reader has a promoted type, but one which doesn't take a type
        # conversion:
        if writer_schema == "int" and reader_schema == "long":
            return self._gen_primitive_decode("int", src, dest)
        if writer_schema == "float" and reader_schema == "double":
            return self._gen_primitive_decode("float", src, dest)

        # Complex case: actually need to promote with a typecast.
        statements: List[stmt] = []
        # Decode the written value into a temporary variable.
        tmp_var = self.new_variable(f"{writer_schema}_value")
        tmp_dest = Name(id=tmp_var, ctx=Store())
        statements.extend(self._gen_primitive_decode(writer_schema, src, tmp_dest))

        # Cast the temporary variable into the final type, assigning to dest.
        tmp_src = Name(id=tmp_var, ctx=Load())

        # int or long -> float or double
        if writer_schema in {"int", "long"} and reader_schema in {"float", "double"}:
            type_cast = Call(
                func=Name(id="float", ctx=Load()),
                args=[tmp_src],
                keywords=[],
            )
        # string -> bytes with str_val.encode("utf8")
        elif writer_schema == "string" and reader_schema == "bytes":
            type_cast = Call(
                func=Attribute(
                    value=tmp_src,
                    attr="encode",
                    ctx=Load(),
                ),
                args=[Constant(value="utf8")],
                keywords=[],
            )
        # bytes -> string with bytes_val.decode("utf8")
        elif writer_schema == "bytes" and reader_schema == "string":
            type_cast = Call(
                func=Attribute(
                    value=tmp_src,
                    attr="decode",
                    ctx=Load(),
                ),
                args=[Constant(value="utf8")],
                keywords=[],
            )
        else:
            raise ValueError(f"unable to promote writer schema {writer_schema} to reader schema {reader_schema}")
        assignment = Assign(
            targets=[dest],
            value=type_cast,
        )
        statements.append(assignment)
        return statements

    def _gen_enum_decode(self, writer_symbols: List[str], reader_symbols: List[str], reader_default: Optional[str], src: Name, dest: AST) -> List[stmt]:
        """
        if the writer's symbol is not present in the reader's enum and the reader
        has a default value, then that value is used, otherwise an error is
        signalled.
        """
        # For each of the reader's symbols, determine what index the writer
        # uses. We'll need to use that value for decoding.
        writer_indexes = {}
        for idx, writer_sym in enumerate(writer_symbols):
            writer_indexes[writer_sym] = idx

        indexes = {}
        for reader_sym in reader_symbols:
            if reader_sym in writer_indexes:
                indexes[reader_sym] = writer_indexes[reader_sym]

        enum_map = DictLiteral(keys=[], values=[])
        for symbol, index in indexes.items():
            enum_map.keys.append(Constant(value=index))
            enum_map.values.append(Constant(value=symbol))

        if reader_default is None:
            # dict[decode_long(src)]
            dict_lookup = Subscript(
                value=enum_map,
                slice=Index(value=call_decoder("long", src)),
                ctx=Load(),
            )
        else:
            # dict.get(decode_long(src), default)
            dict_lookup = Call(
                func=Attribute(
                    value=enum_map,
                    attr="get",
                    ctx=Load(),
                ),
                args=[call_decoder("long", src)],
                keywords=[],
            )
            dict_lookup.args.append(Constant(value=reader_default))

        return [Assign(targets=[dest], value=dict_lookup)]

    def _gen_read_from_union(self, writer_schema: List[SchemaType], reader_schema: SchemaType, src: Name, dest: AST) -> List[stmt]:
        """
        Read data when the writer specified a union, but the reader did not.

        The spec says:
            if writer's schema is a union, but reader's is not:
                If the reader's schema matches the selected writer's schema, it is
                recursively resolved against it. If they do not match, an error is
                signalled.

        This decision can only be made at runtime. If there are any non-matching
        schemas in the writer's union, we generate a 'raise' statement for those
        cases. If there are no schemas that match, then we can be certain that
        the data can never be read, so we raise an error directly and abort
        compilation.
        """
        statements: List[stmt] = []

        # Read a long to figure out which option in the union is chosen.
        idx_var = self.new_variable("union_choice")
        idx_var_dest = Name(id=idx_var, ctx=Store())
        statements.extend(self._gen_primitive_decode("long", src, idx_var_dest))

        idx_var_ref = Name(id=idx_var, ctx=Load())
        prev_if = None

        # Pick a branch based on the option that was chosen.
        # Keep track to make sure that at least one option is even legal.
        any_legal = False
        for idx, option in enumerate(writer_schema):
            # The options are picked based on the index in the union of schemas.
            if_idx_matches = Compare(
                left=idx_var_ref, ops=[Eq()], comparators=[Constant(idx)]
            )
            if_stmt = If(
                test=if_idx_matches,
                orelse=[],
            )
            if schemas_match(option, reader_schema):
                # For options which can be cast into the reader's schema, generate a
                # normal 'decode' statement (and do any casting necessary).
                if_stmt.body = self._gen_resolved_decode(option, reader_schema, src, dest)
                any_legal = True
            else:
                # For options which can't be cast, raise an error.
                msg = f"data written with type {schema_type(option)} is incompatible with reader schema"
                if_stmt.body = [self._gen_schema_error(msg)]

            # Chain statements into a series of if: ... elif: .... sequence
            if prev_if is None:
                statements.append(if_stmt)
            else:
                prev_if.orelse = [if_stmt]
            prev_if = if_stmt

        if not any_legal:
            raise ValueError("none of the options for the writer union can be resolved to reader's schema")
        return statements

    def _gen_schema_error(self, msg: str) -> stmt:
        """
        Generate a statement which represents raising an exception.
        """
        return Raise(
            exc=Call(
                func=Name(id="ValueError", ctx=Load()),
                args=[Constant(value=msg)],
                keywords=[],
            ),
            cause=None,
        )

def schemas_match(writer: SchemaType, reader: SchemaType) -> bool:
    """
    To match, one of the following must hold:
        both schemas are arrays whose item types match
        both schemas are maps whose value types match
        both schemas are enums whose (unqualified) names match
        both schemas are fixed whose sizes and (unqualified) names match
        both schemas are records with the same (unqualified) name
        either schema is a union
        both schemas have same primitive type
        the writer's schema may be promoted to the reader's as follows:
            int is promotable to long, float, or double
            long is promotable to float or double
            float is promotable to double
            string is promotable to bytes
            bytes is promotable to string
    """
    writer_type = schema_type(writer)
    reader_type = schema_type(reader)

    if writer_type == "array" and reader_type == "array":
        return schemas_match[writer["items"], reader["items"]]
    if writer_type == "map" and reader_type == "map":
        return schemas_match[writer["values"], reader["values"]]
    if writer_type == "enum" and reader_type == "enum":
        return writer["name"] == reader["name"]
    if writer_type == "fixed" and reader_type == "fixed":
        return writer["name"] == reader["name"] and writer["size"] == reader["size"]
    if writer_type == "record" and reader_type == "record":
        return writer["name"] == reader["name"]
    if writer_type == "union" or reader_type == "union":
        return True
    if writer_type in PRIMITIVES:
        if writer_type == reader_type:
            return True
        if writer_type == "int":
            return reader_type in {"long", "float", "double"}
        if writer_type == "long":
            return reader_type in {"float", "double"}
        if writer_type == "float":
            return reader_type == "double"
        if writer_type == "string":
            return reader_type == "bytes"
        if writer_type == "bytes":
            return reader_type == "string"
    return False
