from typing import List, Optional

from avroc.avro_common import PRIMITIVES, AVRO_TYPES, is_primitive_schema
from avroc.codegen.read import ReaderCompiler
from avroc.codegen.errors import SchemaResolutionError
from avroc.codegen.graph import find_recursive_types
from avroc.codegen.astutil import call_decoder, literal_from_default, func_call
from avroc.util import SchemaType, clean_name, LogicalTypeError
from avroc.schema import *


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
    expr,
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
    def __init__(self, writer: Schema, reader: Schema):
        self.reader = reader
        self.writer = writer

        self.writer_names = gather_named_types(self.writer)
        self.reader_names = gather_named_types(self.reader)

        if not schemas_match(writer, reader):
            raise SchemaResolutionError(writer, reader, "schemas do not match")

        self.writer_recursive_types = find_recursive_types(self.writer)
        self.writer_recursive_type_names = {x.name for x in self.writer_recursive_types}
        self.reader_recursive_types = find_recursive_types(self.reader)
        self.reader_recursive_type_names = {x.name for x in self.writer_recursive_types}
        super(ResolvedReaderCompiler, self).__init__(writer)

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
        body.append(
            self.generate_upgrader_func(self.writer, self.reader, self.entrypoint_name)
        )

        # Identify recursively-defined schemas. For each one, create a named
        # decoder function, as well as a skip function.
        for recursive_type in self.writer_recursive_types:
            reader_schema = self.reader_names[recursive_type.fullname()]
            body.append(
                self.generate_upgrader_func(
                    writer=recursive_type,
                    reader=reader_schema,
                    name=self._decoder_name(recursive_type),
                )
            )
            body.append(
                self.generate_skip_func(
                    name=self._skipper_name(recursive_type),
                    schema=recursive_type,
                )
            )

        module = Module(
            body=body,
            type_ignores=[],
        )
        module = fix_missing_locations(module)
        return module

    def generate_upgrader_func(
        self, writer: Schema, reader: Schema, name: str
    ) -> FunctionDef:
        """
        Returns an AST describing a function which can decode an Avro message from a
        IO[bytes] source. The data is decoded from the writer's schema and into
        a message shaped according to the reader's schema.
        """
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

        func.body.extend(self._gen_resolved_decode(writer, reader, result_var))
        func.body.append(Return(value=Name(id=result_var.id, ctx=Load())))
        return func

    def _gen_resolved_decode(
        self, writer: Schema, reader: Schema, dest: AST
    ) -> List[stmt]:
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

        if reader == writer:
            # Quick path: Identical schemas can be decoded without any resolution.
            return self._gen_decode(reader, dest)

        if isinstance(reader, LogicalSchema):
            return self._gen_logical_upgrade(writer, reader, dest)

        # We don't actually need to check whether the writer used a logical type
        # if the reader didn't. If they did and the reader didn't, we should
        # deserialize without doing any logical type conversions.

        # Dereference names, checking for recursion.
        if isinstance(writer, NamedSchemaReference):
            writer = writer.referenced_schema
            if writer in self.writer_recursive_types:
                return self._gen_decode_recursive_write(writer, dest)

        if isinstance(reader, NamedSchemaReference):
            reader = reader.referenced_schema

        # Both are primitive types:
        if isinstance(writer, PrimitiveSchema) and isinstance(reader, PrimitiveSchema):
            return self._gen_primitive_upgrade(writer.type, reader.type, dest)

        # Unions
        if isinstance(writer, UnionSchema) and isinstance(reader, UnionSchema):
            return self._gen_union_upgrade(writer, reader, dest)

        if isinstance(writer, UnionSchema):
            return self._gen_read_from_union(writer, reader, dest)

        if isinstance(reader, UnionSchema):
            return self._gen_read_into_union(writer, reader, dest)

        # At this point, we're sure the schemas are dictionaries.

        if isinstance(writer, EnumSchema) and isinstance(reader, EnumSchema):
            return self._gen_enum_upgrade(
                writer.symbols, reader.symbols, reader.default, dest
            )

        if isinstance(writer, RecordSchema) and isinstance(reader, RecordSchema):
            return self._gen_record_upgrade(writer, reader, dest)

        if isinstance(writer, ArraySchema) and isinstance(reader, ArraySchema):
            return self._gen_array_upgrade(writer.items, reader.items, dest)

        if isinstance(writer, MapSchema) and isinstance(reader, MapSchema):
            return self._gen_map_upgrade(writer.values, reader.values, dest)

        if isinstance(writer, FixedSchema) and isinstance(reader, FixedSchema):
            return self._gen_fixed_decode(reader.size, dest)

        raise SchemaResolutionError(
            writer, reader, "reader and writer schemas are incompatible"
        )

    def _gen_primitive_upgrade(
        self, writer_schema: str, reader_schema: str, dest: AST
    ) -> List[stmt]:
        """
        Generate a series of statements that will read a value according to the
        writer's schema, and then promote it into the reader's schema type.
        """
        # Simple case: identical primitives.
        if writer_schema == reader_schema:
            return self._gen_primitive_decode(writer_schema, dest)

        # Reader has a promoted type, but one which doesn't take a type
        # conversion:
        if writer_schema == "int" and reader_schema == "long":
            return self._gen_primitive_decode("int", dest)
        if writer_schema == "float" and reader_schema == "double":
            return self._gen_primitive_decode("float", dest)

        # Complex case: actually need to promote with a typecast.
        statements: List[stmt] = []
        # Decode the written value into a temporary variable.
        tmp_var = self.new_variable(f"{writer_schema}_value")
        tmp_dest = Name(id=tmp_var, ctx=Store())
        statements.extend(self._gen_primitive_decode(writer_schema, tmp_dest))

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
            raise SchemaResolutionError(
                writer_schema, reader_schema, "primitive types are incompatible"
            )
        assignment = Assign(
            targets=[dest],
            value=type_cast,
        )
        statements.append(assignment)
        return statements

    def _gen_enum_upgrade(
        self,
        writer_symbols: List[str],
        reader_symbols: List[str],
        reader_default: Optional[str],
        dest: AST,
    ) -> List[stmt]:
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

        dict_lookup: expr
        if reader_default is None:
            # dict[decode_long(src)]
            dict_lookup = Subscript(
                value=enum_map,
                slice=Index(value=call_decoder("long")),
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
                args=[call_decoder("long")],
                keywords=[],
            )
            dict_lookup.args.append(Constant(value=reader_default))

        return [Assign(targets=[dest], value=dict_lookup)]

    def _gen_union_upgrade(
        self,
        writer: UnionSchema,
        reader: UnionSchema,
        dest: AST,
    ) -> List[stmt]:
        """
        Read data when both the writer and reader specified a union.

        The spec says:
            The first schema in the reader's union that matches the selected
            writer's union schema is recursively resolved against it. if none
            match, an error is signalled.

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
        statements.extend(self._gen_primitive_decode("long", idx_var_dest))

        idx_var_ref = Name(id=idx_var, ctx=Load())
        prev_if = None

        # Pick a branch based on the option that was chosen.
        # Keep track to make sure that at least one option is even legal.
        any_legal = False
        for idx, option in enumerate(writer.options):
            # The options are picked based on the index in the union of schemas.
            if_idx_matches = Compare(
                left=idx_var_ref, ops=[Eq()], comparators=[Constant(idx)]
            )
            if_stmt = If(
                test=if_idx_matches,
                orelse=[],
            )
            # Take the first matching reader schema and use it for decoding.
            for r in reader.options:
                if schemas_match(option, r):
                    # For options which can be cast into the reader's schema, generate a
                    # normal 'decode' statement (and do any casting necessary).
                    if_stmt.body = self._gen_resolved_decode(option, r, dest)
                    any_legal = True
                    break
            else:
                # For options which can't be cast, raise an error.
                msg = f"data written with type {option} is incompatible with reader schema"
                if_stmt.body = [self._gen_schema_error(msg)]

            # Chain statements into a series of if: ... elif: .... sequence
            if prev_if is None:
                statements.append(if_stmt)
            else:
                prev_if.orelse = [if_stmt]
            prev_if = if_stmt

        if not any_legal:
            raise SchemaResolutionError(
                writer,
                reader,
                "none of the options for the writer union can be resolved to reader's schema",
            )
        return statements

    def _gen_read_into_union(
        self, writer_schema: Schema, reader_schema: UnionSchema, dest: AST
    ) -> List[stmt]:
        """
        Read data when the reader specified a union but the writer did not.

        The spec says:
            if reader's is a union, but writer's is not

                The first schema in the reader's union that matches the writer's
                schema is recursively resolved against it. If none match, an
                error is signalled.
        """
        for schema in reader_schema.options:
            if schemas_match(writer_schema, schema):
                return self._gen_resolved_decode(writer_schema, schema, dest)
        raise SchemaResolutionError(
            writer_schema,
            reader_schema,
            "none of the reader's options match the writer",
        )

    def _gen_read_from_union(
        self, writer: UnionSchema, reader: Schema, dest: AST
    ) -> List[stmt]:
        """
        Read data when the writer specified a union, but the reader did not.

        The spec says:
            if writer's schema is a union, but reader's is not:
                If the reader's schema matches the selected writer's schema, it is
                recursively resolved against it. If they do not match, an error is
                signalled.

        This is equivalent to the case where both writer and reader provided a
        union, but as if the reader's union only has one option.
        """
        reader_as_union = UnionSchema(
            type="union",
            default=None,
            _names=reader._names,
            options=[reader],
        )
        return self._gen_union_upgrade(writer, reader_as_union, dest)

    def _gen_record_upgrade(
        self, writer: RecordSchema, reader: RecordSchema, dest: AST
    ) -> List[stmt]:

        reader_fields_by_name = {f.name: f for f in reader.fields}
        writer_fields_by_name = {f.name: f for f in writer.fields}
        # Construct a new dictionary to hold the record contents. If there are
        # any defaults, set those in the literal dictionary. Otherwise, make an
        # empty one.
        record_value = DictLiteral(keys=[], values=[])
        for field in reader.fields:
            if field.name not in writer_fields_by_name:
                if field.default is not None:
                    record_value.keys.append(Constant(value=field.name))
                    default_value = literal_from_default(field.default, field.type)
                    record_value.values.append(default_value)
                else:
                    raise SchemaResolutionError(
                        writer,
                        reader,
                        f"missing field {field.name} from writer schema and no default is set",
                    )

        # We've constructed the AST node representing a dictionary literal. Now,
        # assign it to a variable.
        record_value_name = self.new_variable(clean_name(reader.name))
        statements: List[stmt] = []
        statements.append(
            Assign(
                targets=[Name(id=record_value_name, ctx=Store())],
                value=record_value,
            )
        )

        # Fill in the fields based on the writer's order.
        for writer_field in writer.fields:
            # If the writer's field is present in the reader, read it.
            # Otherwise, skip it.
            if writer_field.name in reader_fields_by_name:
                reader_field = reader_fields_by_name[writer_field.name]
                field_dest = Subscript(
                    value=Name(id=record_value_name, ctx=Load()),
                    slice=Index(value=Constant(value=reader_field.name)),
                    ctx=Store(),
                )
                statements.extend(
                    self._gen_resolved_decode(
                        writer_field.type, reader_field.type, field_dest
                    )
                )
            else:
                statements.extend(self._gen_skip(writer_field.type))

        # Assign the created object to the target dest
        statements.append(
            Assign(
                targets=[dest],
                value=Name(id=record_value_name, ctx=Load()),
            ),
        )
        return statements

    def _gen_array_upgrade(
        self, writer_item_schema: Schema, reader_item_schema: Schema, dest: AST
    ) -> List[stmt]:
        """
        Generate statements to decode an array of data, applying the schema
        resolution argument to the array item schemas.
        """
        statements: List[stmt] = []

        # Create a new empty list to hold the values we'll read.
        name = "array_"
        if isinstance(reader_item_schema, NamedSchema):
            name += reader_item_schema.name
        else:
            name += reader_item_schema.type
        name = clean_name(name)
        list_varname = self.new_variable(name)

        assign_stmt = Assign(
            targets=[Name(id=list_varname, ctx=Store())],
            value=ListLiteral(elts=[], ctx=Load()),
        )
        statements.append(assign_stmt)

        # For each message in the array...
        for_each_message: List[stmt] = []

        # ... read a value...
        value_varname = self.new_variable("array_val")
        value_dest = Name(id=value_varname, ctx=Store())
        read_statements = self._gen_resolved_decode(
            writer_item_schema, reader_item_schema, value_dest
        )
        for_each_message.extend(read_statements)

        # ... and append it to the list.
        list_append_method = Attribute(
            value=Name(id=list_varname, ctx=Load()),
            attr="append",
            ctx=Load(),
        )
        list_append_method_call = Expr(
            Call(
                func=list_append_method,
                args=[Name(id=value_varname, ctx=Load())],
                keywords=[],
            )
        )
        for_each_message.append(list_append_method_call)

        statements.extend(self._gen_block_decode(for_each_message))

        # Finally, assign the list we have constructed into the destination AST
        # node.
        assign_result = Assign(
            targets=[dest],
            value=Name(id=list_varname, ctx=Load()),
        )
        statements.append(assign_result)
        return statements

    def _gen_map_upgrade(
        self, writer_values: Schema, reader_values: Schema, dest: AST
    ) -> List[stmt]:
        """
        Generate statements to decode a map of data, applying the schema resolution
        argument to the map value schemas.
        """
        statements: List[stmt] = []

        name = "map_"
        if isinstance(reader_values, NamedSchema):
            name += reader_values.name
        else:
            name += reader_values.type
        name = clean_name(name)

        map_varname = self.new_variable(name)
        assign_stmt = Assign(
            targets=[Name(id=map_varname, ctx=Store())],
            value=DictLiteral(keys=[], values=[]),
        )
        statements.append(assign_stmt)

        # For each message in a block...
        for_each_message = []

        # ... read a string key...
        key_varname = self.new_variable("key")
        key_dest = Name(id=key_varname, ctx=Store())
        for_each_message.extend(self._gen_primitive_decode("string", key_dest))
        # ... and read the corresponding value.
        value_dest = Subscript(
            value=Name(id=map_varname, ctx=Load()),
            slice=Index(Name(id=key_varname, ctx=Load())),
            ctx=Store(),
        )
        for_each_message.extend(
            self._gen_resolved_decode(writer_values, reader_values, value_dest)
        )

        statements.extend(self._gen_block_decode(for_each_message))

        # Finally, assign our resulting map to the destination target.
        statements.append(
            Assign(
                targets=[dest],
                value=Name(id=map_varname, ctx=Load()),
            )
        )
        return statements

    def _gen_decode_recursive_write(self, writer: NamedSchema, dest: AST) -> List[stmt]:
        funcname = self._decoder_name(writer)
        return [
            Assign(
                targets=[dest],
                value=Call(
                    func=Name(id=funcname, ctx=Load()),
                    args=[Name(id="src", ctx=Load())],
                    keywords=[],
                ),
            )
        ]

    def _gen_decode_recursive_read(
        self, writer: Schema, reader: Schema, dest: AST
    ) -> List[stmt]:
        # I don't think this is reachable?
        raise NotImplementedError("not implemented")

    def _gen_logical_upgrade(
        self, writer: Schema, reader: LogicalSchema, dest: AST
    ) -> List[stmt]:
        lt = reader.logical_type

        if isinstance(writer, LogicalSchema):
            # The writer picked a logical schema too. If it's not the same one
            # as the reader, then something is probably very wrong.
            if writer.logical_type != reader.logical_type:
                raise SchemaResolutionError(writer, reader, "inconsistent logical types between reader and writer")
            # Some logical types are unparameterized. Perhaps reader and writer
            # just differ by a documentation comment, or a default. We can just
            # decode them directly.
            if lt in {"uuid", "date", "time-millis", "timestamp-millis", "time-micros", "timestamp-micros", "duration"}:
                return self._gen_logical_decode(reader, dest)

            # Decimal types are parameterized

        # The writer isn't providing a logical type. Are they at least providing the same Maybe we need to do a type promotion.
        if lt == "uuid":
            assert writer.type == "bytes"
            call = func_call("uuid_from_bytes", [call_decoder("bytes")])
            return [Assign(targets=[dest], value=call)]
        if lt == "decimal":
            assert writer.type == "string"
            call = func_call("decimal_from_string", [call_decoder("string")])
            return [Assign(targets=[dest], value=call)]
        if lt == "time-micros":
            assert writer.type == "int"
            call = func_call("time_micros_from_int", [call_decoder("int")])
            return [Assign(targets=[dest], value=call)]
        if lt == "timestamp-millis":
            assert writer.type == "int"
            call = func_call("timestamp_millis_from_int", [call_decoder("int")])
            return [Assign(targets=[dest], value=call)]
        if lt == "timestamp-micros":
            assert writer.type == "int"
            call = func_call("timestamp_micros_from_int", [call_decoder("int")])
            return [Assign(targets=[dest], value=call)]

        raise SchemaResolutionError(
            writer, reader, "unable to promote for logical type conversion"
        )

    def _gen_logical_uuid_decode(
        self, writer: PrimitiveSchema, reader: SchemaType, dest: AST
    ) -> List[stmt]:
        # Decode a string (or promote from bytes).
        if writer.type == "string":
            # Writer used a string, so we can decode it directly.
            call = func_call("decode_uuid", [Name(id="src", ctx=Load())])
            return [Assign(targets=[dest], value=call)]
        elif writer.type == "bytes":
            # Promote bytes into string.
            call = func_call("uuid_from_bytes", [call_decoder("bytes")])
            return [Assign(targets=[dest], value=call)]
        else:
            raise SchemaResolutionError(
                writer, reader, "cannot read uuid from writer type"
            )

    def _gen_logical_decimal_bytes_decode(
        self, writer: PrimitiveSchema, reader: DecimalBytesSchema, dest: AST
    ) -> List[stmt]:
        if writer.type == "bytes":
            # Writer used bytes, so we can decode it directly. Note that reader
            # and writer must have the same scale and precision; this was
            # checked when we checke dthat the schemas match.
            scale_val = 0 if reader.scale is None else reader.scale

            call = func_call(
                "decode_decimal_bytes",
                [
                    Name(id="src", ctx=Load()),
                    Constant(value=reader.precision),
                    Constant(value=scale_val),
                ],
            )
            return [Assign(targets=[dest], value=call)]
        elif writer.type == "string":
            # Promote string into bytes.
            scale_val = 0 if reader.scale is None else reader.scale
            call = func_call(
                "decimal_from_string",
                [
                    call_decoder("string"),
                    Constant(value=reader.precision),
                    Constant(value=scale_val),
                ],
            )
            return [Assign(targets=[dest], value=call)]
        else:
            raise SchemaResolutionError(
                writer, reader, "cannot read decimal from writer type"
            )

    ### Skip Methods ###
    def _skipper_name(self, type: NamedSchema) -> str:
        return f"_skip_{clean_name(type.fullname())}"

    def generate_skip_func(self, schema: Schema, name: str) -> FunctionDef:
        """
        Returns an AST describing a function which can skip past an Avro message
        from a IO[bytes] source. The data is decoded from the writer's schema
        and discarded.
        """
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

        func.body.extend(self._gen_skip(schema))
        return func

    def _gen_skip(self, schema: Schema) -> List[stmt]:
        """
        Generate code to skip data that follows a given schema.
        """
        if isinstance(schema, NamedSchemaReference):
            if schema.referenced_schema in self.writer_recursive_types:
                return self._gen_skip_recursive_type(schema.referenced_schema)
            schema = schema.referenced_schema
        if isinstance(schema, PrimitiveSchema):
            return self._gen_skip_primitive(schema.type)
        if isinstance(schema, UnionSchema):
            return self._gen_skip_union(schema.options)
        if isinstance(schema, RecordSchema):
            return self._gen_skip_record(schema)
        if isinstance(schema, MapSchema):
            return self._gen_skip_map(schema.values)
        if isinstance(schema, ArraySchema):
            return self._gen_skip_array(schema.items)
        if isinstance(schema, FixedSchema):
            return self._gen_skip_fixed(schema.size)
        if isinstance(schema, EnumSchema):
            return self._gen_skip_enum()

        raise NotImplementedError(f"skip not implemented for schema {schema}")

    def _gen_skip_primitive(self, schema: str) -> List[stmt]:
        """
        Generate code to skip a single primitive value.
        """
        if schema == "null":
            return []
        return [
            Expr(
                value=Call(
                    func=Name(id="skip_" + schema, ctx=Load()),
                    args=[Name(id="src", ctx=Load())],
                    keywords=[],
                ),
            )
        ]

    def _gen_skip_union(self, options: List[Schema]) -> List[stmt]:
        """
        Generate code to skip a union value.
        """
        # Union is encoded as a long which indexes into the options list, and
        # then a value. So, start by reading the long.
        statements: List[stmt] = []
        idx_var_name = self.new_variable("union_choice")
        idx_var_dest = Name(id=idx_var_name, ctx=Store())
        statements.extend(self._gen_primitive_decode("long", idx_var_dest))

        # Now, for each option, generate code to skip whatever is encoded.

        prev_if = None
        for idx, option in enumerate(options):
            if isinstance(option, PrimitiveSchema) and option.type == "null":
                # Skip nulls, since there is nothing to be done with them anyway.
                continue
            if_idx_matches = Compare(
                left=Name(id=idx_var_name, ctx=Load()),
                ops=[Eq()],
                comparators=[Constant(idx)],
            )
            if_stmt = If(
                test=if_idx_matches,
                body=self._gen_skip(option),
                orelse=[],
            )

            if prev_if is None:
                statements.append(if_stmt)
            else:
                prev_if.orelse = [if_stmt]
            prev_if = if_stmt
        assert prev_if is not None
        return statements

    def _gen_skip_record(self, schema: RecordSchema) -> List[stmt]:
        """
        Generate statements to skip an entire record.
        """
        statements: List[stmt] = []
        for field in schema.fields:
            statements.extend(self._gen_skip(field.type))
        return statements

    def _gen_skip_array(self, item_schema: Schema) -> List[stmt]:
        """
        Generate statements to skip an array of data.
        """
        for_each_message = self._gen_skip(item_schema)
        return self._gen_block_decode(for_each_message)

    def _gen_skip_map(self, value_schema: Schema) -> List[stmt]:
        """
        Generate statements to skip an array of data.
        """
        # For each message...
        for_each_message: List[stmt] = []
        # ... skip the string key...
        for_each_message.extend(self._gen_skip_primitive("string"))
        # ... and then skip the value.
        for_each_message.extend(self._gen_skip(value_schema))
        return self._gen_block_decode(for_each_message)

    def _gen_skip_fixed(self, size: int) -> List[stmt]:
        """
        Generate statements to skip a fixed message.
        """
        # Just src.read(size).
        return [
            Expr(
                value=Call(
                    func=Attribute(
                        value=Name(id="src", ctx=Load()),
                        attr="read",
                        ctx=Load(),
                    ),
                    args=[Constant(value=size)],
                    keywords=[],
                )
            )
        ]

    def _gen_skip_enum(self) -> List[stmt]:
        """
        Generate statements to skip an enum.
        """
        return self._gen_skip_primitive("long")

    def _gen_skip_recursive_type(self, type: NamedSchema) -> List[stmt]:
        funcname = self._skipper_name(type)
        return [
            Expr(
                value=Call(
                    func=Name(id=funcname, ctx=Load()),
                    args=[Name(id="src", ctx=Load())],
                    keywords=[],
                ),
            )
        ]

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
