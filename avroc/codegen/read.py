from typing import Dict, Any, List, Optional

from avroc.avro_common import PRIMITIVES
from avroc.util import SchemaType, clean_name, LogicalTypeError
from avroc.codegen.compiler import Compiler
from avroc.codegen.astutil import call_decoder, func_call

from ast import (
    AST,
    Assign,
    Attribute,
    Call,
    Compare,
    Constant,
    Dict as DictLiteral,
    Eq,
    Expr,
    For,
    FunctionDef,
    If,
    IfExp,
    Import,
    ImportFrom,
    Index,
    List as ListLiteral,
    Load,
    Module,
    Name,
    Return,
    Store,
    Subscript,
    alias,
    arg,
    arguments,
    fix_missing_locations,
    stmt,
)


class ReaderCompiler(Compiler):
    def __init__(self, schema: SchemaType):
        super(ReaderCompiler, self).__init__(schema, "decoder")

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
        body.append(self.generate_decoder_func(self.schema, self.entrypoint_name))

        # Identify recursively-defined schemas. For each one, create a named
        # decoder function.
        for recursive_type in self.recursive_types:
            body.append(
                self.generate_decoder_func(
                    name=self._decoder_name(recursive_type["name"]),
                    schema=recursive_type,
                )
            )

        module = Module(
            body=body,
            type_ignores=[],
        )
        module = fix_missing_locations(module)
        return module

    @staticmethod
    def _decoder_name(name: str) -> str:
        return "_decode_" + clean_name(name)

    def generate_decoder_func(self, schema: SchemaType, name: str) -> FunctionDef:
        """
        Returns an AST describing a function which can decode an Avro message from a
        IO[bytes] source. The message is parsed according to the given schema.
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

        func.body.extend(self._gen_decode(schema, result_var))
        func.body.append(Return(value=Name(id=result_var.id, ctx=Load())))
        return func

    def _gen_decode(self, schema: SchemaType, dest: AST) -> List[stmt]:
        """
        Returns a sequence of statements which will read data and write the
        deserialized value into dest.
        """
        if isinstance(schema, str):
            if schema in PRIMITIVES:
                return self._gen_primitive_decode(primitive_type=schema, dest=dest)
            else:
                # Named type reference. Could be recursion?
                if schema in set(t["name"] for t in self.recursive_types):
                    # Yep, recursion. Just generate a function call - we'll have
                    # a separate function to handle this type.
                    return self._gen_recursive_decode_call(schema, dest)
                else:
                    # Not recursion. We can inline the decode, assuming the
                    # schema was already present.
                    referenced_schema = self.named_types.get(schema)
                    if referenced_schema is None:
                        raise ValueError(
                            f"schema {schema} was used before it is defined"
                        )
                    return self._gen_decode(referenced_schema, dest)
        if isinstance(schema, list):
            return self._gen_union_decode(
                options=schema,
                dest=dest,
            )
        if isinstance(schema, dict):
            if "logicalType" in schema:
                return self._gen_logical_decode(
                    schema=schema,
                    dest=dest,
                )
            schema_type = schema["type"]
            if schema_type in PRIMITIVES:
                return self._gen_primitive_decode(
                    primitive_type=schema_type,
                    dest=dest,
                )
            if schema_type == "record" or schema_type == "error":
                return self._gen_record_decode(
                    schema=schema,
                    dest=dest,
                )
            if schema_type == "array":
                return self._gen_array_decode(
                    item_schema=schema["items"],
                    dest=dest,
                )
            if schema_type == "map":
                return self._gen_map_decode(
                    value_schema=schema["values"],
                    dest=dest,
                )
            if schema_type == "fixed":
                return self._gen_fixed_decode(
                    size=schema["size"],
                    dest=dest,
                )
            if schema_type == "enum":
                return self._gen_enum_decode(
                    symbols=schema["symbols"],
                    default=schema.get("default"),
                    dest=dest,
                )

        raise NotImplementedError(f"Schema type not implemented: {schema}")

    def _gen_union_decode(self, options: List[SchemaType], dest: AST) -> List[stmt]:

        # Special case: fields like '["null", "long"] which represent an
        # optional field.
        if len(options) == 2:
            if options[0] == "null":
                return self._gen_optional_decode(1, options[1], dest)
            if options[1] == "null":
                return self._gen_optional_decode(0, options[0], dest)

        statements: List[stmt] = []
        # Read a long to figure out which option in the union is chosen.
        idx_var = self.new_variable("union_choice")
        idx_var_dest = Name(id=idx_var, ctx=Store())
        statements.extend(self._gen_primitive_decode("long", idx_var_dest))

        idx_var_ref = Name(id=idx_var, ctx=Load())
        prev_if = None
        for idx, option in enumerate(options):
            if_idx_matches = Compare(
                left=idx_var_ref, ops=[Eq()], comparators=[Constant(idx)]
            )
            if_stmt = If(
                test=if_idx_matches,
                body=self._gen_decode(option, dest),
                orelse=[],
            )

            if prev_if is None:
                statements.append(if_stmt)
            else:
                prev_if.orelse = [if_stmt]
            prev_if = if_stmt
        return statements

    def _gen_optional_decode(
        self, idx: int, schema: SchemaType, dest: AST
    ) -> List[stmt]:
        statements: List[stmt] = []
        is_populated = Compare(
            left=call_decoder("long"),
            ops=[Eq()],
            comparators=[Constant(idx)],
        )

        if isinstance(schema, str) and schema in PRIMITIVES:
            # We can read the value in one line, so we can do something like:
            #  v1["optional_long"] = decode_long(src) if idx == 1 else None

            if_expr = IfExp(
                test=is_populated,
                body=call_decoder(schema),
                orelse=Constant(None),
            )
            assignment = Assign(
                targets=[dest],
                value=if_expr,
            )
            statements.append(assignment)
        else:
            # It takes more than one line to read the value, so we need a real if block.
            if_stmt = If(
                test=is_populated,
                body=self._gen_decode(schema, dest),
                orelse=[Assign(targets=[dest], value=Constant(None))],
            )
            statements.append(if_stmt)
        return statements

    def _gen_record_decode(self, schema: Dict, dest: AST) -> List[stmt]:
        statements: List[stmt] = []

        # Construct a new empty dictionary to hold the record contents.
        value_name = self.new_variable(clean_name(schema["name"]))
        empty_dict = DictLiteral(keys=[], values=[])
        statements.append(
            Assign(
                targets=[Name(id=value_name, ctx=Store())],
                value=empty_dict,
                lineno=0,
            ),
        )
        value_reference = Name(id=value_name, ctx=Load())

        # Write statements to populate all the fields of the record.
        for field in schema["fields"]:
            # Make an AST node that references an entry in the record dict,
            # using the field name as a key.
            field_dest = Subscript(
                value=value_reference,
                slice=Index(value=Constant(value=field["name"])),
                ctx=Store(),
            )

            # Generate the statements required to read that field's type, and to
            # store it into field_dest.
            read_statements = self._gen_decode(field["type"], field_dest)
            statements.extend(read_statements)

        # Now that we have a fully constructed record, write it into the
        # destination provided.
        statements.append(
            Assign(
                targets=[dest],
                value=value_reference,
                lineno=0,
            )
        )
        return statements

    def _gen_array_decode(self, item_schema: SchemaType, dest: AST) -> List[stmt]:
        """
        Returns a sequence of statements which will deserialize an array of given
        type from src into dest.
        """
        statements: List[stmt] = []

        # Create a new list to hold the values we'll read.
        name = "array_"
        if isinstance(item_schema, dict):
            if "name" in item_schema:
                name += item_schema["name"]
            elif "type" in item_schema and isinstance(item_schema["type"], str):
                name += item_schema["type"]
        elif isinstance(item_schema, str):
            name += item_schema
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
        read_statements = self._gen_decode(item_schema, value_dest)
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

        # Finally, assign the list we have constructed into the destination AST node.
        assign_result = Assign(
            targets=[dest],
            value=Name(id=list_varname, ctx=Load()),
        )
        statements.append(assign_result)
        return statements

    def _gen_map_decode(self, value_schema: SchemaType, dest: AST) -> List[stmt]:
        """
        Returns a sequence of statements which will deserialize a map with given
        value type from src into dest.
        """
        statements: List[stmt] = []

        name = "map_"
        if isinstance(value_schema, dict):
            if "name" in value_schema:
                name += value_schema["name"]
            elif "type" in value_schema and isinstance(value_schema["type"], str):
                name += value_schema["type"]
        elif isinstance(value_schema, str):
            name += value_schema
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
        for_each_message.extend(self._gen_decode(value_schema, value_dest))

        statements.extend(self._gen_block_decode(for_each_message))

        # Finally, assign our resulting map to the destination target.
        statements.append(
            Assign(
                targets=[dest],
                value=Name(id=map_varname, ctx=Load()),
            )
        )
        return statements

    def _gen_block_decode(self, for_each_message: List[stmt]) -> List[stmt]:
        """
        Returns a series of statements which represent iteration over an Avro record
        block, like are used for arrays and maps.

        Blocks are a series of records. The block is prefixed with a long that
        indicates the number of records in the block. A zero-length block
        indicates the end of the array or map.

        If a block's count is negative, its absolute value is used, and the
        count is followed immediately by a long block size indicating the number
        of bytes in the block

        for_each_message is a series of statements that will be injected and
        called for every message in the block.
        """
        statements: List[stmt] = []
        decode_block_call = Call(
            func=Name(id="decode_block", ctx=Load()),
            args=[Name(id="src", ctx=Load())],
            keywords=[],
        )

        read_loop = For(
            target=Name(id="_", ctx=Store()),
            iter=decode_block_call,
            body=for_each_message,
            orelse=[],
        )
        statements.append(read_loop)
        return statements

    def _gen_enum_decode(
        self, symbols: List[str], default: Optional[str], dest: AST
    ) -> List[stmt]:
        statements: List[stmt] = []

        # Construct a literal dictionary which maps integers to symbols.
        enum_map = DictLiteral(keys=[], values=[])
        for i, sym in enumerate(symbols):
            enum_map.keys.append(Constant(value=i))
            enum_map.values.append(Constant(value=sym))

        # Call dict.get(decode_long(src), default=default)
        call = Call(
            func=Attribute(
                value=enum_map,
                attr="get",
                ctx=Load(),
            ),
            args=[call_decoder("long")],
            keywords=[],
        )

        if default is not None:
            call.args.append(Constant(value=default))

        statements.append(
            Assign(
                targets=[dest],
                value=call,
            )
        )
        return statements

    def _gen_fixed_decode(self, size: int, dest: AST) -> List[stmt]:
        # Call dest = src.read(size).
        read = Call(
            func=Attribute(value=Name(id="src", ctx=Load()), attr="read", ctx=Load()),
            args=[Constant(value=size)],
            keywords=[],
        )
        return [
            Assign(
                targets=[dest],
                value=read,
            )
        ]

    def _gen_primitive_decode(self, primitive_type: str, dest: AST) -> List[stmt]:
        """
        Returns a sequence of statements which will deserialize a given primitive
        type from src into dest.
        """
        if primitive_type == "null":
            statement = Assign(
                targets=[dest],
                value=Constant(value=None),
            )
            return [statement]

        statement = Assign(
            targets=[dest],
            value=call_decoder(primitive_type),
        )
        return [statement]

    def _gen_logical_decode(self, schema: Dict[str, Any], dest: AST) -> List[stmt]:
        src = Name(id="src", ctx=Load())
        try:
            lt = schema["logicalType"]
            t = schema["type"]
            call = None
            if lt == "decimal" and t == "bytes":
                prec = Constant(value=schema["precision"])
                scale = Constant(value=schema.get("scale", 0))
                call = func_call("decode_decimal_bytes", [src, prec, scale])
            elif lt == "decimal" and t == "fixed":
                size = Constant(value=schema["size"])
                prec = Constant(value=schema["precision"])
                scale = Constant(value=schema.get("scale", 0))
                call = func_call("decode_decimal_fixed", [src, size, prec, scale])
            elif lt == "uuid" and t == "string":
                call = func_call("decode_uuid", [src])
            elif lt == "date" and t == "int":
                call = func_call("decode_date", [src])
            elif lt == "time-millis" and t == "int":
                call = func_call("decode_time_millis", [src])
            elif lt == "time-micros" and t == "long":
                call = func_call("decode_time_micros", [src])
            elif lt == "timestamp-millis" and t == "long":
                call = func_call("decode_timestamp_millis", [src])
            elif lt == "timestamp-micros" and t == "long":
                call = func_call("decode_timestamp_micros", [src])
            else:
                raise LogicalTypeError("unknown logical type")

            return [Assign(targets=[dest], value=call)]
        except LogicalTypeError:
            # If a logical type is unknown, or invalid, then we should fall back
            # and use the underlying Avro type. We do this by clearing the
            # logicalType field of the schema and calling self._gen_decode.
            schema = schema.copy()
            del schema["logicalType"]
            return self._gen_decode(schema, dest)

    def _gen_recursive_decode_call(
        self, recursive_type_name: str, dest: AST
    ) -> List[stmt]:
        funcname = self._decoder_name(recursive_type_name)
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
