from __future__ import annotations

import functools
import uuid
import json
from typing import Optional, List, Dict, Mapping, Set, Union, Any, Callable
import abc
import copy
from dataclasses import dataclass, field
import re
import warnings
from avroc.avro_common import PRIMITIVES
from avroc.util import SchemaType
from avroc.runtime import typetest
from collections import defaultdict

name_regex = re.compile("[A-Za-z_][A-Za-z0-9_]*$")

# Sentinel value used for representing `"default": null`
NullDefault = object()


def validate(schema: Schema):
    walk(schema, lambda x: x.validate())


def valid_name(s: str) -> bool:
    return name_regex.match(s) is not None


def valid_qualified_name(s: str) -> bool:
    parts = s.split(".")
    for p in parts:
        if not valid_name(p):
            return False
    return True


def load_schema(obj: Union[dict, str, list]) -> Schema:
    """
    Create a Schema given a JSON value (a dictionary, list, or plain string).
    """
    schema = schema_from_obj(obj, {}, "")
    validate(schema)
    return schema


def dump_schema(s: Schema) -> str:
    return json.dumps(s.to_dict())


def schema_from_obj(
    obj: Union[str, list, dict],
    names: Dict[str, NamedSchema],
    parent_namespace: str = "",
) -> Schema:
    """
    Low-level constructor to create a new schema, while keeping track of naming of types.
    """
    if isinstance(obj, str):
        if obj in PRIMITIVES:
            return PrimitiveSchema.from_str(obj)
        else:
            return NamedSchemaReference.from_str(obj, names, parent_namespace)
    if isinstance(obj, list):
        return UnionSchema.from_list(obj, names, parent_namespace)
    if isinstance(obj, dict):
        if "type" not in obj:
            raise SchemaValidationError(f"missing 'type' field: {obj}")
        obj_type = obj["type"]
        if "logicalType" in obj:
            logical_type = obj["logicalType"]
            schema = _logical_type_mapping.get((logical_type, obj_type))
            if schema is not None:
                return schema.from_dict(obj, names, parent_namespace)
            else:
                warnings.warn(
                    SchemaValidationWarning(
                        f"unknown logical type: {logical_type} annotating {obj_type}"
                    )
                )
                # Fallthrough to non-logical behavior.
        schema = _standard_type_mappings.get(obj_type)
        if schema is None:
            # If it's not standard, maybe it's a name.
            schema = NamedSchemaReference
        return schema.from_dict(obj, names, parent_namespace)
    raise ValueError(f"obj must be a list, str, or dict")


def walk(s: Schema, f: Callable[[Schema], None]) -> None:
    # Visit every schema node defined in a schema, in depth-first fashion.
    to_visit = [s]
    seen_names = set()
    i = 0
    while True:
        i += 1
        if i >= 10000:
            raise RecursionError("maximum depth exceeded in schema walk")

        if len(to_visit) == 0:
            return

        next_node = to_visit.pop()
        f(next_node)

        if isinstance(next_node, NamedSchema):
            seen_names.add(next_node.fullname())

        if isinstance(next_node, ContainerSchema):
            children = next_node.children()
            for child in children:
                # Skip any children we have already visited
                if isinstance(child, NamedSchema) and child.fullname() in seen_names:
                    continue
                to_visit.append(child)


def schemas_match(writer: Schema, reader: Schema) -> bool:
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
    # Dereference named types.
    if isinstance(writer, NamedSchemaReference):
        writer = writer.referenced_schema
    if isinstance(reader, NamedSchemaReference):
        reader = reader.referenced_schema

    # Special case for logical decimal types. From the spec:
    #
    #   For the purposes of schema resolution, two schemas that are decimal
    #   logical types match if their scales and precisions match.
    if isinstance(writer, (DecimalBytesSchema, DecimalFixedSchema)) and isinstance(
        reader, (DecimalBytesSchema, DecimalFixedSchema)
    ):
        return writer.scale == reader.scale and writer.precision == reader.precision

    if isinstance(writer, ArraySchema) and isinstance(reader, ArraySchema):
        return schemas_match(writer.items, reader.items)

    if isinstance(writer, MapSchema) and isinstance(reader, MapSchema):
        return schemas_match(writer.values, reader.values)

    if isinstance(writer, EnumSchema) and isinstance(reader, EnumSchema):
        return reader.name_matches(writer)

    if isinstance(writer, FixedSchema) and isinstance(reader, FixedSchema):
        return reader.name_matches(writer) and writer.size == reader.size

    if isinstance(writer, RecordSchema) and isinstance(reader, RecordSchema):
        return reader.name_matches(writer)

    if isinstance(writer, UnionSchema) or isinstance(reader, UnionSchema):
        return True

    if isinstance(writer, PrimitiveSchema) and isinstance(reader, PrimitiveSchema):
        if writer.type == reader.type:
            return True
        return writer.promotable_to(reader)
    return False


@dataclass
class Schema:
    type: str
    default: Optional[Any]

    # Mapping of fully-qualified names to their definitions. This mapping is
    # shared by all schemas that are loaded together.
    _names: Dict[str, NamedSchema] = field(compare=False, repr=False)

    def validate(self):
        if self.default is not None:
            if not self.default_is_valid():
                raise SchemaValidationError("default value is invalid for this type")

    def default_is_valid(self) -> bool:
        raise NotImplementedError("should be implemented by inheriting classes")

    def to_dict(self) -> dict:
        d = {
            "type": self.type,
        }
        if self.default is not None:
            d["default"] = self.default
        return d


@dataclass
class PrimitiveSchema(Schema):
    def validate(self):
        super(PrimitiveSchema, self).validate()
        if self.type not in PRIMITIVES:
            raise SchemaValidationError(f"{type} is not a primitive type")

    def default_is_valid(self) -> bool:
        if self.type == "null":
            return self.default is NullDefault
        elif self.type == "boolean":
            return self.default is False or self.default is True
        elif self.type == "int" or self.type == "long":
            return isinstance(self.default, int)
        elif self.type == "float" or self.type == "double":
            return isinstance(self.default, (int, float))
        elif self.type == "string" or self.type == "bytes":
            return isinstance(self.default, str)
        raise ValueError(f"unexpected type for primitive: {self.type}")

    @classmethod
    def from_str(cls, val: str) -> PrimitiveSchema:
        return PrimitiveSchema(type=val, default=None, _names={})

    @classmethod
    def from_dict(
        cls, obj: Dict, names: dict, parent_namespace: str
    ) -> PrimitiveSchema:
        ps = PrimitiveSchema(
            type=obj["type"],
            default=obj.get("default"),
            _names=names,
        )

        if ps.type == "null":
            if "default" in obj and obj["default"] is None:
                ps.default = NullDefault
        return ps

    def to_dict(self):
        d = {
            "type": self.type,
        }
        if self.default is not None:
            if self.default is NullDefault:
                d["default"] = None
            else:
                d["default"] = self.default
        return d

    def promotable_to(self, other: PrimitiveSchema) -> bool:
        promotions = {
            "int": {"long", "float", "double"},
            "long": {"float", "double"},
            "float": {"double"},
            "bytes": {"string"},
            "string": {"bytes"},
        }
        if self.type not in promotions:
            return False
        return other.type in promotions[self.type]


class ContainerSchema(abc.ABC):
    def children(self) -> List[Schema]:
        raise NotImplementedError("should be implemented by subclasses")


@dataclass
class NamedSchema(Schema):
    name: str
    namespace: Optional[str]
    aliases: Optional[List[str]]
    parent_namespace: str = field(compare=False)

    def __post_init__(self):
        """
        Record self's name (and all aliases) into self._names, making sure that the
        name does not already exist. Should only be called once when the Schema
        is being initialized.
        """
        if self.fullname() in self._names:
            raise SchemaValidationError(f"name {self.fullname()} is used twice")
        for a in self.fullaliases():
            if a in self._names and self._names[a] != self:
                raise SchemaValidationError(
                    f"{self.fullname()} claims {a} is an alias, but it already exists as a different definition as {self._names[a]}"
                )
            self._names[a] = self
        self._names[self.fullname()] = self

    def validate(self) -> bool:
        super(NamedSchema, self).validate()

        if not valid_qualified_name(self.name):
            raise SchemaValidationError(f"name {self.name} has illegal characters")
        if self.namespace is not None:
            if not valid_qualified_name(self.namespace):
                raise SchemaValidationError(
                    f"namespace {self.namespace} has illegal characters"
                )
        if self.aliases is not None:
            for alias in self.aliases:
                if not valid_qualified_name(alias):
                    raise SchemaValidationError(f"alias {alias} has illegal characters")

        return True

    def namespace_for_children(self) -> Optional[str]:
        if "." in self.name:
            return self.name.rsplit(".", 1)[0]
        elif self.namespace is not None and self.namespace != "":
            return self.namespace
        elif self.parent_namespace is not None and self.parent_namespace != "":
            return self.parent_namespace
        else:
            return None

    def fullaliases(self) -> List[str]:
        # Return a list of aliases which are fully qualified against the
        # NamedSchema's namespace.
        if self.aliases is None:
            return []
        # Aliases are relative to the namespace of self.
        ns: str
        if "." in self.name:
            ns = self.name.rsplit(".", 1)[0]
        elif self.namespace != "" and self.namespace is not None:
            ns = self.namespace
        elif self.parent_namespace != "" and self.parent_namespace is not None:
            ns = self.parent_namespace
        else:
            # self is in the null namespace; aliases cannot be qualified any
            # better than they already are.
            return self.aliases

        fully_qualified_aliases: List[str] = []
        for a in self.aliases:
            if "." in a:
                fully_qualified_aliases.append(a)
            else:
                fully_qualified_aliases.append(f"{ns}.{a}")
        return fully_qualified_aliases

    def fullname(self) -> str:
        if "." in self.name:
            return self.name
        elif self.namespace is not None and self.namespace != "":
            return f"{self.namespace}.{self.name}"
        elif self.parent_namespace is not None and self.parent_namespace != "":
            return f"{self.parent_namespace}.{self.name}"
        else:
            return self.name

    def to_dict(self) -> dict:
        d = Schema.to_dict(self)
        d["name"] = self.name
        if self.namespace is not None:
            d["namespace"] = self.namespace
        if self.default is not None:
            d["default"] = self.default
        if self.aliases is not None:
            d.aliases = self.aliases
        return d

    def name_matches(self, other: NamedSchema) -> bool:
        """
        Returns true if self's unqualified name is the same as other's unqualified
        name, or if any of self's aliases are the same as other's unqualified
        name.
        """
        if self.name == other.name:
            return True
        if self.aliases is not None:
            for a in self.aliases:
                if a == other.name:
                    return True
        return False


@dataclass(eq=False)
class NamedSchemaReference(Schema):
    """ A reference to a named type by name"""

    referenced_name: str  # The name used
    referenced_schema: NamedSchema

    @classmethod
    def from_str(
        cls, s: str, names: Dict[str, NamedSchema], parent_namespace: str
    ) -> NamedSchemaReference:

        if "." not in s:
            if parent_namespace != "":
                s = f"{parent_namespace}.{s}"

        if s not in names:
            raise SchemaValidationError(f"unknown name {s}")
        return NamedSchemaReference(
            type=names[s].type,
            default=None,
            _names=names,
            referenced_name=s,
            referenced_schema=names[s],
        )

    @classmethod
    def from_dict(
        cls, obj: dict, names: Dict[str, NamedSchema], parent_namespace: str
    ) -> NamedSchemaReference:
        name = obj["type"]
        if name not in names:
            raise SchemaValidationError(f"unknown name {name}")
        return NamedSchemaReference(
            type=names[name].type,
            default=obj.get("default"),
            _names=names,
            referenced_name=name,
            referenced_schema=names[name],
        )

    def to_dict(self) -> dict:
        d = {"type": self.referenced_name}
        if self.default is not None:
            d["default"] = self.default
        return d

    def default_is_valid(self) -> bool:
        referent_copy = copy.deepcopy(self.referenced_schema)
        referent_copy.default = self.default
        return referent_copy.default_is_valid()

    def __eq__(self, other):
        if not isinstance(other, NamedSchemaReference):
            return False
        return self.referenced_schema.fullname() == other.referenced_schema.fullname()


@dataclass
class RecordSchema(NamedSchema, ContainerSchema):
    fields: List[FieldDefinition]
    doc: Optional[str]
    recursive: bool = field(default=False, init=False)

    def validate(self):
        super(RecordSchema, self).validate()

        if not isinstance(self.fields, list):
            raise SchemaValidationError("fields should be a list")

        used_field_names = set()
        for field in self.fields:
            if field.default is not None and not field.default_is_valid():
                raise SchemaValidationError(
                    f"field {field.name} has an invalid default"
                )

            if field.name in used_field_names:
                raise SchemaValidationError(f"field name {field.name} is used twice")
            used_field_names.add(field.name)

    def children(self) -> List[Schema]:
        l = []
        for field in self.fields:
            l.append(field.type)
        return l

    @classmethod
    def from_dict(
        cls, obj: Dict, names: Dict[str, NamedSchema], parent_namespace: str = ""
    ) -> RecordSchema:
        rs = RecordSchema(
            type="record",
            name=obj["name"],
            namespace=obj.get("namespace"),
            parent_namespace=parent_namespace,
            _names=names,
            default=obj.get("default"),
            aliases=obj.get("aliases"),
            doc=obj.get("doc"),
            fields=[],
        )
        field_namespace = rs.namespace_for_children() or ""
        for field in obj["fields"]:
            field_def = FieldDefinition.from_dict(field, names, field_namespace)
            rs.fields.append(field_def)
        return rs

    def to_dict(self) -> dict:
        d = NamedSchema.to_dict(self)
        d["fields"] = [f.to_dict() for f in self.fields]
        if self.doc is not None:
            d["doc"] = self.doc
        return d

    def default_is_valid(self) -> bool:
        if not isinstance(self.default, dict):
            return False

        fields_by_name = {}
        for field in self.fields:
            fields_by_name[field.name] = field

            if field.aliases is not None:
                for a in field.aliases:
                    fields_by_name[a] = field

            field_has_value = False
            if field.default is None:
                # Always got a value if the field has a default
                field_has_value = True
            elif field.name in self.default:
                # The field is set, so its OK
                field_has_value = True
            elif field.aliases is not None:
                # Maybe the field is present, but under an alias
                for a in field.aliases:
                    if a in self.default:
                        field_has_value = True
            if not field_has_value:
                return False

        for field, value in self.default.items():
            if field not in fields_by_name:
                # Unexpected field
                return False
            field_type = fields_by_name[field]
            field_schema_copy = copy.deepcopy(field_type.type)
            field_schema_copy.default = value
            if not field_schema_copy.default_is_valid():
                return False
        return True


@dataclass
class FieldDefinition:
    name: str
    type: Schema
    doc: Optional[str]
    default: Optional[Any]
    order: Optional[str]
    aliases: Optional[List[str]]

    def default_is_valid(self) -> bool:
        field_schema_copy = copy.deepcopy(self.type)
        field_schema_copy.default = self.default
        return field_schema_copy.default_is_valid()

    @classmethod
    def from_dict(
        cls, obj, names: Dict[str, NamedSchema], parent_namespace: str
    ) -> FieldDefinition:
        fd = FieldDefinition(
            name=obj["name"],
            type=schema_from_obj(obj["type"], names, parent_namespace),
            doc=obj.get("doc"),
            default=obj.get("default"),
            order=obj.get("order"),
            aliases=obj.get("aliases"),
        )

        if "default" in obj and obj["default"] is None:
            fd.default = NullDefault

        return fd

    def to_dict(self) -> dict:
        d = {
            "type": self.type.to_dict(),
            "name": self.name,
        }
        if self.doc is not None:
            d["doc"] = self.doc
        if self.default is not None:
            if self.default is NullDefault:
                d["default"] = None
            else:
                d["default"] = self.default
        if self.order is not None:
            d["order"] = self.order
        if self.aliases is not None:
            d["aliases"] = self.aliases
        return d


@dataclass
class EnumSchema(NamedSchema):
    symbols: List[str]
    doc: Optional[str]
    default: Optional[str]

    def validate(self):
        super(EnumSchema, self).validate()

        syms_seen = set()
        for sym in self.symbols:
            if not valid_name(sym):
                raise SchemaValidationError("symbol {sym} contains illegal characters")
            if sym in syms_seen:
                raise SchemaValidationError("symbol {sym} is defined more than once")
            syms_seen.add(sym)

        if self.default is not None:
            if self.default not in self.symbols:
                raise SchemaValidationError(
                    "default symbol {sym} is in list of symbols"
                )

        return True

    def default_is_valid(self) -> bool:
        return self.default in self.symbols

    @classmethod
    def from_dict(
        cls, obj, names: Dict[str, NamedSchema], parent_namespace: str = ""
    ) -> EnumSchema:
        es = EnumSchema(
            type="enum",
            name=obj["name"],
            symbols=obj["symbols"],
            namespace=obj.get("namespace"),
            parent_namespace=parent_namespace,
            aliases=obj.get("aliases"),
            doc=obj.get("doc"),
            default=obj.get("default"),
            _names=names,
        )
        return es

    def to_dict(self) -> dict:
        d = NamedSchema.to_dict(self)
        d["symbols"] = self.symbols
        if self.doc is not None:
            d["doc"] = self.doc


@dataclass
class FixedSchema(NamedSchema):
    size: int

    def validate(self):
        super(FixedSchema, self).validate()
        if self.size < 0:
            raise SchemaValidationError("size cannot be negative")

    def default_is_valid(self) -> bool:
        if self.default is None:
            return True
        return len(self.default.encode()) == self.size

    @classmethod
    def from_dict(
        cls, obj, names: Dict[str, NamedSchema], parent_namespace: str = ""
    ) -> FixedSchema:
        fs = FixedSchema(
            type="fixed",
            name=obj["name"],
            size=obj["size"],
            namespace=obj.get("namespace"),
            parent_namespace=parent_namespace,
            aliases=obj.get("aliases"),
            default=obj.get("default"),
            _names=names,
        )
        return fs

    def to_dict(self) -> dict:
        d = NamedSchema.to_dict(self)
        d["size"] = self.size


@dataclass
class ArraySchema(Schema, ContainerSchema):
    items: Schema
    default: Optional[List[Any]]

    def children(self) -> List[Schema]:
        return [self.items]

    def default_is_valid(self) -> bool:
        if self.default is None:
            return True
        # Copy the item schema, and then modify it to change the 'default' value
        # for each value in the list, and test if it's valid.
        item_schema_copy = copy.deepcopy(self.items)
        for obj in self.default:
            item_schema_copy.default = obj
            if not item_schema_copy.default_is_valid():
                return False
        return True

    @classmethod
    def from_dict(
        cls, obj, names: Dict[str, NamedSchema], parent_namespace: str = ""
    ) -> ArraySchema:
        return ArraySchema(
            type="array",
            items=schema_from_obj(obj["items"], names, parent_namespace),
            default=obj.get("default"),
            _names=names,
        )

    def to_dict(self) -> dict:
        d = {"type": "array", "items": self.items.to_dict()}
        if self.default is not None:
            d["default"] = self.default
        return d


@dataclass
class MapSchema(Schema, ContainerSchema):
    values: Schema
    default: Optional[Dict[str, Any]]

    def children(self) -> List[Schema]:
        return [self.values]

    def validate(self):
        super(MapSchema, self).validate()
        self.values.validate()

    def default_is_valid(self) -> bool:
        if self.default is None:
            return True
        # Copy the values schema, and then modify it to change the 'default' value
        # for each value in the map, and test if it's valid.
        values_schema_copy = copy.deepcopy(self.values)
        for obj in self.default.values():
            values_schema_copy.default = obj
            if not values_schema_copy.default_is_valid():
                return False
        return True

    @classmethod
    def from_dict(
        cls,
        obj,
        names: Dict[str, NamedSchema],
        parent_namespace: str = "",
    ) -> MapSchema:
        return MapSchema(
            type="map",
            values=schema_from_obj(obj["values"], names, parent_namespace),
            default=obj.get("default"),
            _names=names,
        )

    def to_dict(self) -> dict:
        d = {
            "type": "map",
            "values": self.values.to_dict(),
        }
        if self.default is not None:
            d["default"] = self.default
        return d


@dataclass
class UnionSchema(Schema, ContainerSchema):
    options: List[Schema]

    def children(self) -> List[Schema]:
        return self.options

    def validate(self):
        super(UnionSchema, self).validate()
        if len(self.options) < 2:
            raise SchemaValidationError("unions must have at least two elements")
        for o in self.options:
            o.validate()

    def default_is_valid(self) -> bool:
        # A Union's default must match the first schema in the union.
        schema_copy = copy.deepcopy(self.options[0])
        schema_copy.default = self.default
        return schema_copy.default_is_valid()

    @classmethod
    def from_list(
        cls, l, names: Dict[str, NamedSchema], parent_namespace: str = ""
    ) -> UnionSchema:
        us = UnionSchema(
            type="union",
            options=[],
            default=None,
            _names=names,
        )
        for option in l:
            us.options.append(schema_from_obj(option, names, parent_namespace))
        return us

    def to_list(self) -> list:
        return [o.to_dict() for o in self.options]

    def to_dict(self) -> list:
        return self.to_list()

    def is_nullable(self) -> bool:
        """
        Returns true if the union contains 'null' as an option.
        """
        for o in self.options:
            if isinstance(o, PrimitiveSchema) and o.type == "null":
                return True
        return False


@dataclass
class LogicalSchema:
    logical_type: str


@dataclass
class DecimalBytesSchema(LogicalSchema, PrimitiveSchema):
    precision: int
    scale: Optional[int]

    @classmethod
    def from_dict(cls, obj, names: Dict[str, Schema], parent_namespace: str):
        dbs = DecimalBytesSchema(
            type=obj["type"],
            logical_type="decimal",
            default=obj.get("default", None),
            precision=obj["precision"],
            scale=obj.get("scale"),
            _names=names,
        )
        return dbs

    def to_dict(self) -> dict:
        d = {
            "type": "bytes",
            "logicalType": "decimal",
            "precision": self.precision,
        }
        if self.default is not None:
            d["default"] = self.default
        if self.scale is not None:
            d["scale"] = self.scale
        return d


@dataclass
class DecimalFixedSchema(LogicalSchema, FixedSchema):
    precision: int
    scale: Optional[int]

    @classmethod
    def from_dict(
        cls, obj, names: Dict[str, NamedSchema], parent_namespace: str = ""
    ) -> DecimalFixedSchema:
        dbs = DecimalFixedSchema(
            type=obj["type"],
            default=obj.get("default", None),
            logical_type="decimal",
            name=obj["name"],
            namespace=obj.get("namespace"),
            parent_namespace=parent_namespace,
            aliases=obj.get("aliases"),
            size=obj["size"],
            precision=obj["precision"],
            scale=obj.get("scale"),
            _names=names,
        )
        return dbs

    def to_dict(self) -> dict:
        d = FixedSchema.to_dict(self)
        d["logicalType"] = "decimal"
        d["precision"] = (self.precision,)
        if self.scale is not None:
            d["scale"] = self.scale
        return d


@dataclass
class UUIDSchema(LogicalSchema, PrimitiveSchema):
    def validate(self):
        if self.type != "string":
            raise SchemaValidationError("UUID must be a string")

    def default_is_valid(self) -> bool:
        if not isinstance(self.default, str):
            return False
        try:
            uuid.UUID(self.default)
            return True
        except:
            return False

    @classmethod
    def from_dict(cls, obj, names, parent_namespace) -> UUIDSchema:
        return UUIDSchema(
            type=obj["type"],
            logical_type="uuid",
            default=obj.get("default"),
            _names=names,
        )

    def to_dict(self) -> dict:
        return {
            "type": "string",
            "logicalType": "uuid",
        }


@dataclass
class DateSchema(LogicalSchema, PrimitiveSchema):
    def validate(self):
        if self.type != "int":
            raise SchemaValidationError("date must be an int")

    def default_is_valid(self) -> bool:
        if not isinstance(self.default, int):
            return False
        return True

    @classmethod
    def from_dict(cls, obj, names, parent_namespace) -> DateSchema:
        return DateSchema(
            type=obj["type"],
            logical_type="date",
            default=obj.get("default"),
            _names=names,
        )

    def to_dict(self) -> dict:
        return {
            "type": "int",
            "logicalType": "date",
        }


@dataclass
class TimeMillisSchema(LogicalSchema, PrimitiveSchema):
    def validate(self):
        if self.type != "int":
            raise SchemaValidationError("time-millis must be an int")

    @classmethod
    def from_dict(
        cls, obj, names: Dict[str, Schema], parent_namespace: str
    ) -> TimeMillisSchema:
        return TimeMillisSchema(
            type=obj["type"],
            logical_type="time-millis",
            default=obj.get("default"),
            _names=names,
        )

    def to_dict(self) -> dict:
        return {
            "type": "int",
            "logicalType": "time-millis",
        }


@dataclass
class TimeMicrosSchema(LogicalSchema, PrimitiveSchema):
    def validate(self):
        if self.type != "long":
            raise SchemaValidationError("time-micros must be a long")

    @classmethod
    def from_dict(cls, obj, names, parent_namespace) -> TimeMicrosSchema:
        return TimeMicrosSchema(
            type=obj["type"],
            logical_type="time-micros",
            default=obj.get("default"),
            _names=names,
        )

    def to_dict(self) -> dict:
        return {
            "type": "long",
            "logicalType": "time-micros",
        }


@dataclass
class TimestampMillisSchema(LogicalSchema, PrimitiveSchema):
    def validate(self):
        if self.type != "long":
            raise SchemaValidationError("timestamp-millis must be a long")

    @classmethod
    def from_dict(cls, obj, names, parent_namespace) -> TimestampMillisSchema:
        return TimestampMillisSchema(
            type=obj["type"],
            logical_type="timestamp-millis",
            default=obj.get("default"),
            _names=names,
        )

    def to_dict(self) -> dict:
        return {
            "type": "long",
            "logicalType": "timestamp-millis",
        }


@dataclass
class TimestampMicrosSchema(LogicalSchema, PrimitiveSchema):
    def validate(self):
        if self.type != "long":
            raise SchemaValidationError("timestamp-micros must be a long")

    @classmethod
    def from_dict(cls, obj, names, parent_namespace) -> TimestampMicrosSchema:
        return TimestampMicrosSchema(
            type=obj["type"],
            logical_type="timestamp-micros",
            default=obj.get("default"),
            _names=names,
        )

    def to_dict(self) -> dict:
        return {
            "type": "long",
            "logicalType": "timestamp-micros",
        }


@dataclass
class DurationSchema(LogicalSchema, FixedSchema):
    def validate(self):
        super(DurationSchema, self).validate()
        if self.size != 12:
            raise SchemaValidationError("duration must have size of 12")

    @classmethod
    def from_dict(cls, obj, names, parent_namespace) -> DurationSchema:
        return DurationSchema(
            type=obj["type"],
            logical_type="duration",
            size=obj["size"],
            name=obj["name"],
            namespace=obj.get("namespace"),
            parent_namespace=parent_namespace,
            aliases=obj.get("aliases"),
            default=obj.get("default"),
            _names=names,
        )

    def to_dict(self) -> dict:
        d = FixedSchema.to_dict(self)
        d["logicalType"] = "duration"
        return d


class SchemaValidationError(Exception):
    pass


class SchemaValidationWarning(UserWarning):
    pass


def gather_named_types(schema: Schema) -> Dict[str, NamedSchema]:
    """
    Traverse the schema to find the definitions of all named types. Returns a
    dictionary, mapping fully-qualified type names to their definitions. Aliases
    are included (in fully-qualified form).
    """
    result = {}

    def visitor(s: Schema) -> None:
        if not isinstance(s, NamedSchema):
            return
        if s.fullname() in result:
            return
        result[s.fullname()] = s
        for alias in s.fullaliases():
            result[alias] = s

    walk(schema, visitor)
    return result


# Mapping of (logicalType, type) pairs to the associated schema
_logical_type_mapping = {
    ("decimal", "bytes"): DecimalBytesSchema,
    ("decimal", "fixed"): DecimalFixedSchema,
    ("uuid", "string"): UUIDSchema,
    ("date", "int"): DateSchema,
    ("time-millis", "int"): TimeMillisSchema,
    ("time-micros", "long"): TimeMicrosSchema,
    ("timestamp-millis", "long"): TimestampMillisSchema,
    ("timestamp-micros", "long"): TimestampMicrosSchema,
    ("duration", "fixed"): DurationSchema,
}

# Mapping of type to associated schema
_standard_type_mappings = {
    "int": PrimitiveSchema,
    "long": PrimitiveSchema,
    "bytes": PrimitiveSchema,
    "string": PrimitiveSchema,
    "float": PrimitiveSchema,
    "double": PrimitiveSchema,
    "boolean": PrimitiveSchema,
    "null": PrimitiveSchema,
    "array": ArraySchema,
    "map": MapSchema,
    "enum": EnumSchema,
    "fixed": FixedSchema,
    "record": RecordSchema,
    "error": RecordSchema,
}
