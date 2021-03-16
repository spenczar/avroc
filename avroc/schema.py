from __future__ import annotations

from typing import Optional, List, Dict, Mapping, Set

import copy
from dataclasses import dataclass, field
import re
import warnings
from avroc.avro_common import PRIMITIVES
from avroc.util import SchemaType
from avroc.runtime import typetest
from collections import defaultdict

name_regex = re.compile("[A-Za-z_][A-Za-z0-9_]*$")


def validate(schema: SchemaType):
    load_schema(schema)


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
    schema = schema_from_obj(obj, {}, None)
    schema.validate()
    return schema


def schema_from_obj(obj: Union[str, list, dict], names: Dict[str, Schema], parent_namespace: str="") -> Schema:
    """
    Low-level constructor to create a new schema, while keeping track of naming of types.
    """
    if isinstance(obj, str):
        if obj in PRIMITIVES:
            return PrimitiveSchema.from_str(obj)
        else:
            if obj in names:
                return names[obj]
        raise SchemaValidationError(f"unknown name {obj}")
    if isinstance(obj, list):
        return UnionSchema.from_list(obj, names, parent_namespace)
    if isinstance(obj, dict):
        if 'type' not in obj:
            raise SchemaValidationError(f"missing 'type' field: {obj}")
        if obj['type'] in PRIMITIVES:
            return PrimitiveSchema.from_dict(obj)
        if obj["type"] == "array":
            return ArraySchema.from_dict(obj, names, parent_namespace)
        if obj["type"] == "map":
            return MapSchema.from_dict(obj, names, parent_namespace)
        if obj["type"] == "enum":
            return EnumSchema.from_dict(obj, names, parent_namespace)
        if obj["type"] == "fixed":
            return FixedSchema.from_dict(obj, names, parent_namespace)
        if obj["type"] == "record":
            return RecordSchema.from_dict(obj, names, parent_namespace)
        if obj["type"] in names:
            return names[obj["type"]]
        raise SchemaValidationError(f"unknown schema type {obj['type']}")
    raise ValueError(f"obj must be a list, str, or dict")


def walk(s: Schema, f: Callable[[Schema], bool]) -> bool:
    # Visit every schema node defined in a schema, in breadth-first fashion.
    keep_going = f(s)
    if not keep_going:
        return
    if isinstance(s, UnionSchema):
        for sub_schema in s:
            if not walk(sub_schema, f):
                return False
        return True
    if isinstance(s, RecordSchema):
        for field in s.fields:
            if not walk(field.type, f):
                return False
        return True
    if isinstance(s, MapSchema):
        return walk(s.values, f)
    if isinstance(s, ArraySchema):
        return walk(s.items, f)

@dataclass
class Schema:
    type: str
    default: Optional[Any]

    # Mapping of fully-qualified names to their definitions. This mapping is
    # shared by all schemas that are loaded together.
    _names: Dict[str, Schema] = field(compare=False, repr=False)

    def validate(self):
        if self.default is not None:
            if not self.default_is_valid():
                raise SchemaValidationError("default value is invalid for this type")

    def default_is_valid(self) -> bool:
        raise NotImplementedError("should be implemented by inheriting classes")

@dataclass
class PrimitiveSchema(Schema):
    def validate(self):
        super(PrimitiveSchema, self).validate()
        if self.type not in PRIMITIVES:
            raise SchemaValidationError(f'{type} is not a primitive type')

    def default_is_valid(self) -> bool:
        if self.type == "null":
            return self.default is None
        elif self.type == "boolean":
            return self.default is False or self.default is True
        elif self.type == "int" or self.type == "long":
            return isinstance(self.default, int)
        elif self.type == "float" or self.type == "double":
            return isinstance(self.default, (int, float))
        elif self.type == "string" or self.type == "bytes":
            return isinstance(self.default, str)

    @classmethod
    def from_str(cls, val: str) -> PrimitiveSchema:
        return PrimitiveSchema(type=val, default=None, _names={})

    @classmethod
    def from_dict(cls, obj: Dict) -> PrimitiveSchema:
        return PrimitiveSchema(type=obj["type"], default=obj.get("default"), _names={})

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
                raise SchemaValidationError(f"{self.fullname()} claims {a} is an alias, but it already exists as a different definition as {self._names[a]}")
            self._names[a] = self
        self._names[self.fullname()] = self


    def validate(self) -> bool:
        super(NamedSchema, self).validate()

        if not valid_qualified_name(self.name):
            raise SchemaValidationError(f'name {self.name} has illegal characters')
        if self.namespace is not None:
            if not valid_qualified_name(self.namespace):
                raise SchemaValidationError(f'namespace {self.namespace} has illegal characters')
        if self.aliases is not None:
            for alias in self.aliases:
                if not valid_qualified_name(alias):
                    raise SchemaValidationError(f'alias {alias} has illegal characters')

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
        if self.aliases is None: return []
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


@dataclass
class RecordSchema(NamedSchema):
    fields: List[FieldDefinition]

    def validate(self):
        super(RecordSchema, self).validate()

        if not isinstance(self.fields, list):
            raise SchemaValidationError("fields should be a list")

        used_field_names = set()
        for field in self.fields:
            field.type.validate()
            if field.default is not None and not field.default_is_valid():
                raise SchemaValidationError(f"field {field.name} has an invalid default")

            if field.name in used_field_names:
                raise SchemaValidationError(f"field name {field.name} is used twice")
            used_field_names.add(field.name)

    @classmethod
    def from_dict(cls, obj: Dict, names: Dict[str, Schema], parent_namespace: str="") -> RecordSchema:
        rs = RecordSchema(
            type="record",
            name=obj["name"],
            namespace=obj.get("namespace"),
            parent_namespace=parent_namespace,
            _names=names,
            default=obj.get("default"),
            aliases=obj.get("aliases"),
            fields=[],
        )
        field_namespace = rs.namespace_for_children()
        for field in obj["fields"]:
            field_def = FieldDefinition.from_dict(field, names, field_namespace)
            rs.fields.append(field_def)
        return rs

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
            elif field in self.default:
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

    def validate(self):
        self.type.validate()

    def default_is_valid(self) -> bool:
        field_schema_copy = copy.deepcopy(self.type)
        field_schema_copy.default = self.default
        return field_schema_copy.default_is_valid()

    @classmethod
    def from_dict(cls, obj, names: Dict[str, Schema], parent_namespace: Optional[str]=None) -> FieldDefinition:
        return FieldDefinition(
            name=obj["name"],
            type=schema_from_obj(obj["type"], names, parent_namespace),
            doc=obj.get("doc"),
            default=obj.get("default"),
            order=obj.get("order"),
            aliases=obj.get("aliases"),
        )

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
                raise SchemaValidationError("default symbol {sym} is in list of symbols")

        return True

    def default_is_valid(self) -> bool:
        return self.default in self.symbols

    @classmethod
    def from_dict(cls, obj, names: Dict[str, Schema], parent_namespace: str="") -> EnumSchema:
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

@dataclass
class FixedSchema(NamedSchema):
    size: int

    def validate(self):
        super(FixedSchema, self).validate()
        if self.size < 0:
            raise SchemaValidationError("size cannot be negative")

    def default_is_valid(self) -> bool:
        return len(self.default.encode()) == self.size

    @classmethod
    def from_dict(cls, obj, names: Dict[str, Schema], parent_namespace: str="") -> FixedSchema:
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

@dataclass
class ArraySchema(Schema):
    items: Schema
    default: Optional[List[Any]]

    def validate(self):
        self.items.validate()
        super(ArraySchema, self).validate()

    def default_is_valid(self) -> bool:
        # Copy the item schema, and then modify it to change the 'default' value
        # for each value in the list, and test if it's valid.
        item_schema_copy = copy.deepcopy(self.items)
        for obj in self.default:
            item_schema_copy.default = obj
            if not item_schema_copy.default_is_valid():
                return False
        return True

    @classmethod
    def from_dict(cls, obj, names: Dict[str, Schema], parent_namespace: str="") -> ArraySchema:
        return ArraySchema(
            type="array",
            items=schema_from_obj(obj["items"], names, parent_namespace),
            default=obj.get("default"),
            _names=names,
        )

@dataclass
class MapSchema(Schema):
    values: Schema
    default: Optional[Dict[str, Any]]

    def validate(self):
        self.values.validate()
        super(MapSchema, self).validate()

    def default_is_valid(self) -> bool:
        # Copy the values schema, and then modify it to change the 'default' value
        # for each value in the map, and test if it's valid.
        values_schema_copy = copy.deepcopy(self.values)
        for obj in self.default.values():
            values_schema_copy.default = obj
            if not values_schema_copy.default_is_valid():
                return False
        return True

    @classmethod
    def from_dict(cls,
                  obj,
                  names: Dict[str, Schema],
                  parent_namespace: str="",
    ) -> ArraySchema:
        return MapSchema(
            type="map",
            values=schema_from_obj(obj["values"], names, parent_namespace),
            default=obj.get("default"),
            _names=names,
        )

@dataclass
class UnionSchema(Schema):
    options: List[Schema]

    def validate(self):
        if len(self.options) < 2:
            raise SchemaValidationError("unions must have at least two elements")
        for o in self.options:
            o.validate()

    @classmethod
    def from_list(cls, l, names: Dict[str, Schema], parent_namespace: str="") -> UnionSchema:
        us = UnionSchema(
            type="union",
            options=[],
            default=None,
            _names=names,
        )
        for option in l:
            us.options.append(schema_from_obj(option, names, parent_namespace))
        return us

class SchemaValidationError(Exception):
    pass

class SchemaValidationError(Exception):
    pass

def gather_named_types(schema: Schema) -> Dict[str, Schema]:
    """
    Traverse the schema to find the definitions of all named types. Returns a
    dictionary, mapping fully-qualified type names to their definitions. Aliases
    are included (in fully-qualified form).
    """
    result = {}

    to_be_visited = [schema]

    while True:
        if len(to_be_visited) == 0:
            break
        schema = to_be_visited.pop()

        # Add named schemas to the result dictionary.
        if isinstance(schema, NamedSchema):
            if schema.fullname() in result:
                # We've already visited this schema. Move on.
                continue
            result[schema.fullname()] = schema
            for alias in schema.fullaliases():
                result[alias] = schema

        # For any schema types that can contain other schemas, add those
        # children to the stack to be visited.
        if isinstance(schema, UnionSchema):
            for option in schema.options:
                to_be_visited.append(option)
        elif isinstance(schema, ArraySchema):
            to_be_visited.append(schema.items)
        elif isinstance(schema, MapSchema):
            to_be_visited.append(schema.values)
        elif isinstance(schema, RecordSchema):
            for field in schema.fields:
                to_be_visited.append(field.type)

    return result

def expand_names():
    pass
