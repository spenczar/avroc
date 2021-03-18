"""
This module contains functions devoted to finding recursive components of
the graph formed by an Avro schema.

Recursion only occurs for named types which contain a reference to their own
name. Only a few types can have names: records, enums, and fixeds.

In addition, only a few types can contain a reference to a named type:
- Arrays (the type of their items can be named)
- Records (the type of their fields can be named)
- Maps (the type of their values can be named)
- Unions (any of their permitted types can be named)

Because enums and fixeds can't contain references, they can never be recursive.
Only records can, although they might be recursive through a chain that flows
through some other complex type like an array.
"""
from __future__ import annotations

from typing import Dict, Set, Optional, Iterable, List, Deque, DefaultDict

from avroc.avro_common import PRIMITIVES
from avroc.schema import (
    Schema,
    NamedSchema,
    UnionSchema,
    PrimitiveSchema,
    RecordSchema,
    ArraySchema,
    MapSchema,
    EnumSchema,
    FixedSchema,
    NamedSchemaReference,
)
import collections


def find_recursive_types(schema: Schema) -> List[RecordSchema]:
    """
    Find the schemas of all types in the schema which are defined recursively.

    The return value is a list. Each item is the schema of a type which includes
    a reference to itself, either in its fields or somewhere down the tree of
    its fields' fields.

    The list is returned in depth-first ordering.
    """
    result: List[RecordSchema] = []

    names: Dict[str, "NamegraphNode"] = {}
    graph_roots = _schema_to_graph(schema, names)
    for root in graph_roots:
        # There could be multiple graph roots if the schema is a top-level
        # union.
        result.extend([node.schema for node in _find_cycle_roots(root)])  #type: ignore
    return result


def _find_cycle_roots(graph: "NamegraphNode") -> List["NamegraphNode"]:
    stack: Deque["NamegraphNode"] = collections.deque()

    # Map of node -> count of how many times it has appeared in the current
    # stack.
    visited: DefaultDict["NamegraphNode", int] = collections.defaultdict(int)

    roots: Set["NamegraphNode"] = set()

    def visit(node: "NamegraphNode"):
        stack.append(node)
        visited[node] += 1
        for ref in node.references:
            if ref in visited:
                # Found a cycle.
                roots.add(ref)
            else:
                visit(ref)
        visited[node] -= 1
        stack.pop()

    visit(graph)
    return list(roots)


class NamegraphNode:
    name: str  # Fully-qualified name.
    schema: NamedSchema
    references: Set["NamegraphNode"]

    def __init__(
        self, schema: NamedSchema, references: Optional[Iterable["NamegraphNode"]] = None
    ):
        self.schema = schema
        self.name = schema.name
        if references is not None:
            self.references = set(references)
        else:
            self.references = set()

    def add_reference(self, ref: "NamegraphNode"):
        self.references.add(ref)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, NamegraphNode):
            return NotImplemented
        if self.name != other.name:
            return False
        if len(self.references) != len(other.references):
            return False
        self_names = set(x.name for x in self.references)
        other_names = set(x.name for x in other.references)
        return self_names == other_names

    def __hash__(self):
        return hash(self.name)

    def __repr__(self) -> str:
        name = repr(self.name)
        refs = repr(self.references)
        return f"NamegraphNode(name={name}, references={refs})"


def _schema_to_graph(schema: Schema, names: Dict) -> List[NamegraphNode]:
    # Convert a schema definition into a graph representation which only
    # includes named-type components.

    if isinstance(schema, PrimitiveSchema):
        return []
    if isinstance(schema, UnionSchema):
        result = []
        for s in schema.options:
            result.extend(_schema_to_graph(s, names))
        return result
    elif isinstance(schema, ArraySchema):
        return _schema_to_graph(schema.items, names)
    elif isinstance(schema, MapSchema):
        return _schema_to_graph(schema.values, names)
    elif isinstance(schema, RecordSchema):
        node = NamegraphNode(schema)

        names[schema.fullname()] = node
        for alias in schema.fullaliases():
            names[alias] = node

        for field in schema.fields:
            for subnode in _schema_to_graph(field.type, names):
                node.add_reference(subnode)
        return [node]
    elif isinstance(schema, (EnumSchema, FixedSchema)):
        # Enums and fixeds cannot include named references. But they can be "terminal nodes".
        node = NamegraphNode(schema)

        names[schema.fullname()] = node
        for alias in schema.fullaliases():
            names[alias] = node
        return []
    elif isinstance(schema, NamedSchemaReference):
        node = names[schema.referenced_name]
        return [node]
    else:
        raise TypeError(f"unexpected type: {type(schema)}")
