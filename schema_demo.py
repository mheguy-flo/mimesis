"""Quick demo of lazy data generation."""

from typing import Any

from mimesis.enums import Gender, TimestampFormat
from mimesis.random import Random
from mimesis.schema import BaseField, Field, Fieldset


class LazyField(BaseField):
    """A class that evaluates lazily when used in an AdvancedSchema."""

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.perform(*args, **kwargs)


SchemaNode = (
    tuple[Field | Fieldset | LazyField, str, dict[str, Any]] | dict[str, "SchemaNode"]
)
SchemaType = dict[str, SchemaNode]


class EvaluatedSchema:
    """Evaluates a schema in order to allow lazy generation."""

    LAZY_TYPES = (LazyField,)
    EAGER_TYPES = (Field, Fieldset)

    def __init__(
        self,
        schema: SchemaType,
    ) -> None:
        self.eager_schema = self._extract_schema(schema, self.EAGER_TYPES)
        self.lazy_schema = self._extract_schema(schema, self.LAZY_TYPES)

    def _extract_schema(
        self, schema: SchemaType, target_types: tuple[type, ...]
    ) -> SchemaType:
        """Extract schema recursively according to the types requested."""
        extracted_schema: SchemaType = {}

        for node_name, node in schema.items():
            if isinstance(node, dict):
                extracted_schema[node_name] = self._extract_schema(node, target_types)
            elif isinstance(node[0], target_types):
                extracted_schema[node_name] = node

        return extracted_schema

    def create(self) -> dict[str, Any]:
        eager_results = self._evaluate_eager(self.eager_schema)
        lazy_results = self._evaluate_lazy(self.lazy_schema, eager_results)
        return deep_merge(eager_results, lazy_results)

    def _evaluate_eager(
        self,
        schema: SchemaType,
    ) -> dict[str, Any]:
        results = {}
        for node_name, node in schema.items():
            if isinstance(node, dict):
                results[node_name] = self._evaluate_eager(node)
            else:
                field, handler, kwargs = node
                results[node_name] = field(handler, **kwargs)

        return results

    def _evaluate_lazy(
        self,
        schema: SchemaType,
        existing_results: dict[str, Any],
    ) -> dict[str, Any]:
        new_results = {}
        for node_name, node in schema.items():
            if isinstance(node, dict):
                new_results[node_name] = self._evaluate_lazy(node, existing_results)
            else:
                field, handler, kwargs = node
                new_results[node_name] = field(
                    handler, _eager_data=existing_results, **kwargs
                )

        return new_results


def deep_merge(d1: dict[str, Any], d2: dict[str, Any]) -> dict[str, Any]:
    result = dict(d1)
    for key, value in d2.items():
        if isinstance(value, dict):
            node = result.get(key, {})
            result[key] = deep_merge(node, value)
        else:
            result[key] = value
    return result


def custom_email_handler(
    random: Random,
    *,
    domains: list[str],
    _eager_data: dict[str, Any],
    **_kwargs: Any,
) -> str:
    name: str = _eager_data["owner"]["creator"]
    username = name.replace(" ", ".").lower()

    return f"{username}@{random.choice(domains)}"


fieldset = Fieldset()
field = Field()
lazy_field = LazyField()
lazy_field.register_handler("custom_email", custom_email_handler)  # type: ignore

schema = EvaluatedSchema(
    {
        "pk": (field, "increment", {}),
        "uid": (field, "uuid", {}),
        "name": (field, "text.word", {}),
        "version": (field, "version", {}),
        "timestamp": (field, "timestamp", {"fmt": TimestampFormat.POSIX}),
        "owner": {
            "email": (lazy_field, "custom_email", {"domains": ["mimesis.name"]}),
            "creator": (field, "full_name", {"gender": Gender.FEMALE}),
        },
        "apiKeys": (fieldset, "token_hex", {"key": lambda s: s[:16], "i": 3}),
    }
)

print(schema.create())
