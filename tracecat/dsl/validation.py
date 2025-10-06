from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError
from temporalio import activity

from tracecat.dsl.common import DSLInput
from tracecat.dsl.models import TriggerInputs
from tracecat.expressions.expectations import ExpectedField, create_expectation_model
from tracecat.logger import logger
from tracecat.validation.models import DSLValidationResult, ValidationDetail

_TYPE_ALIAS_MAP: dict[str, str] = {
    "string": "str",
    "String": "str",
    "integer": "int",
    "Integer": "int",
    "boolean": "bool",
    "Boolean": "bool",
}


def _normalize_type_string(type_str: str) -> str:
    normalized = type_str
    for alias, canonical in _TYPE_ALIAS_MAP.items():
        normalized = re.sub(rf"\b{alias}\b", canonical, normalized)
    return normalized


def validate_trigger_inputs(
    dsl: DSLInput,
    payload: TriggerInputs | None = None,
    *,
    raise_exceptions: bool = False,
    model_name: str = "TriggerInputsValidator",
) -> DSLValidationResult:
    parsed_inputs: dict[str, Any] | None = None
    if not dsl.entrypoint.expects:
        # If there's no expected trigger input schema, we don't validate it
        # as it's ignored anyways
        if isinstance(payload, Mapping):
            parsed_inputs = dict(payload)
        elif payload is None:
            parsed_inputs = {}
        return DSLValidationResult(
            status="success",
            msg="No trigger input schema, skipping validation.",
            parsed_inputs=parsed_inputs,
        )
    logger.trace(
        "DSL entrypoint expects", expects=dsl.entrypoint.expects, payload=payload
    )

    expects_schema: dict[str, ExpectedField] = {}
    for field_name, field_schema in dsl.entrypoint.expects.items():
        field = ExpectedField.model_validate(field_schema)
        normalized_type = _normalize_type_string(field.type)
        if normalized_type != field.type:
            field = field.model_copy(update={"type": normalized_type})
        expects_schema[field_name] = field

    payload_mapping: Mapping[str, Any] | None
    if payload is None:
        payload_mapping = {}
    elif isinstance(payload, Mapping):
        payload_mapping = payload
    else:
        payload_mapping = None

    if payload_mapping is not None:
        validator = create_expectation_model(expects_schema, model_name=model_name)
        try:
            validated = validator.model_validate(dict(payload_mapping))
        except ValidationError as e:
            if raise_exceptions:
                raise
            return DSLValidationResult(
                status="error",
                msg=(
                    "Validation error in trigger inputs "
                    f"({e.title}). Please refer to the schema for more details."
                ),
                detail=ValidationDetail.list_from_pydantic(e),
            )
        parsed_inputs = validated.model_dump()
        if isinstance(payload, dict):
            payload.clear()
            payload.update(parsed_inputs)

    return DSLValidationResult(
        status="success",
        msg="Trigger inputs are valid.",
        parsed_inputs=parsed_inputs,
    )


class ValidateTriggerInputsActivityInputs(BaseModel):
    model_config: ConfigDict = ConfigDict(arbitrary_types_allowed=True)
    dsl: DSLInput
    trigger_inputs: TriggerInputs


@activity.defn
async def validate_trigger_inputs_activity(
    inputs: ValidateTriggerInputsActivityInputs,
) -> DSLValidationResult:
    res = validate_trigger_inputs(
        inputs.dsl, inputs.trigger_inputs, raise_exceptions=True
    )
    return res
