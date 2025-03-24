from __future__ import annotations

import datetime
import functools
import logging
import re
import warnings
from abc import ABC, abstractmethod
from collections import Counter
from copy import deepcopy
from inspect import isabstract
from numbers import Number
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import pandas as pd
from dateutil.parser import parse
from typing_extensions import ParamSpec, dataclass_transform

from great_expectations import __version__ as ge_version
from great_expectations._docs_decorators import public_api
from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import Field, ModelMetaclass, StrictStr
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.metric_function_types import (
    SummarizationMetricNameSuffixes,
)
from great_expectations.core.result_format import ResultFormat
from great_expectations.core.suite_parameters import (
    get_suite_parameter_key,
    is_suite_parameter,
)
from great_expectations.exceptions import (
    GreatExpectationsError,
    InvalidExpectationConfigurationError,
    InvalidExpectationKwargsError,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
    parse_result_format,
)
from great_expectations.expectations.model_field_descriptions import (
    COLUMN_A_DESCRIPTION,
    COLUMN_B_DESCRIPTION,
    COLUMN_DESCRIPTION,
    COLUMN_LIST_DESCRIPTION,
    WINDOWS_DESCRIPTION,
)
from great_expectations.expectations.model_field_types import (
    ConditionParser,
    MostlyField,
)
from great_expectations.expectations.registry import (
    get_metric_kwargs,
    register_expectation,
    register_renderer,
)
from great_expectations.expectations.sql_tokens_and_types import (
    valid_sql_tokens_and_types,
)
from great_expectations.expectations.window import Window
from great_expectations.render import (
    AtomicDiagnosticRendererType,
    AtomicPrescriptiveRendererType,
    CollapseContent,
    LegacyDiagnosticRendererType,
    LegacyRendererType,
    RenderedAtomicContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
    renderedAtomicValueSchema,
)
from great_expectations.render.exceptions import RendererConfigurationError
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    build_count_and_index_table,
    build_count_table,
    num_to_str,
)
from great_expectations.util import camel_to_snake
from great_expectations.validator.computed_metric import MetricValue  # noqa: TCH001
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.core.expectation_diagnostics.expectation_diagnostics import (
        ExpectationDiagnostics,
    )
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.execution_engine import (
        ExecutionEngine,
    )
    from great_expectations.render.renderer_configuration import MetaNotes
    from great_expectations.validator.validator import ValidationDependencies, Validator

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T", List[RenderedStringTemplateContent], RenderedAtomicContent)

from great_expectations.expectations.expectation import BatchExpectation

from great_expectations.expectations.registry import _registered_metrics

class ColumnAggregateExpectation(BatchExpectation, ABC):
    """Base class for column aggregate Expectations.

    These types of Expectation produce an aggregate metric for a column, such as the mean, standard deviation,
    number of unique values, column type, etc.

    --Documentation--
        - https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations/

    Args:
     domain_keys (tuple): A tuple of the keys used to determine the domain of the
         expectation.
     success_keys (tuple): A tuple of the keys used to determine the success of
         the expectation.

         - A  "column" key is required for column expectations.

    Raises:
        InvalidExpectationConfigurationError: If no `column` is specified
    """  # noqa: E501 # FIXME CoP

    column: StrictStr = Field(min_length=1, description=COLUMN_DESCRIPTION)
    row_condition: Union[str, None] = None
    condition_parser: Union[ConditionParser, None] = None

    domain_keys: ClassVar[Tuple[str, ...]] = (
        "batch_id",
        "column",
        "row_condition",
        "condition_parser",
    )
    domain_type: ClassVar[MetricDomainTypes] = MetricDomainTypes.COLUMN

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[ColumnAggregateExpectation]) -> None:
            BatchExpectation.Config.schema_extra(schema, model)
            schema["properties"]["metadata"]["properties"].update(
                {
                    "domain_type": {
                        "title": "Domain Type",
                        "type": "string",
                        "const": model.domain_type,
                        "description": "Column Aggregate",
                    }
                }
            )
    
    


