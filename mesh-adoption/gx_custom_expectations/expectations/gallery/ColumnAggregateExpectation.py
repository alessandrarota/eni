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
    """  # noqa: E501

    column: StrictStr = Field(min_length=1, description=COLUMN_DESCRIPTION)
    row_condition: Union[str, None] = None
    condition_parser: Union[ConditionParser, None] = None

    domain_keys: ClassVar[Tuple[str, ...]] = (
        "batch_id",
        "table",
        "column",
        "row_condition",
        "condition_parser",
    )
    domain_type: ClassVar[MetricDomainTypes] = MetricDomainTypes.COLUMN

    @override
    def get_validation_dependencies(
        self,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = super().get_validation_dependencies(
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        # chiavi_spark = [
        #     key for key, value in _registered_metrics.items()
        #     if 'SparkDFExecutionEngine' in value.get('providers', {})
        # ]

        # print(chiavi_spark)

        metric_kwargs: dict

        # metric_kwargs = get_metric_kwargs(
        #     metric_name=f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        #     configuration=self.configuration,
        #     runtime_configuration=runtime_configuration,
        # )
        # validation_dependencies.set_metric_configuration(
        #     metric_name=f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        #     metric_configuration=MetricConfiguration(
        #         metric_name=f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        #         metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
        #         metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        #     ),
        # )

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            configuration=self.configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        metric_kwargs = get_metric_kwargs(
            metric_name="table.row_count",
            configuration=self.configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name="table.row_count",
            metric_configuration=MetricConfiguration(
                metric_name="table.row_count",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        result_format_str: Optional[str] = validation_dependencies.result_format.get(
            "result_format"
        )
        include_unexpected_rows: Optional[bool] = validation_dependencies.result_format.get(
            "include_unexpected_rows"
        )

        if result_format_str == ResultFormat.BOOLEAN_ONLY:
            return validation_dependencies

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
            configuration=self.configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        
        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
            configuration=self.configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        if result_format_str == ResultFormat.BASIC:
            return validation_dependencies

        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
            configuration=self.configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )
        metric_kwargs = get_metric_kwargs(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
            configuration=self.configuration,
            runtime_configuration=runtime_configuration,
        )
        validation_dependencies.set_metric_configuration(
            metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
            metric_configuration=MetricConfiguration(
                metric_name=f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
                metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
                metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
            ),
        )

        return validation_dependencies

        # for metric_name in chiavi_spark:
        #     metric_kwargs = get_metric_kwargs(
        #         metric_name=metric_name,
        #         configuration=self.configuration,
        #         runtime_configuration=runtime_configuration,
        #     )
        #     validation_dependencies.set_metric_configuration(
        #         metric_name=metric_name,
        #         metric_configuration=MetricConfiguration(
        #             metric_name=metric_name,
        #             metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
        #             metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        #         ),
        #     )

        #print([key for key in _registered_metrics.keys()])
        #print(self.metric_dependencies)

        # results = {}
        # for key, value in _registered_metrics.items():
        #     metric_kwargs = get_metric_kwargs(
        #         metric_name=key,
        #         configuration=self.configuration, 
        #         runtime_configuration=runtime_configuration 
        #     )
        #     results[key] = {
        #         "metric_domain_kwargs": metric_kwargs["metric_domain_kwargs"],
        #         "metric_value_kwargs": metric_kwargs["metric_value_kwargs"]
        #     }
        
        # configuration = self.configuration
        # metric_name: str
        # for metric_name in self.metric_dependencies:
        #     metric_kwargs = get_metric_kwargs(
        #         metric_name=metric_name,
        #         configuration=self.configuration,
        #         runtime_configuration=runtime_configuration,
        #     )
        #     validation_dependencies.set_metric_configuration(
        #         metric_name=metric_name,
        #         metric_configuration=MetricConfiguration(
        #             metric_name=metric_name,
        #             metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
        #             metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        #         ),
        #     )

        # for metric_name, value in _registered_metrics.items():
        #     metric_kwargs = get_metric_kwargs(
        #         metric_name=metric_name,
        #         configuration=self.configuration,
        #         runtime_configuration=runtime_configuration,
        #     )
        #     validation_dependencies.set_metric_configuration(
        #         metric_name=metric_name,
        #         metric_configuration=MetricConfiguration(
        #             metric_name=metric_name,
        #             metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
        #             metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        #         ),
        #     )
        
        # print(validation_dependencies)
        # # element_count
        # metric_kwargs = get_metric_kwargs(
        #     metric_name="table.row_count",
        #     configuration=configuration,
        #     runtime_configuration=runtime_configuration,
        # )
        # validation_dependencies.set_metric_configuration(
        #     metric_name="table.row_count",
        #     metric_configuration=MetricConfiguration(
        #         metric_name="table.row_count",
        #         metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
        #         metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        #     ),
        # )
        
        # # unexpected_count
        # metric_kwargs = get_metric_kwargs(
        #     metric_name=f"column_values.between.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        #     configuration=self.configuration,
        #     runtime_configuration=runtime_configuration,
        # )
        # validation_dependencies.set_metric_configuration(
        #     metric_name=f"column_values.between.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        #     metric_configuration=MetricConfiguration(
        #         metric_name=f"column_values.between.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        #         metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
        #         metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        #     ),
        # )

        # # null_count
        # metric_kwargs = get_metric_kwargs(
        #     metric_name=f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        #     configuration=self.configuration,
        #     runtime_configuration=runtime_configuration,
        # )
        # validation_dependencies.set_metric_configuration(
        #     metric_name=f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        #     metric_configuration=MetricConfiguration(
        #         metric_name=f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        #         metric_domain_kwargs=metric_kwargs["metric_domain_kwargs"],
        #         metric_value_kwargs=metric_kwargs["metric_value_kwargs"],
        #     ),
        # )

        #return validation_dependencies

    @override
    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        
        # print("\n\n")
        # print(_registered_metrics.get('table.row_count'))
        # print("\n\n")
        # print(_registered_metrics.get('column_values'))

        # _registered_metrics_serializable = {
        #     key: value if isinstance(value, (str, int, float, list, dict)) else str(value)
        #     for key, value in _registered_metrics.items()
        # }
        # import json
        # # Ora serializzi il dizionario
        # print(json.dumps(_registered_metrics_serializable, indent=4))

        # # import json
        # # print(json.dumps(_registered_metrics, indent=4))

        print(metrics)
        
        gx_result = self._validate_metric_value_between(
            metric_name=self.metric_dependencies[0],
            metrics=metrics,
            runtime_configuration=runtime_configuration,
            execution_engine=execution_engine,
        )

        success = gx_result['success']
        element_count = metrics.get('table.row_count')
        unexpected_count = metrics.get('column_values.between.unexpected_count')
        print(unexpected_count)
        null_count: Optional[int] = metrics.get(
            f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
        )
        print(null_count)
        nonnull_count = element_count - null_count
        
        skip_missing = False
        missing_count: Optional[int] = None
        if nonnull_count is None:
            skip_missing = True
        else:
            missing_count = element_count - nonnull_count
            if unexpected_count is not None and element_count > 0:
                unexpected_percent_total = unexpected_count / element_count * 100

            if not skip_missing and missing_count is not None:
                if nonnull_count is not None and nonnull_count > 0:
                    unexpected_percent_nonmissing = unexpected_count / nonnull_count * 100
                else:
                    unexpected_percent_nonmissing = None
            else:
                unexpected_percent_nonmissing = unexpected_percent_total
        
        return_obj: Dict[str, Any] = {"success": success}
        return_obj["result"] = {
            "element_count": element_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": unexpected_percent_nonmissing,
        }

        return return_obj


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
    
    


