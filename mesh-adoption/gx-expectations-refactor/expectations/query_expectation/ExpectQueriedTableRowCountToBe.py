"""
This is an example of a Custom QueryExpectation.
For detailed information on QueryExpectations, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
"""

from typing import ClassVar, List, Tuple, Union

from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)
from great_expectations.util import convert_to_json_serializable
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine
from great_expectations.compatibility.typing_extensions import override
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Type, Union
from great_expectations.validator.validator import ValidationDependencies

from great_expectations.expectations.registry import (
    get_metric_kwargs,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class ExpectQueriedTableRowCountToBe(QueryExpectation):
    """Expect the expect the number of rows returned from a queried table to equal a specified value.

    expect_queried_table_row_count_to_be is a \
    [Query Expectation](https://docs.greatexpectations.io/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations)

    Args:
        value (int): \
            Expected number of returned rows
        query (str): \
            SQL query to be executed (default will perform a SELECT COUNT(*) on the table)

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Successful if at least mostly fraction of values match the expectation. \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).
    """

    value: int = 0
    query: str = """
            SELECT COUNT(*)
            FROM {batch}
            """

    metric_dependencies: ClassVar[Tuple[str, ...]] = ("query.table",)

    success_keys: ClassVar[Tuple[str, ...]] = (
        "value",
        "query",
    )

    domain_keys: ClassVar[Tuple[str, ...]] = (
        "batch_id",
        "row_condition",
        "condition_parser",
    )

    examples: ClassVar[List[dict]] = [
        {
            "data": [
                {
                    "data": {
                        "col1": [1, 2, 2, 3, 4],
                        "col2": ["a", "a", "b", "b", "a"],
                    },
                },
            ],
            "suppress_test_for": ["snowflake"],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "value": 5,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "value": 2,
                    },
                    "out": {"success": False},
                },
                {
                    "title": "positive_test_static_data_asset",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "value": 5,
                        "query": """
                                 SELECT COUNT(*)
                                 FROM test
                                 """,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "positive_test_row_condition",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "value": 2,
                        "row_condition": 'col("col1")==2',
                        "condition_parser": "great_expectations",
                    },
                    "out": {"success": True},
                },
            ],
        },
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": ["query-based"],
        "contributors": ["@austiezr"],
    }

    # @override
    # def _validate(
    #     self,
    #     metrics: dict,
    #     runtime_configuration: dict = None,
    #     execution_engine: ExecutionEngine = None,
    # ) -> Union[ExpectationValidationResult, dict]:
    #     configuration = self.configuration
    #     metrics = convert_to_json_serializable(data=metrics)
    #     query_result = list(metrics.get("query.table")[0].values())[0]
    #     value = configuration["kwargs"].get("value")

    #     success = query_result == value

    #     return {
    #         "success": success,
    #         "result":{
    #             "element_count": query_result,
    #             "unexpected_count":0,
    #             "unexpected_percent":0.0,
    #         }
    #     }

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

        return validation_dependencies

    def _validate(
        self,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        configuration = self.configuration
        metrics = convert_to_json_serializable(data=metrics)
        query_result = list(metrics.get("query.table")[0].values())[0]
        value = configuration["kwargs"].get("value")

        return {
            "success": query_result == value,
            "result": {
                "expectation_output_metric": query_result,
                "element_count": metrics.get("table.row_count")
            },
        }





# if __name__ == "__main__":
#     ExpectQueriedTableRowCountToBe().print_diagnostic_checklist()