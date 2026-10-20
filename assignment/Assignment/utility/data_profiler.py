"""
YAML-driven Great Expectations profiler for pandas DataFrames.

Install:
    pip install "great-expectations>=1.0,<2.0" pandas pyyaml

Example:
    profiler = DataProfiler(data=df, config_path="config/orders_expectations.yaml")
    report = profiler.run()  # JSON-serializable dict
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Type

import pandas as pd
import yaml

try:
    import great_expectations as gx
    from great_expectations.expectations import (
        ExpectColumnDistinctValuesToBeInSet,
        ExpectColumnMaxToBeBetween,
        ExpectColumnMeanToBeBetween,
        ExpectColumnMedianToBeBetween,
        ExpectColumnMinToBeBetween,
        ExpectColumnMostCommonValueToBeInSet,
        ExpectColumnPairValuesAToBeGreaterThanB,
        ExpectColumnProportionOfUniqueValuesToBeBetween,
        ExpectColumnSumToBeBetween,
        ExpectColumnToExist,
        ExpectColumnValueLengthsToBeBetween,
        ExpectColumnValueLengthsToEqual,
        ExpectColumnValuesToBeBetween,
        ExpectColumnValuesToBeInSet,
        ExpectColumnValuesToBeInTypeList,
        ExpectColumnValuesToBeNull,
        ExpectColumnValuesToBeUnique,
        ExpectColumnValuesToMatchRegex,
        ExpectColumnValuesToNotBeNull,
        ExpectTableRowCountToBeBetween,
        ExpectTableRowCountToEqual,
    )
except ImportError as exc:
    raise ImportError(
        'Install dependencies with: pip install '
        '"great-expectations>=1.0,<2.0" pandas pyyaml'
    ) from exc


class DataProfiler:
    """Execute enabled YAML-configured Great Expectations checks on a DataFrame."""

    EXPECTATION_REGISTRY: Dict[str, Type[Any]] = {
        "column_exists": ExpectColumnToExist,
        "column_values_to_not_be_null": ExpectColumnValuesToNotBeNull,
        "column_values_to_be_null": ExpectColumnValuesToBeNull,
        "column_values_to_be_unique": ExpectColumnValuesToBeUnique,
        "column_values_to_be_between": ExpectColumnValuesToBeBetween,
        "column_values_to_be_in_set": ExpectColumnValuesToBeInSet,
        "column_values_to_match_regex": ExpectColumnValuesToMatchRegex,
        "column_values_to_be_in_type_list": ExpectColumnValuesToBeInTypeList,
        "column_value_lengths_to_be_between": ExpectColumnValueLengthsToBeBetween,
        "column_value_lengths_to_equal": ExpectColumnValueLengthsToEqual,
        "column_distinct_values_to_be_in_set": ExpectColumnDistinctValuesToBeInSet,
        "column_most_common_value_to_be_in_set": ExpectColumnMostCommonValueToBeInSet,
        "column_min_to_be_between": ExpectColumnMinToBeBetween,
        "column_max_to_be_between": ExpectColumnMaxToBeBetween,
        "column_mean_to_be_between": ExpectColumnMeanToBeBetween,
        "column_median_to_be_between": ExpectColumnMedianToBeBetween,
        "column_sum_to_be_between": ExpectColumnSumToBeBetween,
        "column_proportion_of_unique_values_to_be_between": (
            ExpectColumnProportionOfUniqueValuesToBeBetween
        ),
        "column_pair_values_a_to_be_greater_than_b": (
            ExpectColumnPairValuesAToBeGreaterThanB
        ),
        "table_row_count_to_be_between": ExpectTableRowCountToBeBetween,
        "table_row_count_to_equal": ExpectTableRowCountToEqual,
    }

    def __init__(
        self,
        data: pd.DataFrame,
        config_path: str | Path,
        datasource_name: str = "runtime_pandas_datasource",
        asset_name: str = "runtime_dataframe",
    ) -> None:
        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"`data` must be a pandas DataFrame, got {type(data).__name__}.")
        if not config_path:
            raise ValueError("`config_path` is required; pass the YAML file for this dataset.")

        self.data = data
        self.config_path = Path(config_path).expanduser().resolve()
        self.datasource_name = datasource_name
        self.asset_name = asset_name
        self.config = self._load_config()
        self._validate_config()

    @classmethod
    def register_expectation(cls, name: str, expectation_class: Type[Any]) -> None:
        """Register a supported Great Expectations expectation class."""
        if not name or not isinstance(name, str):
            raise ValueError("Expectation name must be a non-empty string.")
        cls.EXPECTATION_REGISTRY[name] = expectation_class

    def _load_config(self) -> Dict[str, Any]:
        if not self.config_path.is_file():
            raise FileNotFoundError(f"Expectation YAML file not found: {self.config_path}")
        with self.config_path.open("r", encoding="utf-8") as stream:
            config = yaml.safe_load(stream) or {}
        if not isinstance(config, dict):
            raise ValueError("YAML root must be a mapping.")
        return config

    def _validate_config(self) -> None:
        expectations = self.config.get("expectations")
        if not isinstance(expectations, list):
            raise ValueError("YAML must contain an `expectations` list.")
        for index, item in enumerate(expectations):
            if not isinstance(item, dict):
                raise ValueError(f"expectations[{index}] must be a mapping.")
            if not isinstance(item.get("name"), str):
                raise ValueError(f"expectations[{index}] requires a string `name`.")
            if not isinstance(item.get("enabled"), bool):
                raise ValueError(f"expectations[{index}].enabled must be true or false.")
            if not isinstance(item.get("kwargs", {}), dict):
                raise ValueError(f"expectations[{index}].kwargs must be a mapping.")

    def _build_suite(self) -> Any:
        suite = gx.ExpectationSuite(name="dynamic_data_profiler_suite")
        for item in self.config["expectations"]:
            if not item["enabled"]:
                continue
            name = item["name"]
            expectation_class = self.EXPECTATION_REGISTRY.get(name)
            if expectation_class is None:
                raise ValueError(
                    f"Unknown expectation `{name}`. Registered names: "
                    f"{', '.join(sorted(self.EXPECTATION_REGISTRY))}"
                )
            kwargs = dict(item.get("kwargs", {}))
            if item.get("meta") is not None:
                kwargs["meta"] = dict(item["meta"])
            try:
                suite.add_expectation(expectation_class(**kwargs))
            except TypeError as exc:
                raise ValueError(f"Invalid kwargs for `{name}`: {exc}") from exc
        return suite

    def run(self) -> Dict[str, Any]:
        """Run enabled checks and return a JSON-serializable report dictionary."""
        enabled_count = sum(bool(item["enabled"]) for item in self.config["expectations"])
        skipped_count = len(self.config["expectations"]) - enabled_count

        if enabled_count == 0:
            return {
                "success": True,
                "summary": {"total": 0, "successful": 0, "unsuccessful": 0,
                            "skipped": skipped_count},
                "results": [],
            }

        try:
            context = gx.get_context(mode="ephemeral")
            datasource = context.data_sources.add_pandas(name=self.datasource_name)
            asset = datasource.add_dataframe_asset(name=self.asset_name)
            batch_definition = asset.add_batch_definition_whole_dataframe(
                name="runtime_dataframe_batch"
            )
            suite = self._build_suite()
            context.suites.add(suite)
            validation_definition = gx.ValidationDefinition(
                name="dynamic_data_profiler_validation",
                data=batch_definition,
                suite=suite,
            )
            context.validation_definitions.add(validation_definition)
            raw_report = validation_definition.run(
                batch_parameters={"dataframe": self.data}
            ).to_json_dict()
        except Exception as exc:
            raise RuntimeError(
                "Great Expectations failed to initialize or run. This implementation "
                "targets Great Expectations 1.x; verify the installed version and "
                f"expectation kwargs. Original error: {type(exc).__name__}: {exc}"
            ) from exc

        results = []
        for item in raw_report.get("results", []):
            expectation_config = item.get("expectation_config", {})
            results.append({
                "expectation_type": expectation_config.get("type"),
                "kwargs": expectation_config.get("kwargs", {}),
                "success": bool(item.get("success", False)),
                "result": item.get("result", {}),
                "exception_info": item.get("exception_info", {}),
            })

        successful = sum(item["success"] for item in results)
        report = {
            "success": bool(raw_report.get("success", False)),
            "summary": {
                "total": len(results),
                "successful": successful,
                "unsuccessful": len(results) - successful,
                "skipped": skipped_count,
            },
            "results": results,
        }
        return json.loads(json.dumps(report, default=str))

    def run_and_raise(self) -> Dict[str, Any]:
        """Run checks and raise AssertionError if any enabled expectation fails."""
        report = self.run()
        if not report["success"]:
            failures = [
                f"- {item['expectation_type']}: {item['kwargs']}"
                for item in report["results"] if not item["success"]
            ]
            raise AssertionError("Data validation failed:\n" + "\n".join(failures))
        return report
