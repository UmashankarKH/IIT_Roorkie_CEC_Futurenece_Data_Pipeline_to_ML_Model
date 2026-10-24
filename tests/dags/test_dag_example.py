"""Example DAGs test. This test ensures that all Dags have tags, retries set to two, and no import errors. This is an example pytest and may not be fit the context of your DAGs. Feel free to add and remove tests."""

import os
import logging
from contextlib import contextmanager
import pytest
from airflow.models import DagBag


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        # prepend "(None,None)" to ensure that a test object is always created even if it's a no op.
        return [(None, None)] + [
            (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
        ]


def get_dags():
    """
    Generate a tuple of dag_id, <DAG objects> in the DagBag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Test for import errors on a file"""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


APPROVED_TAGS = {}


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_tags(dag_id, dag, fileloc):
    """
    test if a DAG is tagged and if those TAGs are in the approved list
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert not set(dag.tags) - APPROVED_TAGS


@pytest.mark.parametrize(
    "dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_retries(dag_id, dag, fileloc):
    """
    test if a DAG has retries set
    """
    assert (
        dag.default_args.get("retries", None) >= 2
    ), f"{dag_id} in {fileloc} must have task retries >= 2."


def test_nammamart_dag_stage_order():
    dag_bag = DagBag(include_examples=False)
    etl = dag_bag.dags["nammamart_etl_pipeline"]
    elt = dag_bag.dags["nammamart_elt_surender_singh"]

    assert etl is not None
    assert elt is not None

    etl_extracts = {
        "extract_customers",
        "extract_products",
        "extract_stores",
        "extract_orders",
        "extract_payments",
    }
    assert {
        task_id: etl.get_task(task_id).downstream_task_ids
        for task_id in etl_extracts
    } == {task_id: {"validate"} for task_id in etl_extracts}
    assert etl.get_task("transform").upstream_task_ids == {"validate"}
    assert etl.get_task("load").upstream_task_ids == {"transform"}
    assert etl.get_task("reconcile_and_report").upstream_task_ids == {"load"}
    assert etl.get_task("audit_run").upstream_task_ids == {
        "reconcile_and_report"
    }

    assert "load_raw_sources" in elt.task_group.children
    assert elt.get_task("load_orders").downstream_task_ids == {
        "load_payments"
    }
    assert elt.get_task("load_payments").downstream_task_ids == {
        "load_customers"
    }
    assert elt.get_task("load_customers").downstream_task_ids == {
        "load_products"
    }
    assert elt.get_task("load_products").downstream_task_ids == {
        "load_stores"
    }
    assert elt.get_task("transform_staging").upstream_task_ids == {
        "load_stores"
    }
    assert elt.get_task("build_marts").upstream_task_ids == {
        "transform_staging"
    }
    assert elt.get_task("audit_run").upstream_task_ids == {"build_marts"}
