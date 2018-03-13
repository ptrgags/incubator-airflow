"""
This module defines two example example DAGs that are built using DagBuilder.
"""
import re
from datetime import timedelta

from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.utils.dag_builder import DagBuilder, SubDagBuilder

DEFAULTS = {
    'owner': 'airflow',
    'provide_context': False,
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

SCHEDULE = "@once"


def make_subdag(task_id, num_tasks):
    """
    Make a dummy subdag to test the SubDagBuilder
    """
    # "begin" splits out into several paths
    # TODO: Fix this so it is Python 2/3 compatible
    fork_edges = [
        ("begin", "task{}".format(i))
        for i in xrange(num_tasks)]
    # each path is followed by "end"
    join_edges = [
        ("task{}".format(i), "end")
        for i in xrange(num_tasks)]

    # Build the subdag
    return (
        SubDagBuilder(
            "dag_builder_example",
            task_id,
            DEFAULTS,
            dummies=True)
        .add_edges(fork_edges)
        .add_edges(join_edges)
        .build_subdag_operator())


def task_id_to_op(task_id, config):
    """
    This is a simple exmaple of mapping task_id -> airflow Operator

    HIGHLY RECOMMENDED: Use the Factory Method design pattern so this
    doesn't become a huge if/else block!!
    """
    # Map a specific task to an operator
    if task_id == "print_hostname":
        return BashOperator(task_id=task_id, bash_command="hostname")

    # Example of a subdag.
    # this is also a simple example of how to map task_ids by a pattern.
    if task_id.startswith("subdag"):
        return make_subdag(task_id, 3)

    # Map a pattern of tasks onto a type of operator.
    # If the number of regular expressions you need is more than a few,
    # consider using a loop or delegate to a function.
    match = re.match(r'hello_(.*)', task_id)
    if match:
        name, = match.groups()
        return PythonOperator(
            task_id=task_id,
            python_callable=lambda: 'Hello, ' + name.capitalize())

    # Always raise a ValueError for unknown task ID. This allows the
    # DagBuilder to know to make a dummy operator or raise an exception
    # depending on the configuration
    raise ValueError


# TODO: Move this comment to the documentation
#
# Pro Tip: If you ask Airflow nicely, you can get it to import any python file
# you'd like!
#
# Dear airflow,
#
#    Will you please import this DAG?
#
# Thank you
# ------------------------
#
# (Airflow only imports python files that include both the string "DAG" and
# the string "airflow" anywhere in the file. Thus, this comment makes or breaks
# the code.)
dag = (
    DagBuilder(
        "dag_builder_example",
        DEFAULTS,
        SCHEDULE,
        # If this option is specified, unknown task IDs will turn into
        # dummy operators instead of raising an exception This is useful for
        # development, but do not use in production!
        dummies=True)
    # This is the typical way to add vertices/edges
    .add_edge("print_hostname", "hello_world")
    # If you have a list of edges, it's easier to do this:
    .add_edges([
        ("hello_world", "hello_sam"),
        ("hello_world", "hello_peter"),
        ("hello_world", "hello_tom")])
    # This task is disjoint from the rest of the graph
    .add_standalone_vertex("task1")
    # This results in hello_world -> task2 -> hello_peter. Note
    # that the link from hello_world -> hello_peter is removed.
    .insert_vertex_between("task2", "hello_world", "hello_peter")
    # You can remove vertices too. Note that this does not
    # attempt to reconnect the nodes on either side of the removed
    # graph vertex.
    .add_edge('hello_peter', 'remove_me')
    .add_edge('remove_me', 'disconnected')
    .remove_vertex('remove_me')
    # Add a couple subdags to try out SubDagBuilder
    .add_edge('hello_world', 'subdag_test1')
    .add_edge('hello_world', 'subdag_test2')
    # Another step after one of the subdags
    .add_edge('subdag_test1', 'wait')
    # pass in a function that maps task_id -> Airflow operator.
    .build(task_id_to_op))

# Here's another DAG that pulls its config from a config dict and
# adds changes from a config file
BASE_DAG = {
    "dag_changes": [
        # Make a single edge. This calls
        # DagBuilder.add_edge("hello_world", "hello_someone")
        ["add_edge", "hello_world", "hello_someone"],
        # Since this is equivalent to calling DagBuilder.add_edges(*args),
        # this works too!
        ["add_edges", [
            ["hello_someone", "hello_me"],
            ["hello_someone", "hello_another"]
        ]],
        # Any method on DagBuilder can be called, so long as the arguments
        # are valid JSON types. Since all of the basic methods accept
        # strings or iterables of strings, all of these work nicely
        ["remove_vertex", "hello_another"],
        # This vertex will be removed by the config file
        ["add_standalone_vertex", "remove_me"]
    ]
}

# In our internal example DAG, we loaded this dictonary
CUSTOM_CHANGES = {
    "dag_changes": [
        [
            "insert_vertex_between",
            "print_hostname",
            "hello_someone",
            "hello_me"
        ],
        ["add_edge", "hello_someone", "hello_peter"],
        ["remove_vertex", "remove_me"]
    ]
}

dict_dag = (
    DagBuilder(
        "dag_builder_config_example",
        DEFAULTS,
        SCHEDULE,
        dummies=True)
    # In this case, I apply a set of hard-coded defaults
    # and then load further changes from a config file.
    # This is a recommended way of doing this, however any number
    # of changelists can be applied.
    .apply_changelist(BASE_DAG)
    .apply_changelist(CUSTOM_CHANGES)
    .build(task_id_to_op))
