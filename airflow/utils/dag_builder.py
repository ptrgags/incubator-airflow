"""
This defines the DagBuilder class which abstracts the DAG structure from the
implementation of each operator
"""
import airflow.models
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator


class TaskNotFoundException(Exception):
    """Exception raised for an undefined task_id"""
    pass


class DagBuilder(object):
    """
    Class to facilitate the creation of DAGs. This follows the Builder
    design pattern so every method returns self
    """
    def __init__(
            self,
            dag_name,
            dag_defaults,
            schedule,
            dummies=False):
        """
        Constructor

        :param str dag_name: the name of the DAG we are building
        :param dict dag_defaults: default arguments for the DAG
        :param str or timedelta schedule: the schedule for the DAG

        :param bool dummies: If True, whenever an invalid task_id
            is found, create a DummyOperator.
            **THIS MUST BE SET TO FALSE IN PRODUCTION**
        """
        # Construct an empty dag
        self.dag = DAG(
            dag_name,
            default_args=dag_defaults,
            schedule_interval=schedule)

        self.use_dummies = dummies

        # These keep track of ids in the graph
        self.vertices = set()
        self.edges = set()

        # Map of id -> operator
        self.operators = {}

    def add_edge(self, before, after):
        """
        Insert a new edge into the DAG.
        before and after are ids of vertices in the dag.

        This works even if the vertices do not already exist
        in the graph.

        This is the usual way you add vertices and edges to the graph.

        :param str before: The task ID of the upstream task
        :param str after: the task ID of the downstream task
        :returns: self reference for chaining
        """
        self.vertices.add(before)
        self.vertices.add(after)
        self.edges.add((before, after))
        return self

    def add_edges(self, edges):
        """
        Insert edges from a list of (before, after) pairs. This is a
        bulk operation version of add_edge(before, after)

        :param [(str, str)] edges: the edges to create
        :returns: self reference for chaining
        """
        for edge in edges:
            self.add_edge(*edge)
        return self

    def add_standalone_vertex(self, vertex):
        """
        Add a single vertex to the graph with no
        connections.

        These are the two main cases where this is useful:

        1. When testing the first operator of the dag, there is no second
           vertex so it makes no sense to add an edge yet.
        2. Trying to add a task that runs in parallel with no dependencies

        :param str vertex: the vertex to add to the graph
        :returns: self reference for chaining
        """
        self.vertices.add(vertex)
        return self

    def insert_vertex_between(self, vertex, before, after):
        """
        Insert a vertex between before and after vertices. This
        removes the edge (before, after) if it exists and replaces
        it with two edges: (before, vertex) and (vertex, after)

        :param str vertex: The new vertex's task ID
        :param str before: The upstream vertex's task ID
        :param str after: The downstream vertex's task ID
        :returns: self reference for chaining
        """
        self.vertices.add(vertex)
        self.edges.discard((before, after))
        self.edges.add((before, vertex))
        self.edges.add((vertex, after))
        return self

    def remove_vertex(self, vertex):
        """
        Remove a vertex and any connections

        Note that this method does NOT attempt to reconnect upstream and
        downstream vertices. That must be configured by the user of this
        library.

        :param str vertex: the vertex to remove
        :returns: self reference for chaining
        """
        self.vertices.discard(vertex)
        self.edges = set(edge for edge in self.edges if vertex not in edge)
        return self

    def remove_vertices(self, vertices):
        """
        Remove a list of vertices one-by-one.

        This is a bulk version of remove_vertex(vertex)

        :param [str] vertices: the vertices to remove
        :returns: self reference for chaining
        """
        for vertex in vertices:
            self.remove_vertex(vertex)
        return self

    def apply_changelist(self, changelist):
        """
        Run commands automatically from a changelist JSON file.

        The changelist should have the following structure:
        {
            "dag_changes": [
                ["method_name", arg, arg],
                ["method_name", arg, ...],
                ...
            ]
        }

        getattr() is used to lookup the function. Everything after the
        function name is considered an argument. So for example:

        ["add_edge", "task1", "task2"]

        calls

        self.add_edge("task1", "task2")

        This will work for any of the basic graph manipulation methods on
        this class. Since it is implemented generically, it will even work
        for subclass methods!

        :param dict changelist: dict of changes to apply to the graph.
        :returns: self reference for method chaining
        """
        try:
            # Pull out the "changes" field of the JSON
            changes = changelist['dag_changes']
        except KeyError:
            raise ValueError('Changelist must have the field "changes"')

        for change in changes:
            try:
                # Changes have the format [func_name, arg, ...]
                method, args = change[0], change[1:]
            except ValueError:
                raise ValueError('Invalid change {}'.format(change))

            try:
                # Look up the function name.
                func = getattr(self, method)
            except AttributeError as exc:
                raise ValueError((
                    '{}: The first element of {} must match one of the methods'
                    ' of DagBuilder').format(exc, change))

            # Finally, call the function. If this one raises an exception,
            # let it bubble up.
            func(*args)

        # Return self for chaining
        return self

    def build(self, operator_mapping=None, config=None):
        """
        Build the DAG. This is always the last method in the chain

        :param dict config: user-defined config that will be passed
            into the mapping function
        :param str -> Operator operator_mapping: A function that, when
            given a task_id, constructs an Operator. If the task_id is
            unknown, raise a ValueError so DagBuilder can handle it properly.
            Also, the `dag` parameter for each operator is not required
            since the operators are created within a `with` block.

            If this is None, a default one will be provided.
        :returns: a DAG object
        """
        if config is None:
            config = {}

        # if no custom mapping is specified, use the current class' default
        mapping = operator_mapping or self.default_operator_mapping

        # Make the operators
        self._make_operators(mapping, config)
        self._connect_edges()
        return self.dag

    # This method intentionally does nothing but raise an exception
    # pylint: disable=unused-argument,no-self-use
    def default_operator_mapping(self, task_id, config):
        """
        The default mapping defines no valid operators.
        If dummies is True in the constructor,
        this means every task becomes a dummy task

        Override this in subclasses to make a new default mapping
        """
        # Raising ValueError will cause a dummy to be
        # created (see self._make_operator())
        raise ValueError

    def _make_operator(self, operator_mapping, task_id, config):
        """
        map a task_id onto an Airflow operator. If the the task_id
        is not found (denoted by a ValueError), one of two things happen:

        1. If dummies was specified in the constructor,
           return a DummyOperator with the task ID. this is useful during
           development to view the structure of a DAG without having every
           step implemented.
        2. Otherwise, raise a TaskNotFoundException.

        :param str -> Operator operator_mapping: A function that, when
            given a task_id, constructs an Operator. If the task_id is
            unknown, raise a ValueError so DagBuilder can handle it properly
            Also, the `dag` parameter for each operator is not required
            since the operators are created within a `with` block.
        :param str task_id: The ID of the resulting operator
        :returns: an Airflow operator.
        """
        try:
            return operator_mapping(task_id, config)
        except ValueError:
            if self.use_dummies:
                return DummyOperator(task_id=task_id)
            else:
                raise TaskNotFoundException(
                    "Could not make an operator for {}".format(task_id))

    def _make_operators(self, operator_mapping, config):
        """
        Iterate over the vertices and make an operator for each one.
        """
        with self.dag:
            for task_id in self.vertices:
                operator = self._make_operator(
                    operator_mapping, task_id, config)
                self.operators[task_id] = operator

    def _connect_edges(self):
        """
        Iterate over the edges and connect the two operators.
        """
        for before, after in self.edges:
            before_op = self.operators[before]
            after_op = self.operators[after]
            before_op.set_downstream(after_op)


class SubDagBuilder(DagBuilder):
    """
    SubDags are very similar to regular dags. These are the main differences:

    1. According to the Airflow docs, subdags should be named with the
        following naming convention: parent_dag_name.sub_dag_name
    2. The schedule needs to be set else the subdag will not run. Thus,
        I set the schedule to @daily.
    3. Subdags need to be wrapped in a SubDagOperator. Use
        .build_subdag_operator() for this
    """
    def __init__(
            self,
            parent_dag_name,
            subdag_name,
            dag_defaults,
            dummies=False):
        """
        Constructor

        :param str parent_dag_name: name of the parent dag
        :param str subdag_name: name of the subdag
        :param dict dag_defaults: Airflow dag defaults.
        :param bool dummies: if True, create DummyOperators
            on unknown task_ids
        """

        super(SubDagBuilder, self).__init__(
            # Follow subdag naming conventions
            '{}.{}'.format(parent_dag_name, subdag_name),
            dag_defaults,
            '@daily',
            dummies)

        # Keep track of these just in case we need them
        self.parent_dag_name = parent_dag_name
        self.subdag_name = subdag_name

    def build_subdag_operator(self, operator_mapping=None, config=None):
        """
        Build the dag, but then wrap it in a SubDagOperator
        """
        # NOTE: This is not needed in Airflow 1.9
        # We are inside the correct context manager, but
        # SubDagOperator ignores it in Airflow 1.8 and throws
        # an exception.
        # pylint: disable=protected-access
        # TODO: Try removing this
        dag = airflow.models._CONTEXT_MANAGER_DAG

        if config is None:
            config = {}

        subdag = self.build(operator_mapping, config)
        return SubDagOperator(
            task_id=self.subdag_name,
            subdag=subdag,
            dag=dag)
