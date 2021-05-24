import logging
import sys
from typing import List

from argo_workflows_sdk.dag import DAG
from argo_workflows_sdk.runtime.cli.dag import invoke as invoke_dag
from argo_workflows_sdk.runtime.cli.node import invoke as invoke_node


def invoke(
    dag: DAG,
    argv: List[str] = sys.argv[1:],
):
    parser = call_arg_parser()
    args = parser.parse_args(argv)
    logging.debug(f"Arguments supplied to CLI are {args}")

    if args.node_name:
        if args.node_name not in dag.nodes:
            raise ValueError(
                f"You used the --node-name parameter to select node '{args.node_name}'. However, this DAG does not contain any node with such a name. These are the names the DAG contains: {list(dag.nodes.keys())}"
            )

        invoke_node(
            dag.nodes[args.node_name],
            input_locations={
                input_name: input_location for input_name, input_location in args.inputs
            },
            output_locations={
                output_name: output_location
                for output_name, output_location in args.outputs
            },
        )
    else:
        invoke_dag(
            dag,
            input_locations={
                input_name: input_location for input_name, input_location in args.inputs
            },
            output_locations={
                output_name: output_location
                for output_name, output_location in args.outputs
            },
        )


def call_arg_parser():
    import argparse

    parser = argparse.ArgumentParser(
        description="Run a DAG, either completely, or partially using the filters specified in the arguments"
    )
    parser.add_argument(
        "--node-name",
        type=str,
        help="Select a specific node to run. It must be properly namespaced with the name of all the parent DAGs.",
    )
    parser.add_argument(
        "--output",
        action="append",
        default=[],
        dest="outputs",
        nargs=2,
        metavar=("name", "location"),
        help="Store a given output into the location specified. Currently, we only support storing outputs in the local filesystem",
    )
    parser.add_argument(
        "--input",
        action="append",
        default=[],
        dest="inputs",
        nargs=2,
        metavar=("name", "location"),
        help="Retrieve a given input from the location specified. Currently, we only support retrieving inputs from the local filesystem",
    )
    return parser
