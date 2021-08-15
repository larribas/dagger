"""Command-line Interface to run DAGs or Tasks taking their inputs from files and storing their outputs into files."""

import logging
import sys
from typing import List

from dagger.dag import DAG
from dagger.runtime.cli.invoke import invoke_with_locations


def invoke(
    dag: DAG,
    argv: List[str] = sys.argv[1:],
):
    """
    Invoke the supplied DAG (or a node therein) retrieving the inputs from, and storing the outputs into, the specified locations.

    You can run this command with the `--help` flag to get more details about all supported arguments. Here is a non-exhaustive list:

    * `--input <name> <location>` -- Retrieve input <name> of the DAG from <location>
    * `--output <name> <location>` -- Store output <name> of the DAG into <location>
    * `--node-name <name>` (optional) -- Select a specific node of the DAG to run. If your DAG contains other nested DAGs you can access nodes using dot-notation (e.g. nested-dag-name.node-name)


    Parameters
    ----------
    dag : DAG
        DAG to execute

    argv : List of str (by default, the system's CLI arguments)
        List of arguments expected by the Command-Line Interface of this runtime.
        Check the documentation or tun `--help` for more details.


    Raises
    ------
    ValueError
        When the location of any required input/output is missing

    TypeError
        When any of the outputs cannot be obtained from the return value of their node

    SerializationError
        When some of the outputs cannot be serialized with the specified Serializer
    """
    parser = _call_arg_parser()
    args = parser.parse_args(argv)
    logging.debug(f"Arguments supplied to CLI are {args}")

    input_locations = {
        input_name: input_location for input_name, input_location in args.inputs
    }
    output_locations = {
        output_name: output_location for output_name, output_location in args.outputs
    }

    invoke_with_locations(
        dag,
        node_address=[n for n in args.node_name.split(".") if n != ""],
        input_locations=input_locations,
        output_locations=output_locations,
    )


def _call_arg_parser():
    import argparse

    parser = argparse.ArgumentParser(
        description="Run a DAG, either completely, or partially using the filters specified in the arguments"
    )
    parser.add_argument(
        "--node-name",
        type=str,
        default="",
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
