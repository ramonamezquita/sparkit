import argparse
import importlib

_CLI_DOCSTRING = """
Entrypoint for running PySpark jobs.

Job arguments are given using the `--job-args` option as
a list of space-separated key=value pairs. Example:

    --job-args key1=value1 key2=value2 key3=value3
"""


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=_CLI_DOCSTRING,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--job",
        type=str,
        dest="job_name",
        help="<Required> Name of the job to run.",
        required=True,
    )

    parser.add_argument(
        "--job-args",
        nargs="*",
        help="List of space-separated key=value pairs.",
    )

    return parser


def resolve_args(args: list[str] | None = None) -> dict:

    if args is None:
        return {}

    job_args_tuples = [arg_str.split("=") for arg_str in args]
    return {a[0]: a[1] for a in job_args_tuples}


def main():
    """Runs a job."""

    parser = create_parser()
    parsed_args, unknown = parser.parse_known_args()

    # Resolve user args.
    job_name = parsed_args.job_name
    job_args = resolve_args(args=parsed_args.job_args)
    print(f"Called job `{job_name}` with arguments: {job_args}")

    job_module = importlib.import_module(f"jobs.{job_name}")

    try:
        result = job_module.run(**job_args)
    except Exception as exc:
        raise exc


if __name__ == "__main__":
    main()