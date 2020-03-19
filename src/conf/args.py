import argparse
import sys


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--work_dir", help="work dir", default="upp")
    parser.add_argument(
        "--bucket_prefix", help="bucket prefix", default="universal_pricing_platform"
    )
    parser.add_argument("--input_path", help="input path", required=False)
    parser.add_argument("--data_path", help="data path", required=False)
    parser.add_argument("--output_format", help="output format", required=False)
    # TODO remove this hack. this is a param used by runner
    parser.add_argument("--module", help="module to run", default="")
    args = sys.argv[1:] if args is None else args
    return parser.parse_args(args)