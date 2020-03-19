import logging

from src.conf.args import parse_args


class Settings:
    def __init__(self, data_path, input_path, output_format):
        self.data_path = data_path
        self.output_data_path = self.data_path + "/output_data"
        self.input_path = input_path
        self.input_data_path = self.input_path + "/data/raw"
        self.processed_files_path = self.data_path + "/processed"
        self.external_files_path = self.data_path + "/external"
        self.interim_files_path = self.data_path + "/interim"
        self.output_format = output_format


def load_settings(args=None):
    if args is None:
        args = parse_args()
    data_path = args.data_path or "gs://{bucket_prefix}-data/{work_dir}".format(
        bucket_prefix=args.bucket_prefix, work_dir=args.work_dir
    )
    input_path = args.input_path or "gs://{bucket_prefix}-input".format(
        bucket_prefix=args.bucket_prefix
    )
    settings = Settings(
        data_path=data_path,
        input_path=input_path,
        output_format=args.output_format or "avro",
    )
    logging.info("load settings {}".format(settings))
    return settings
