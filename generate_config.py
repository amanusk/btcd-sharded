#!/usr/bin/python3

import argparse
import logging
import json

DEFAULT_LOG_FILE = "debug_json.log"


def generate_configs(num_coords, num_shards):
    for coord_num in range(num_coords):
        coord = coord_num + 1
        filename = "config{}.json".format(coord)
        data = {"server":
                {
                    "server_log": "testlog{}.log".format(coord),
                    "server_shards_port": "{}2345".format(coord),
                    "server_coords_port": "{}2346".format(coord),
                    "server_db": "testdbc{}".format(coord),
                    "server_target_server": ("127.0.0.1:{}2346"
                                             .format(coord_num)),
                }}

        with open(filename, 'w') as outfile:
            json.dump(data, outfile, ensure_ascii=False, indent=4)

        # Create config per shard
        for shard in range(num_shards):
            filename = "config_s{}_{}.json".format(coord, shard)

            data = {
                "server": {
                    "server_shards_ip": "127.0.0.1",
                    "server_shards_port": "{}2345".format(coord)
                },
                "shard": {
                    "shard_log": "stestlog{}_{}.log".format(coord, shard),
                    "shard_inter_port": "{}23{}".format(coord, 50 + shard),
                    "shard_intra_port": "{}24{}".format(coord, 50 + shard),
                    "shard_ip": "127.0.0.1",
                    "shard_db": "testdbs{}_{}".format(coord, shard)
                }
            }
            with open(filename, 'w') as outfile:
                json.dump(data, outfile, ensure_ascii=False, indent=4)


def main():
    args = get_args()

    # Setup logging util
    global log_file
    level = ""
    log_file = DEFAULT_LOG_FILE
    if args.debug:
        level = logging.DEBUG
        log_file = args.debug_file
        log_formatter = logging.Formatter(
            "%(asctime)s [%(funcName)s()] [%(levelname)-5.5s]  %(message)s")
        root_logger = logging.getLogger()
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(log_formatter)
        root_logger.addHandler(file_handler)
        root_logger.setLevel(level)

    generate_configs(int(args.coords), int(args.num_shards))


def get_args():

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('-d', '--debug',
                        default=False, action='store_true',
                        help="Output debug log to _s-tui.log")
    parser.add_argument('-n', '--num_shards',
                        default=1,
                        help="Number of shards to run")
    parser.add_argument('-c', '--coords',
                        default=1,
                        help="Corrdinator index")
    args = parser.parse_args()
    return args


if '__main__' == __name__:
        main()
