#!/usr/bin/python3

import argparse
import logging
import json
import sys

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
                    "shard_inter_ip": "127.0.0.1",
                    "shard_intra_ip": "127.0.0.1",
                    "shard_db": "testdbs{}_{}".format(coord, shard)
                }
            }
            with open(filename, 'w') as outfile:
                json.dump(data, outfile, ensure_ascii=False, indent=4)


def generate_single_config(is_coord, coord, shard, coord_ip, shard_inter_ip,
                           shard_intra_ip, coord_target_ip):
    if is_coord:
        filename = "config{}.json".format(coord)
        data = {"server":
                {
                    "server_log": "testlog{}.log".format(coord),
                    "server_shards_port": "12345".format(coord),
                    "server_coords_port": "12346".format(coord),
                    "server_db": "testdbc{}".format(coord),
                    "server_target_server": ("{}:12346"
                                             .format(coord_target_ip)),
                }}

        with open(filename, 'w') as outfile:
            json.dump(data, outfile, ensure_ascii=False, indent=4)
        json.dump(data, sys.stdout, ensure_ascii=False, indent=4)
    else:
        filename = "config_s{}_{}.json".format(coord, shard)

        data = {
            "server": {
                "server_shards_ip": coord_ip,
                "server_shards_port": "12345"
            },
            "shard": {
                "shard_log": "stestlog{}_{}.log".format(coord, shard),
                "shard_inter_port": "12350",
                "shard_intra_port": "12450",
                "shard_inter_ip": shard_inter_ip,
                "shard_intra_ip": shard_intra_ip,
                "shard_db": "testdbs{}_{}".format(coord, shard)
            }
        }
        with open(filename, 'w') as outfile:
            json.dump(data, outfile, ensure_ascii=False, indent=4)
        json.dump(data, sys.stdout, ensure_ascii=False, indent=4)


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

    if args.single:
        generate_single_config(args.coord_conf, args.coord_num,
                               args.shard_num, args.coord_ip,
                               args.shard_inter_ip, args.shard_intra_ip,
                               args.coord_target_ip)
    else:
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

    parser.add_argument('-single', '--single',
                        default=False, action="store_true",
                        help="Create a single config with some parameters")

    parser.add_argument('-sn', '--shard_num',
                        help="Shard number ")
    parser.add_argument('-cn', '--coord_num',
                        default="127.0.0.1",
                        help="Coord number")
    parser.add_argument('-siraip', '--shard_intra_ip',
                        default="127.0.0.1",
                        help="Shard Intra IP")
    parser.add_argument('-sierip', '--shard_inter_ip',
                        default="127.0.0.1",
                        help="Shard Inter IP")
    parser.add_argument('-cip', '--coord_ip',
                        help="Target coord IP")
    parser.add_argument('-tcip', '--coord_target_ip',
                        help="Coord IP")
    parser.add_argument('-coord', '--coord_conf',
                        default=False, action='store_true',
                        help="Config a corrd")

    args = parser.parse_args()
    return args


if '__main__' == __name__:
        main()
