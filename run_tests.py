#!/usr/bin/python3

import argparse
import logging
import subprocess
import time
import os

DEFAULT_LOG_FILE = "debug_test.log"

DEFAULT_COORD = 1


def run_oracle(num_shards):
    cmd = str(os.getcwd()) + '/btcd'
    print("Running" + cmd)
    rc = subprocess.call([cmd, "--mode=oracle", "--n=" + str(num_shards)],
                         None, stdin=None,
                         stdout=None, stderr=None, shell=False)
    print("Return Code " + str(rc))
    return rc


def run_shard(server_num, shard_num, num_shards):
    cmd = str(os.getcwd()) + '/btcd'
    print("Running" + cmd)
    shard_num = str(server_num) + str(shard_num)
    p = subprocess.Popen([cmd, "--mode=shard", "--n=" + str(num_shards),
                          "--conf=config_s" + str(shard_num) + ".json"],
                         None, stdin=None, stdout=None,
                         stderr=None, shell=False)
    return p


def run_server(server_num, num_shards, bootstrap):
    cmd = str(os.getcwd()) + '/btcd'
    print("Running" + cmd)
    boot = ""
    if bootstrap:
        boot = "--bootstrap"
    p = subprocess.Popen([cmd, "--mode=server", "--n=" + str(num_shards),
                          "--conf=config" + str(server_num) + ".json",
                          boot],
                         None, stdin=None, stdout=None,
                         stderr=None, shell=False)
    return p


def run_n_shard_node(coord_num, n, bootstrap):
    processes = list()
    coord_process = run_server(coord_num, n, bootstrap=bootstrap)
    processes.append(coord_process)
    time.sleep(1)
    for i in range(n):
        p = run_shard(coord_num, i, n)
        processes.append(p)
        time.sleep(1)

    if bootstrap:
        run_oracle(n)

    return processes


def remove_log_files(num_coords, num_shards):
    def clear_oracle_log():
        path = os.getcwd() + '/otestlog.log'
        print("Clearing " + path)
        os.remove(path)

    def clear_shard_logs(coord_num, num_shards):
        for shard_num in range(num_shards):
            path = (os.getcwd() + '/stestlog' + str(coord_num) +
                    str(shard_num) + ".log")
            print("Clearing " + path)
            os.remove(path)

    def clear_node_logs(coord_num, num_shards):
        path = (os.getcwd() + '/testlog' + str(coord_num) + ".log")
        print("Clearing " + path)
        os.remove(path)
        clear_shard_logs(coord_num, num_shards)

    try:
        clear_oracle_log()
    except (FileNotFoundError):
        pass

    for coord_num in range(num_coords):
        clear_node_logs(coord_num + 1, num_shards)


def kill_all_prcesses(p_list):
    for p in p_list:
        p.kill()


def scan_log_files(num_coords, num_shards):
    def error_found(filename):
        with open(filename, 'r') as logfile:
            for line in logfile:
                if "BADBLOCK" in line:
                    return True
        return False

    def scan_oracle_log():
        path = os.getcwd() + '/otestlog.log'
        return error_found(path)

    def scan_shard_logs(coord_num, num_shards):
        error = False
        for shard_num in range(num_shards):
            path = (os.getcwd() + '/stestlog' + str(coord_num) +
                    str(shard_num) + ".log")
            error |= error_found(path)
        return error

    def scan_node_logs(coord_num, num_shards):
        path = (os.getcwd() + '/testlog' + str(coord_num) + ".log")
        print("Clearing " + path)
        error = error_found(path)
        error |= scan_shard_logs(coord_num, num_shards)
        return error

    try:
        error = scan_oracle_log()
    except (FileNotFoundError):
        pass

    if error:
        return error

    for coord_num in range(num_coords):
        return scan_node_logs(coord_num + 1, num_shards)


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

    # # This runs a single node, and makes sure the coordinator + orcacle work
    # p_list = run_n_shard_node(DEFAULT_COORD, 1, bootstrap=True)
    # kill_all_prcesses(p_list)

    # if scan_log_files(2, 3):
        # logging.debug("An error detected in one of the files")

    # Try with 2 shards
    p_list = run_n_shard_node(DEFAULT_COORD, 2, bootstrap=True)
    kill_all_prcesses(p_list)

    # Try with 3 shards
    # p_list = run_n_shard_node(DEFAULT_COORD, 2, bootstrap=True)
    # p2_list = run_n_shard_node(2, 2, False)

    # time.sleep(5)

    # if scan_log_files(2, 2):
    #     logging.debug("An error detected in one of the files")

    # kill_all_prcesses(p_list)
    # kill_all_prcesses(p2_list)


def get_args():

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('-d', '--debug',
                        default=False, action='store_true',
                        help="Output debug log to _s-tui.log")
    parser.add_argument('-n', '--num_shards',
                        default=1,
                        help="Number of shards to run")
    parser.add_argument('-c', '--coordinator',
                        default=1,
                        help="Corrdinator index")
    parser.add_argument('-b', '--bootstrap',
                        default=1,
                        help="Add when starting the first node")
    args = parser.parse_args()
    return args


if '__main__' == __name__:
        main()
