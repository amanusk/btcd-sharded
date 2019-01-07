#!/usr/bin/python3

import argparse
import logging
import subprocess
import time
import csv
import os
import re

DEFAULT_LOG_FILE = "debug_test.log"

DEFAULT_COORD = 1


def run_oracle(num_shards, num_txs, network):
    cmd = str(os.getcwd()) + '/btcd'
    if network == "regress":
        cmd = [cmd, "--mode=oracle", "--n=" + str(num_shards),
               "--tx=" + str(num_txs), "--conf=config_full.json",
               "--network="+network]
        print("Running ", " ".join(cmd))
        rc = subprocess.call(cmd, None, stdin=None,
                             stdout=None, stderr=None, shell=False)
    else:
        cmd = [cmd, "--mode=full", "--n=" + str(num_shards),
               "--tx=" + str(num_txs), "--conf=config_full.json",
               "--network="+network]
        print("Running ", " ".join(cmd))
        rc = subprocess.call(cmd, None, stdin=None,
                             stdout=None, stderr=None, shell=False)

    print("Return Code " + str(rc))
    return rc


def run_shard(server_num, shard_num, num_shards, network):
    cmd = str(os.getcwd()) + '/btcd'
    shard_id = "{}_{}".format(server_num, shard_num)
    cmd = [cmd, "--mode=shard", "--n=" + str(num_shards),
           "--conf=config_s" + shard_id + ".json", "--network=" + network]
    print("Running "+ " ".join(cmd))
    p = subprocess.Popen(cmd, None, stdin=None, stdout=None,
                         stderr=None, shell=False)
    return p


def run_server(server_num, num_shards, bootstrap, network):
    cmd = str(os.getcwd()) + '/btcd'
    print("Running" + cmd)
    boot = ""
    if bootstrap:
        boot = "--bootstrap"
    cmd = [cmd, "--mode=server", "--n=" + str(num_shards),
           "--conf=config" + str(server_num) + ".json",
           boot, "--network="+network]
    print("Running " + " ".join(cmd))
    p = subprocess.Popen(cmd, None, stdin=None, stdout=None,
                         stderr=None, shell=False)
    return p


def run_n_shard_node(coord_num, n, bootstrap, num_txs, network):
    processes = list()
    coord_process = run_server(coord_num, n, bootstrap=bootstrap,
                               network=network)
    processes.append(coord_process)
    time.sleep(1)
    for i in range(n):
        p = run_shard(coord_num, i, n, network)
        processes.append(p)
        time.sleep(1)

    if bootstrap:
        run_oracle(n, num_txs, network)

    return processes


def remove_log_files(num_coords, num_shards):
    def clear_oracle_log():
        path = os.getcwd() + '/otestlog.log'
        print("Clearing " + path)
        os.remove(path)

    def clear_shard_logs(coord_num, num_shards):
        for shard_num in range(num_shards):
            path = (os.getcwd() + '/stestlog' + str(coord_num) + "_" +
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
    [p.kill() for p in p_list]


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
                    "_" + str(shard_num) + ".log")
            error |= error_found(path)
            return error

    def scan_node_logs(coord_num, num_shards):
        path = (os.getcwd() + '/testlog' + str(coord_num) + ".log")
        print("Scanning " + path)
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


def reverse_readline(filename, buf_size=8192):
    """a generator that returns the lines of a file in reverse order"""
    with open(filename) as fh:
        segment = None
        offset = 0
        fh.seek(0, os.SEEK_END)
        file_size = remaining_size = fh.tell()
        while remaining_size > 0:
            offset = min(file_size, offset + buf_size)
            fh.seek(file_size - offset)
            buffer = fh.read(min(remaining_size, buf_size))
            remaining_size -= buf_size
            lines = buffer.split('\n')
            # the first line of the buffer is probably not a complete line so
            # we'll save it and append it to the last line of the next buffer
            # we read
            if segment is not None:
                # if the previous chunk starts right from the beginning of line
                # do not concact the segment to the last line of new chunk
                # instead, yield the segment first
                if buffer[-1] is not '\n':
                    lines[-1] += segment
                else:
                    yield segment
            segment = lines[0]
            for index in range(len(lines) - 1, 0, -1):
                if len(lines[index]):
                    yield lines[index]
        # Don't yield None if the file was empty
        if segment is not None:
            yield segment


def run_multi_tests():
    def get_top_block_time(coord_num):
        filename = (os.getcwd() + "/testlog{}.log".format(coord_num))
        for line in reverse_readline(filename):
            if "took" in line:
                print(line)
                blocktime = float(
                    re.findall(r"\d+\.\d+", (line.split(" ")[4]))[0])
                print(blocktime)
                return blocktime

    def run_test(num_coords, num_shards, num_txs):
        p_list = run_n_shard_node(DEFAULT_COORD, num_shards,
                                  bootstrap=True, num_txs=num_txs)
        for i in range(num_coords - 1):
            p_list += (run_n_shard_node(i + 2, num_shards,
                                        bootstrap=False, num_txs=num_txs))

        # About the expected time to finish processing
        time.sleep(10)

        if scan_log_files(num_coords, num_shards):
            logging.debug("An error detected in one of the files")

        kill_all_prcesses(p_list)

        cmd = str(os.getcwd()) + '/clean.sh'
        print("Running clean")
        subprocess.call([cmd], None, stdin=None,
                        stdout=None, stderr=None, shell=False)

        top_block_time = get_top_block_time(num_coords)
        print("Top block took {} s to process".format(top_block_time))
        remove_log_files(num_coords, num_shards)
        return top_block_time

    for num_shards in range(1, 3):
        csv_file_name = "proc_{}_shards.csv".format(num_shards)
        with open(csv_file_name, 'w') as csvfile:
            fieldnames = ['Txs', 'Time']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()  # file doesn't exist yet, write a header

            for num_txs in range(1000, 5000, 1000):
                block_time = run_test(2, num_shards, num_txs)
                d = {'Time': block_time, 'Txs': num_txs}
                writer.writerow(d)
                csvfile.flush()


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

    # run_multi_tests()
    network = args.network
    # # This runs a single node, and makes sure the coordinator + orcacle work
    # p_list = run_n_shard_node(DEFAULT_COORD, 1, bootstrap=True,
    #                           num_txs=1000, network=network)
    # kill_all_prcesses(p_list)
    # if scan_log_files(2, 2):
    #     logging.debug("An error detected in one of the files")
    #     exit(1)

    # # # for line in lines:
    # # #     print(line)

    # cmd = str(os.getcwd()) + '/clean.sh'
    # print("Running clean")
    # subprocess.call([cmd], None, stdin=None,
    #                 stdout=None, stderr=None, shell=False)

    # # Try with 2 shards
    # p_list = run_n_shard_node(DEFAULT_COORD, 2, bootstrap=True,
    #                           num_txs=1000, network=network)
    # kill_all_prcesses(p_list)

    # cmd = str(os.getcwd()) + '/clean.sh'
    # print("Running clean")
    # subprocess.call([cmd], None, stdin=None,
    #                 stdout=None, stderr=None, shell=False)

    # Try with 2 nodes, 2 shards
    p_list = run_n_shard_node(DEFAULT_COORD, 2, bootstrap=True,
                              num_txs=100, network=network)
    p2_list = run_n_shard_node(2, 2, bootstrap=False, num_txs=100,
                                network=network)

    time.sleep(100)

    # if scan_log_files(2, 2):
    #     logging.debug("An error detected in one of the files")
    #     exit(1)

    # kill_all_prcesses(p_list)
    # kill_all_prcesses(p2_list)

    # cmd = str(os.getcwd()) + '/clean.sh'
    # print("Running clean")
    # subprocess.call([cmd], None, stdin=None,
    #                stdout=None, stderr=None, shell=False)


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
    parser.add_argument('-tx', '--transactions',
                        default=100,
                        help="How many transactions in the main block")
    parser.add_argument('-net', '--network',
                        default="regress",
                        help="Which network to test")
    args = parser.parse_args()
    return args


if '__main__' == __name__:
    main()
