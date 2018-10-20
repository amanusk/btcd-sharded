#!/usr/bin/python3

import argparse
import logging
import subprocess
import time
import csv
import os
import re
import spur

DEFAULT_LOG_FILE = "debug_test.log"

DEFAULT_COORD = 1


go_dir = "/home/ubuntu/go/src/github.com/btcsuite/btcd"
home_dir = "/home/ubuntu/"

def run_oracle(num_shards, num_txs):
    cmd = str(os.getcwd()) + '/btcd'
    print("Running" + cmd)
    rc = subprocess.call([cmd, "--mode=oracle", "--n=" + str(num_shards),
                          "--tx=" + str(num_txs)],
                         None, stdin=None,
                         stdout=None, stderr=None, shell=False)
    print("Return Code " + str(rc))
    return rc

def run_remote_oracle(num_shards, num_txs):
    cmd = go_dir + '/btcd'
    shell = spur.SshShell(hostname="10.0.0.11",
                          username="ubuntu",
                          private_key_file='/home/ubuntu/.ssh/aws-kp.pem',
                          missing_host_key=spur.ssh.MissingHostKey.accept,
                         )
    print("Running" + cmd + "--mode=oracle" +  "--n=" + str(num_shards) +
                          "--tx=" + str(num_txs))
    rc = shell.run([cmd, "--mode=oracle", "--n=" + str(num_shards),
                          "--tx=" + str(num_txs)])
    print("Return Code " + str(rc))
    return rc


def run_shard(server_num, shard_num, num_shards):
    cmd = str(os.getcwd()) + '/btcd'
    print("Running" + cmd)
    shard_id = "{}_{}".format(server_num, shard_num)
    p = subprocess.Popen([cmd, "--mode=shard", "--n=" + str(num_shards),
                          "--conf=config_s" + shard_id + ".json"],
                         None, stdin=None, stdout=None,
                         stderr=None, shell=False)
    return p


def run_remote_shard(server_num, shard_num, num_shards):
    cmd = go_dir + '/btcd'
    conf = home_dir + '/config_s{}_{}.json'.format(server_num, shard_num)
    shell = spur.SshShell(hostname="10.0.0.{}".format(50 + int(server_num) * 50 + int(shard_num)),
                          username="ubuntu",
                          private_key_file='/home/ubuntu/.ssh/aws-kp.pem',
                          missing_host_key=spur.ssh.MissingHostKey.accept,
                         )
    print("Running" + cmd)
    shard_id = "{}_{}".format(server_num, shard_num)
    p = shell.spawn([cmd, "--mode=shard", "--n=" + str(num_shards),
                          "--conf=" + str(conf)])
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

def run_remote_server(server_num, num_shards, bootstrap):
    cmd = go_dir + '/btcd'
    conf = home_dir + '/config{}.json'.format(server_num)
    shell = spur.SshShell(hostname="10.0.0.1{}".format(server_num),
                          username="ubuntu",
                          private_key_file='/home/ubuntu/.ssh/aws-kp.pem',
                          missing_host_key=spur.ssh.MissingHostKey.accept,
                         )
    print("Running" + cmd)
    boot = ""
    if bootstrap:
        boot = "--bootstrap"
    p = shell.spawn([cmd, "--mode=server", "--n=" + str(num_shards),
                          "--conf=" + str(conf), boot])
    return p


def run_n_shard_node(coord_num, n, bootstrap, num_txs):
    processes = list()
    coord_process = run_server(coord_num, n, bootstrap=bootstrap)
    processes.append(coord_process)
    time.sleep(1)
    for i in range(n):
        run_remote_shard(coord_num, i, n)
        # processes.append(p)
        time.sleep(1)

    if bootstrap:
        run_remote_oracle(n, num_txs)

    return processes

def run_remote_n_shard_node(coord_num, n, bootstrap, num_txs):
    processes = list()
    run_remote_server(coord_num, n, bootstrap=bootstrap)
    #processes.append(coord_process)
    time.sleep(1)
    for i in range(n):
        run_remote_shard(coord_num, i, n)
        #processes.append(p)
        time.sleep(1)

    if bootstrap:
        run_remote_oracle(n, num_txs)

    return processes


def clean_with_ssh(num_coords, num_shards):
    for c in range(1, num_coords+1):
        cmd = go_dir + '/kill_all.sh'
        shell = spur.SshShell(hostname="10.0.0.1{}".format(c),
                              username="ubuntu",
                              private_key_file='/home/ubuntu/.ssh/aws-kp.pem',
                              missing_host_key=spur.ssh.MissingHostKey.accept,
                             )
        print("Running" + cmd)
        p = shell.run([cmd])
        try:
            p = shell.run(["rm", "/home/ubuntu/testlog{}.log".format(c)])
            print(p)
        except:
            pass
        for s in range(num_shards):
            shell = spur.SshShell(hostname="10.0.0.{}".format(50 + int(c) * 50 + int(s)),
                                  username="ubuntu",
                                  private_key_file='/home/ubuntu/.ssh/aws-kp.pem',
                                  missing_host_key=spur.ssh.MissingHostKey.accept,
                                 )
            p = shell.run([cmd])
            p = shell.run(["rm", "/home/ubuntu/stestlog{}_{}.log".format(c,s)])
            print(p)


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

def kill_all_remote_prcesses(p_list):
    [p.send_signal(9) for p in p_list]


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
                    re.findall("\d+\.\d+", (line.split(" ")[4]))[0])
                if "ms" in line:
                    blocktime = float(blocktime/1000)
                print(blocktime)
                return blocktime

    def run_test(num_coords, num_shards, num_txs):
        p_list = run_remote_n_shard_node(DEFAULT_COORD, num_shards,
                                  bootstrap=True, num_txs=num_txs)
        for i in range(num_coords - 1):
            p_list += (run_n_shard_node(i + 2, num_shards,
                                        bootstrap=False, num_txs=num_txs))

        # About the expected time to finish processing
        time.sleep(30)

        #if scan_log_files(num_coords, num_shards):
        #    logging.debug("An error detected in one of the files")


        top_block_time = get_top_block_time(num_coords)
        print("Top block took {} s to process".format(top_block_time))

        #remove_log_files(num_coords, num_shards)

        clean_with_ssh(num_coords, num_shards)

        path = os.getcwd() + '/testlog{}.log'.format(num_coords)
        print("Clearing " + path)
        os.remove(path)

        return top_block_time

    for num_shards in range(7, 9):
        csv_file_name = "proc_{}_shards.csv".format(num_shards)
        with open(csv_file_name, 'w') as csvfile:
            fieldnames = ['Txs', 'Time']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()  # file doesn't exist yet, write a header

            for num_txs in range(10000, 110000, 10000):
                block_time = run_test(2, num_shards, num_txs)
                d = {'Time': block_time, 'Txs': num_txs}
                writer.writerow(d)
                csvfile.flush()
                if int(block_time > 15):
                    break


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

    run_multi_tests()

    # This runs a single node, and makes sure the coordinator + orcacle work
    # p_list = run_n_shard_node(DEFAULT_COORD, 1, True, 1000)
    # kill_all_prcesses(p_list)

    # for line in lines:
    #     print(line)

    # Try with 2 shards
    # p_list = run_n_shard_node(DEFAULT_COORD, 2, bootstrap=True)
    # kill_all_prcesses(p_list)

    # Try with 2 nodes, 2 shards
    # p_list = run_remote_n_shard_node(DEFAULT_COORD, 2, True, 10000)
    # p2_list = run_n_shard_node(2, 2, False, 10000)
    # time.sleep(10)
    # clean_with_ssh(2, 2)
    # kill_all_remote_prcesses(p_list)
    # kill_all_prcesses(p2_list)

    # time.sleep(5)

    # if scan_log_files(2, 2):
    #     logging.debug("An error detected in one of the files")

    # kill_all_prcesses(p_list)
    # kill_all_prcesses(p2_list)

    # run_n_shard_node(2, 3, False)


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
    args = parser.parse_args()
    return args


if '__main__' == __name__:
    main()

