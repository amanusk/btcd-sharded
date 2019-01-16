#!/usr/bin/python3

import argparse
import logging
import subprocess
import time
import csv
import os
import re
import spur
import gc

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


def run_remote_oracle(num_shards, num_txs, network):
    cmd = go_dir + '/btcd'
    shell = spur.SshShell(hostname="10.0.0.11",
                          username="ubuntu",
                          private_key_file='/home/ubuntu/.ssh/aws-kp.pem',
                          missing_host_key=spur.ssh.MissingHostKey.accept)
    if network == "regress":
        rc = shell.run([cmd, "--mode=oracle", "--n=" + str(num_shards),
                        "--tx=" + str(num_txs), "--network="+network])
    else:
        rc = shell.run([cmd, "--mode=full", "--n=" + str(num_shards),
                        "--tx=" + str(num_txs), "--network="+network])
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


def run_remote_shard(server_num, shard_num, num_shards, network):
    cmd = go_dir + '/btcd'
    print("server ", server_num)
    print("server ", server_num)
    conf = home_dir + '/config_s{}_{}.json'.format(server_num, shard_num)
    host = "10.0.0.{}".format(50 + int(server_num) * 50 + int(shard_num))
    print("Host " + host)
    shell = spur.SshShell(hostname=host,
                          username="ubuntu",
                          private_key_file='/home/ubuntu/.ssh/aws-kp.pem',
                          missing_host_key=spur.ssh.MissingHostKey.accept)
    cmd = [cmd, "--mode=shard", "--n=" + str(num_shards),
           "--conf=" + str(conf), "--network="+network]
    print("Running " + " ".join(cmd))
    p = shell.spawn(cmd)
    return p


def run_server(server_num, num_shards, bootstrap):
    cmd = str(os.getcwd()) + '/btcd'
    print("Running " + cmd)
    boot = ""
    if bootstrap:
        boot = "--bootstrap"
    p = subprocess.Popen([cmd, "--mode=server", "--n=" + str(num_shards),
                          "--conf=config" + str(server_num) + ".json",
                          boot],
                         None, stdin=None, stdout=None,
                         stderr=None, shell=False)
    return p


def run_remote_server(server_num, num_shards, bootstrap, network):
    cmd = go_dir + '/btcd'
    conf = home_dir + '/config{}.json'.format(server_num)
    host = "10.0.0.1{}".format(server_num)
    print("Host " + host)
    shell = spur.SshShell(hostname=host,
                          username="ubuntu",
                          private_key_file='/home/ubuntu/.ssh/aws-kp.pem',
                          missing_host_key=spur.ssh.MissingHostKey.accept)
    boot = ""
    if bootstrap:
        boot = "--bootstrap"
    cmd = [cmd, "--mode=server", "--n=" + str(num_shards),
           "--conf=" + str(conf), boot, "--network=" + network]
    print("Running " + " ".join(cmd))
    p = shell.spawn(cmd)
    return p


def run_n_shard_node(coord_num, n, bootstrap, num_txs, network):
    processes = list()
    coord_process = run_server(coord_num, n, bootstrap=bootstrap)
    processes.append(coord_process)
    time.sleep(1)
    for i in range(n):
        run_remote_shard(coord_num, i, n, network)
        # processes.append(p)
        # time.sleep(1)

    if bootstrap:
        run_remote_oracle(n, num_txs, network)

    return processes


def run_remote_n_shard_node(coord_num, n, bootstrap, num_txs, network):
    processes = list()
    run_remote_server(coord_num, n, bootstrap=bootstrap, network=network)
    # processes.append(coord_process)
    time.sleep(1)
    for i in range(n):
        run_remote_shard(coord_num, i, n, network)
        # processes.append(p)
        # time.sleep(1)

    if bootstrap:
        run_remote_oracle(n, num_txs, network)

    return processes


def clean_with_ssh(num_coords, num_shards):
    for c in range(1, num_coords+1):
        try:
            shell = spur.SshShell(hostname="10.0.0.1{}".format(c),
                                  username="ubuntu",
                                  private_key_file='/home/ubuntu/.ssh/aws-kp.pem',
                                  missing_host_key=spur.ssh.MissingHostKey.accept)
            cmd = go_dir + '/kill_all.sh'
            print("Running" + cmd)
            p = shell.run([cmd])
            cmd = go_dir + '/clean_db.sh'
            print("Running" + cmd)
            p = shell.run([cmd])
        except:
            pass
        try:
            p = shell.run(["rm", "/home/ubuntu/testlog{}.log".format(c)])
            print(p)
        except:
            pass
        for s in range(num_shards):
            try:
                shell = spur.SshShell(hostname="10.0.0.{}".format(50 + int(c) * 50 + int(s)),
                                      username="ubuntu",
                                      private_key_file='/home/ubuntu/.ssh/aws-kp.pem',
                                      missing_host_key=spur.ssh.MissingHostKey.accept)
                cmd = go_dir + '/kill_all.sh'
                print("Running" + cmd)
                p = shell.run([cmd])
                print(p)
                cmd = go_dir + '/clean_db.sh'
                print("Running" + cmd)
                p = shell.run([cmd])
                print(p)
                p = shell.run(["rm", "/home/ubuntu/stestlog{}_{}.log".format(c, s)])
                print(p)
            except:
                pass


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


def get_remote_result(num_shards, num_txs, filename, keyword, host):
    script = go_dir + '/scripts/get_sync_time.py'
    shell = spur.SshShell(hostname=host,
                          username="ubuntu",
                          private_key_file='/home/ubuntu/.ssh/aws-kp.pem',
                          missing_host_key=spur.ssh.MissingHostKey.accept)
    # rc = shell.run(["python3", script, "-n={}".format(str(num_shards)),
    #                 "-tx={}".format(str(num_txs))])
    rc = shell.run(["python3", script, "--single", "--filename={}".format(filename),
                    "--keyword={}".format(keyword)])
    print(rc.output)
    return(rc.output)

def collect_to_csv(num_shards, coord, num_txs):

    def get_result(keyword, coord_num):
        filename = (os.getcwd() + "/testlog{}.log".format(coord_num))
        # print(filename)
        for line in reverse_readline(filename):
            if keyword in line:
                result = re.findall(r"\d+\.\d+", line)[0]
                print(keyword + "took "+ str(result))
                if "ms" in line:
                    result = float(float(result)/float(1000))
                else:
                    result = float(result)
                return result

    csv_file_name = "{}_shards.csv".format(num_shards)
    file_exists = os.path.isfile(csv_file_name)
    with open(csv_file_name, 'a') as csvfile:
        fieldnames = ['Txs',
                      'FetchingBlockFromDB',
                      'ConstructingToSend',
                      'Sending',
                      'OutputFetch',
                      'ReqTxOuts',
                      'SendReqTxOuts',
                      'CheckInputs',
                      'CheckSigs',
                      'Total',
                      'Remote']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()  # file doesn't exist yet, write a header

        fetch_from_db = get_remote_result(num_shards, num_txs, 'stestlog1_0.log', 'FetchingBlockFromDB', '10.0.0.100')
        fetch_from_db = float(fetch_from_db)

        const_to_send = get_remote_result(num_shards, num_txs, 'stestlog1_0.log', 'ConstructingToSend', '10.0.0.100')
        const_to_send = float(const_to_send)

        sending = get_remote_result(num_shards, num_txs, 'stestlog1_0.log', 'Sending', '10.0.0.100')
        sending = float(sending)

        output_fetch = get_remote_result(num_shards, num_txs, 'stestlog2_0.log', 'OutputFetch', '10.0.0.150')
        output_fetch = float(output_fetch)

        req_tx_outs = get_remote_result(num_shards, num_txs, 'stestlog2_0.log', 'ReqTxOuts', '10.0.0.150')
        req_tx_outs = float(req_tx_outs)

        send_req_tx_outs = get_remote_result(num_shards, num_txs, 'stestlog2_0.log', 'SendReqTxOuts', '10.0.0.150')
        send_req_tx_outs = float(send_req_tx_outs)

        check_inputs = get_remote_result(num_shards, num_txs, 'stestlog2_0.log', 'CheckInputs', '10.0.0.150')
        check_inputs = float(check_inputs)

        check_sigs = get_remote_result(num_shards, num_txs, 'stestlog2_0.log', 'CheckSigs', '10.0.0.150')
        check_sigs = float(check_sigs)

        remote_send = get_remote_result(num_shards, num_txs, 'testlog1.log', 'Block', '10.0.0.11')
        remote_send = float(remote_send)

        total = get_result("Block", coord)
        d = {'Txs': num_txs,
             'FetchingBlockFromDB': fetch_from_db,
             'ConstructingToSend': const_to_send,
             'Sending': sending,
             'OutputFetch': output_fetch,
             'ReqTxOuts': req_tx_outs,
             'SendReqTxOuts': send_req_tx_outs,
             'CheckInputs': check_inputs,
             'CheckSigs': check_sigs,
             'Total': total,
             'Remote': remote_send}
        writer.writerow(d)
        csvfile.flush()

def run_multi_tests(network):


    def run_test(num_coords, num_shards, num_txs, network):
        p_list = run_remote_n_shard_node(DEFAULT_COORD, num_shards,
                                         bootstrap=True, num_txs=num_txs,
                                         network=network)
        for i in range(num_coords - 1):
            p_list += (run_n_shard_node(i + 2, num_shards,
                                        bootstrap=False, num_txs=num_txs,
                                        network=network))

        # About the expected time to finish processing
        time.sleep((20 - num_shards) * (num_txs/50000) + 20)
        # time.sleep(10)

        #if scan_log_files(num_coords, num_shards):
        #    logging.debug("An error detected in one of the files")

        # remote_result = get_remote_result(num_shards, num_txs)
        # print("Block send took " + str(float(remote_result)))

        collect_to_csv(num_shards, 2, num_txs)

        # remove_log_files(num_coords, num_shards)

        clean_with_ssh(num_coords, num_shards)

        try:
            path = os.getcwd() + '/testlog{}.log'.format(num_coords)
            print("Clearing " + path)
            os.remove(path)
            cmd = str(os.getcwd()) + '/clean.sh'
            print("Running clean")
            subprocess.call([cmd], None, stdin=None,
                            stdout=None, stderr=None, shell=False)

        except:
            pass


    options = [2**i for i in range(3, -1, -1)]
    for num_shards in options:
        for num_txs in range(1000000, 1100000, 200000):
            for j in range(1):
                run_test(2, num_shards, num_txs, network)
                gc.collect()


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

    run_multi_tests(args.network)
    # collect_to_csv(15, 2, 300000)

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
    parser.add_argument('-net', '--network',
                        default="regress",
                        help="Which network to test")
    args = parser.parse_args()
    return args


if '__main__' == __name__:
    main()
