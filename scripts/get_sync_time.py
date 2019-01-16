#!/usr/bin/python3
import argparse
import csv
import os
import re

def get_top_block_time(filename, keyword):
    filename = (os.getcwd() + "/" + filename)
    for line in reverse_readline(filename):
        if keyword in line:
            blocktime = None
            try:
                time = re.findall(r"\dm\d+\.\d+", line)[0]
                minute, second = time.split("m")
                blocktime = float(minute) * 60 + float(second)
            except:
                time = re.findall(r"\d+\.\d+", line)[0]
                blocktime = float(time)
            return blocktime


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


def main():
    args = get_args()
    if args.single:
        result = get_top_block_time(args.filename, args.keyword)
        print(result)
        return
    num_shards = args.num_shards
    num_txs = args.transactions
    csv_file_name = "proc_{}_shards.csv".format(num_shards)
    file_exists = os.path.isfile(csv_file_name)
    with open(csv_file_name, 'a') as csvfile:
        fieldnames = ['Txs', 'Time']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()  # file doesn't exist yet, write a header

        sync_time = get_top_block_time("testlog1.log", "Block")
        print(sync_time)
        d = {'Time': sync_time, 'Txs': num_txs}
        writer.writerow(d)


def get_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('-n', '--num_shards',
                        default=1,
                        help="Number of shards to run")
    parser.add_argument('-tx', '--transactions',
                        default=1,
                        help="Number of transactions in block")
    parser.add_argument('-single', '--single',
                        default=False,
                        action='store_true',
                        help="Only get a single stat")
    parser.add_argument('-keyword', '--keyword',
                        default="",
                        help="Keyword to search in file")
    parser.add_argument('-filename', '--filename',
                        default="",
                        help="Log file to search")
    args = parser.parse_args()
    return args


if '__main__' == __name__:
    main()
