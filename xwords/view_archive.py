#!/usr/bin/env python

# This is a small example to show how to download the data that powers this archive:
# https://q726kbxun.github.io/xwords/xwords.html

from urllib.request import urlopen, Request
import gzip, json, os, sys

# Simple wrapper for command line parsing
_commands = []
def cmd(cmd, args, desc):
    def helper(func):
        _commands.append({"cmd": cmd, "args": args, "desc": desc, "func": func})
        def wrapper(*args2, **kwargs):
            return func(*args2, **kwargs)
        return wrapper
    return helper

# This is the main helper that grabs data from github
_cache = {}
def get_data(num, start, len, mode='json', header=None, cache=False):
    if cache:
        # When running through all the files, store the data in a local cache to prevent
        # thousands of calls
        if num not in _cache:
            print(f"Getting data for archive {num}...")
            _cache[num] = urlopen(f"https://q726kbxun.github.io/xwords/xwords_data_{num:02d}.dat").read()
        data = _cache[num][start:start+len]
    else:
        # Otherwise, just request the exact bytes we need
        req = Request(
            f"https://q726kbxun.github.io/xwords/xwords_data_{num:02d}.dat",
            headers={
                'Range': f'bytes={start}-{start+len-1}',
            }
        )
        resp = urlopen(req)
        data = resp.read()

    if header is not None:
        # The compressed data is stored without the gzip header to save some space,
        # Add the header back in when we want to decompress it
        data = header + data

    if mode == 'json':
        # This mode just parses json data
        return json.loads(data)
    elif mode == 'raw':
        # Return the raw data
        return data
    elif mode == 'gzip':
        # Decompress, and parse json data
        data = gzip.decompress(data)
        return json.loads(data)
    else:
        raise Exception()

@cmd("show_all", 0, "= Show all available puzzles")
def show_puzzles():
    # Get the meta data
    meta = get_data(0, 22, 78)
    # Then from the metadata, we pull in the gzip header info
    header = get_data(*meta[5:8], mode='raw')
    # And finally the list of all available items
    data = get_data(*meta[2:5], mode='gzip', header=header)

    # Just enumerate through the items and show each one
    total = 0
    for xword, years in data.items():
        for year, months in years.items():
            for month, days in months.items():
                for puz in days:
                    total += 1
                    print(f"Number: {total:,}, Source: {xword}, Puzzle: {year}-{month}-{puz}")

@cmd("dump_all", 0, "= Download and dump out all puzzles")
def dump_all_puzzles():
    # Note that this will write out around 1.5gb of data

    # Get the meta data
    meta = get_data(0, 22, 78)
    # Then from the metadata, we pull in the gzip header info
    header = get_data(*meta[5:8], mode='raw')
    # And finally the list of all available items
    data = get_data(*meta[2:5], mode='gzip', header=header)

    # Now enumerate through all the items
    for xword, years in data.items():
        for year, months in years.items():
            for month, days in months.items():
                for puz, info in days.items():
                    # For each item, create a directory to store the results in
                    dn = os.path.join("puzzles", xword, year, month)
                    if not os.path.isdir(dn):
                        os.makedirs(dn)
                    fn = os.path.join(dn, f"{year}-{month}-{puz}.json")

                    # And pull down the data and write it out
                    with open(fn, "wt", newline="", encoding="utf-8") as f:
                        # Using cache here so only the first load for each num hits the internet
                        data = get_data(*info, mode='gzip', header=header, cache=True)
                        json.dump(data, f, indent=4)
                    print(f"Wrote {fn}")

def main():
    # Dirt simple TUI
    for cur in _commands:
        if len(sys.argv) == cur['args'] + 2 and sys.argv[1] == cur['cmd']:
            cur['func'](*sys.argv[2:])
            exit(0)
    print("Usage:")
    for cur in sorted(_commands, key=lambda x: x['cmd']):
        print(f"  {cur['cmd']} {cur['desc']}")

if __name__ == "__main__":
    main()
