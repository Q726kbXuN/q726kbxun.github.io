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

def output_puzzle(xword, year, month, puz, data, f):
    # The data format is simply:
    # data[0]: width
    # data[1]: height
    # data[2]: The cells, an array of arrays, each value is a string, or 0 for blocks
    # data[3]: An array of clues, see below
    # data[4]: (optional) True if this data is problematic (won't survive a trip to a .puz encoder)
    # data[5]: (optional) The puzzle type, either "", "acrostic", or "diagramless"

    # The clue format
    # clue[0]: String of the clue
    # clue[1]: Category, 0 is Across, and 1 is Down
    # clue[2]: The number of this clue
    # clue[3 ... x]: A list of the x, y locations of each cell for this clue

    # Simple text view of a scrossword
    block_left = "\u2590"
    block_mid = "\u2588"
    block_right = "\u258c"

    # Dump out the name first
    f.write(f"{xword}\n{year}-{month}-{puz}\n\n")

    # Run through each row of the crossword
    for y in range(data[1]):
        # Build up this row of the crossword
        row = " "
        for x in range(data[0]):
            if data[2][y][x] == 0:
                row += "# "
            else:
                row += data[2][y][x][0] + " "
        # Replace the "#" blocks with ASCII art
        for x in range(len(row), 0, -1):
            row = row.replace(" " + "# " * x, block_left + block_mid.join([block_mid] * x) + block_right)

        # Dump out the row
        f.write(f"{row}\n")

    # And now dump out the clues
    for dir_num, dir_desc in ((0, "Across"), (1, "Down")):
        need_header = True
        for cur in data[3]:
            if cur[1] == dir_num:
                if need_header:
                    # First clue in this section, so a header
                    f.write(f"\n{dir_desc}:\n")
                    need_header = False
                # And just show the clue
                f.write(f"{cur[2]}: {cur[0]}\n")

    f.write("\n")

@cmd("dump_all", 0, "= Download and dump out all puzzles")
def dump_all_puzzles():
    # Note that this will write out around 1.7gb of data

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
                    fn_json = os.path.join(dn, f"{year}-{month}-{puz}.json")
                    fn_txt = os.path.join(dn, f"{year}-{month}-{puz}.txt")

                    # And pull down the data and write it out
                    # Using cache here so only the first load for each num hits the internet
                    data = get_data(*info, mode='gzip', header=header, cache=True)

                    # Write out a JSON dumnp
                    with open(fn_json, "wt", newline="", encoding="utf-8") as f:
                        json.dump(data, f, indent=4)
                    
                    with open(fn_txt, "wt", newline="", encoding="utf-8") as f:
                        output_puzzle(xword, year, month, puz, data, f)

                    # Write out a simple text version
                    print(f"Wrote {fn_json} & .txt")

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
