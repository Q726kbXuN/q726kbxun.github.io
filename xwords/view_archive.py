#!/usr/bin/env python

# This is a small example to show how to download the data that powers this archive:
# https://q726kbxun.github.io/xwords/xwords.html

from urllib.request import urlopen, Request
import gzip, html, json, os, re, struct, sys

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
    # data[6]: (optional) Metadata dict, see below

    # The clue format
    # clue[0]: String of the clue
    # clue[1]: Category, 0 is Across, and 1 is Down
    # clue[2]: The number of this clue
    # clue[3 ... x]: A list of the x, y locations of each cell for this clue

    # The metadata format, every key is optional:
    # "title": The title of the puzzle
    # "author": The author of the puzzle
    # "note": A note or hint shown to solvers
    # "flags": An array of [x, y, flags] cell decorations, each flags value is a
    #          comma separated list of "circle", "shaded", "top-bar", and/or "left-bar"
    meta = data[6] if len(data) > 6 else {}

    # Simple text view of a crossword
    block_left = "\u2590"
    block_mid = "\u2588"
    block_right = "\u258c"

    # Dump out the name first, along with any metadata
    f.write(f"{xword}\n{year}-{month}-{puz}\n")
    for key in ("title", "author", "note"):
        if key in meta:
            f.write(f"{key.title()}: {meta[key]}\n")
    f.write("\n")

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

    # Note any decorated cells, grouped by the decoration
    if "flags" in meta:
        groups = {}
        for x, y, flags in meta["flags"]:
            groups.setdefault(flags, []).append(f"({x},{y})")
        f.write("\n")
        for flags, cells in groups.items():
            f.write(f"{flags}: {' '.join(cells)}\n")

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

# Common characters outside latin-1, along with the windows-1252 control range,
# replaced with plain text equivalents
_clean_chars = {
    "\u2018": "'", "\u2019": "'", "\u201a": "'", "\u201c": '"', "\u201d": '"',
    "\u2013": "-", "\u2014": "-", "\u2022": "*", "\u2026": "...", "\u00a0": " ",
    "\x85": "...", "\x91": "'", "\x92": "'", "\x93": '"', "\x94": '"',
    "\x96": "-", "\x97": "-",
}
def to_latin1(val):
    # .puz files store latin-1 text, so decode any HTML entities and replace
    # anything that doesn't fit
    val = html.unescape(val)
    for src, dest in _clean_chars.items():
        val = val.replace(src, dest)
    return val.encode("latin-1", errors="replace")

def puz_checksum(data, value=0):
    # The rolling checksum used throughout the .puz format
    for byte in data:
        if value & 1:
            value = (value >> 1) + 0x8000
        else:
            value >>= 1
        value = (value + byte) & 0xffff
    return value

def puz_section(name, data):
    # An extra section, like the circled cells or rebus values
    return name + struct.pack("<HH", len(data), puz_checksum(data)) + data + b"\0"

def output_puz(name, data):
    # Turn one puzzle into a .puz file, the same way the archive webpage does,
    # raising ValueError for puzzles the .puz format can't represent
    width, height, cells, clues = data[0], data[1], data[2], data[3]
    meta = data[6] if len(data) > 6 else {}

    circled = set()
    for x, y, flags in meta.get("flags", []):
        if "circle" in flags.split(","):
            circled.add((x, y))

    # Build the solution and state grids, tracking rebus and circled cells
    solution, state, markup, rebus_grid = b"", b"", b"", b""
    rebus_ids = {}
    for y in range(height):
        for x in range(width):
            cell = cells[y][x]
            if cell == 0:
                solution += b"."
                state += b"."
                markup += b"\0"
                rebus_grid += b"\0"
            else:
                solution += to_latin1(cell[0].upper())
                state += b"-"
                markup += b"\x80" if (x, y) in circled else b"\0"
                if len(cell) > 1:
                    if cell not in rebus_ids:
                        rebus_ids[cell] = len(rebus_ids) + 1
                    rebus_grid += bytes([rebus_ids[cell] + 1])
                else:
                    rebus_grid += b"\0"

    if not re.match(b"^[.:A-Za-z0-9@#$%&+?]+$", solution):
        raise ValueError("solution contains characters .puz can't store")

    # Order the clues the way .puz expects: walking the cells, for each cell
    # any Across clue that starts there, then any Down clue
    starts = {}
    for clue in clues:
        starts.setdefault((clue[3], clue[4], clue[1]), []).append(clue)
    ordered = []
    for y in range(height):
        for x in range(width):
            for direction in (0, 1):
                for clue in starts.get((x, y, direction), []):
                    ordered.append(to_latin1(clue[0]))

    # The format derives the clue count from the grid, so bail out on puzzles
    # where that doesn't line up
    expected = 0
    for y in range(height):
        for x in range(width):
            if cells[y][x] != 0:
                if (x == 0 or cells[y][x - 1] == 0) and x < width - 1 and cells[y][x + 1] != 0:
                    expected += 1
                if (y == 0 or cells[y - 1][x] == 0) and y < height - 1 and cells[y + 1][x] != 0:
                    expected += 1
    if expected != len(ordered):
        raise ValueError(f"grid expects {expected} clues, found {len(ordered)}")

    title = to_latin1(meta.get("title") or name)
    author = to_latin1(meta["author"]) if meta.get("author") else None
    notepad = to_latin1(meta["note"]) if meta.get("note") else None

    header = bytearray(52)
    header[2:14] = b"ACROSS&DOWN\0"
    header[24:28] = b"1.2c"
    header[44] = width
    header[45] = height
    struct.pack_into("<HHH", header, 46, len(ordered), 1, 0)

    # The checksums: the grid info, then one rolled over the grids and text,
    # then the four masked "ICHEATED" pairs (for file version 1.2 the text
    # part doesn't include the notepad)
    text = title + b"\0" + (b"" if author is None else author + b"\0") + b"".join(ordered)
    sums = [puz_checksum(header[44:52]), puz_checksum(solution), puz_checksum(state), puz_checksum(text)]
    struct.pack_into("<H", header, 14, sums[0])
    value = puz_checksum(text, puz_checksum(state, puz_checksum(solution, sums[0])))
    struct.pack_into("<H", header, 0, value)
    for i, mask in enumerate(b"ICHEATED"):
        header[16 + i] = mask ^ ((sums[i] & 0xff) if i < 4 else (sums[i - 4] >> 8))

    # The strings section: title, author, copyright, the clues, then notepad
    strings = title + b"\0" + (author or b"") + b"\0" + b"\0"
    strings += b"\0".join(ordered) + b"\0"
    strings += (notepad or b"") + b"\0"

    ret = bytes(header) + solution + state + strings
    if len(rebus_ids) > 0:
        ret += puz_section(b"GRBS", rebus_grid)
        temp = b""
        for rebus_value, rebus_id in sorted(rebus_ids.items(), key=lambda x: x[1]):
            temp += ("%2d:" % rebus_id).encode("latin-1") + rebus_value.encode("latin-1", errors="replace") + b";"
        ret += puz_section(b"RTBL", temp)
    if b"\x80" in markup:
        ret += puz_section(b"GEXT", markup)
    return ret

@cmd("dump_all", 0, "= Download and dump out all puzzles")
def dump_all_puzzles():
    # Note that this will write out around 2gb of data

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
                    fn_puz = os.path.join(dn, f"{year}-{month}-{puz}.puz")

                    # And pull down the data and write it out
                    # Using cache here so only the first load for each num hits the internet
                    puz_data = get_data(*info, mode='gzip', header=header, cache=True)

                    # Write out a JSON dump
                    with open(fn_json, "wt", newline="", encoding="utf-8") as f:
                        json.dump(puz_data, f, indent=4)

                    # Write out a simple text version
                    with open(fn_txt, "wt", newline="", encoding="utf-8") as f:
                        output_puzzle(xword, year, month, puz, puz_data, f)

                    # And a .puz version, skipping the puzzles that can't make the
                    # trip, just like the webpage does
                    wrote_puz = ""
                    if not (len(puz_data) > 4 and puz_data[4]):
                        try:
                            temp = output_puz(f"{xword} - {year}-{month}-{puz}", puz_data)
                            with open(fn_puz, "wb") as f:
                                f.write(temp)
                            wrote_puz = " & .puz"
                        except ValueError as e:
                            print(f"Skipping {year}-{month}-{puz}.puz: {e}")

                    print(f"Wrote {fn_json} & .txt{wrote_puz}")

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
