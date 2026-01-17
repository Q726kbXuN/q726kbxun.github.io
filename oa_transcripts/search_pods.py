#!/usr/bin/env python3

# This example shows how to pull down the data to search 
# all transcripts for this podcast.
# 
# Please see the following link to search online:
# https://q726kbxun.github.io/openargs/openargs.html

# A markdown overview of the data file and the process behind this:

r'''
# Search Engine Data Format

This document outlines the file specifications, naming conventions, and binary layout expected by the static, 
client-side search engine.

The system relies on **HTTP Range Requests** to fetch specific compressed chunks of data from larger container files. 
This architecture allows the search engine to host large datasets statically while minimizing bandwidth usage.

Note: Due to issues with GitHub Page's HTTP server, this system can also fall back to not using byte range requests. 
For the first bootstrap data file, it's requested in whole, and parsed in two passes. The rest of the files are 
constructed to only have one batch when the target is Github Pages, so they only ever need to be requested in whole on 
that server.

## 1. Naming Conventions

All data files must reside in the same directory as the HTML search interface.

| Filename Pattern | Description | Format |
| --- | --- | --- |
| `search_data_00.dat` | **Root Index.** The entry point for the search engine. | Plain Text JSON, and Binary |
| `search_data_XX.dat` | **Container Files.** `XX` is a zero-padded, two-digit number (e.g., `01`, `15`). | 
      Binary (Concatenated GZIP) |
| `search_data_lemma.dat` | **Lemmatization.** (Optional) Dictionary for fuzzy matching. | GZIP Compressed JSON |

## 2. Binary Layout & Container Files

The files matching the pattern `search_data_XX.dat` (where XX > 0) are **Container Files**.

These are **not** valid monolithic archives (like ZIP or TAR). Instead, they consist of multiple GZIP streams 
concatenated back-to-back.

**Layout Diagram:**

```text
[ GZIP STREAM A ] [ GZIP STREAM B ] [ GZIP STREAM C ] ...
^ Offset 0        ^ Offset N        ^ Offset N+M
```

* **Access Method:** The client issues `fetch` requests with the header `Range: bytes=START-END`.
* **Decoding:** The resulting blob is passed through a `DecompressionStream("gzip")` to extract the underlying JSON.

## 3. Data Structures

### A. The Root Index (`search_data_00.dat`)

This file starts with an uncompressed plain text JSON, always of 100 bytes.  It bootstraps the application by pointing 
to the location of the **Batch List**.
It is padded with space characters to be 100 bytes.  The batch list is in this file as well, though the batch list is 
stored compressed.

```json
{
  "created": "2023-10-27", 
  "items": 1234,              // Total searchable items (episodes)
  "before": 15,               // Context words to display before a hit
  "after": 100,               // Context words to display after a hit
  "data": [0, 100, 123]       // Pointer to the Batch List
}
```

* **`data`**: An array formatted as `[file_number, byte_offset, byte_length]`.

### B. The Batch List

This data block is stored as a compressed chunk within a container file (pointed to by the Root Index). When 
decompressed, the batch list yields a JSON array of pointers. Each pointer defines a "Batch" of transcripts.

```json
[
  [1, 2548, 50000],  // Points to Batch 1, inside search_data_01.dat
  [1, 52548, 48000], // Points to Batch 2, also inside search_data_01.dat
  [2, 0, 30500]      // Points to Batch 3, inside search_data_02.dat
]
```

The target size for each batch is 10mb before compression. This is a target, and not a guarantee on the batch size.

### C. The Transcript Item

A "Batch" decompresses into a JSON Array of **Item Objects**. Each Item Object represents a single episode or document.

```json
{
  "title": "Episode 101: The Example",
  "link": "https://example.com/ep101",
  "published": "2023-01-01",
  "group": "Season 1",                      // Optional
  "remote": "url to remote transcript",     // Optional
  "words": "Welcome to the podcast...", 
  "start": [0, 50, 45, 12, ...], 
  "speaker": "AAABBB...", 
  "speakers": {                             // Optional
    "A": "Host Name",
    "B": "Guest Name"
  },
  "segments": {                             // Optional
    "title": ["Intro", "Interview"],
    "offset": [0, 15000] 
  }
}
```

#### Field Logic

| Field | Type | Description | Note |
| --- | --- | --- | --- |
| `words` | String | The full transcript content. Tokenization is performed by splitting on spaces. | |
| `start` | Array`<Int>` | **Delta-encoded timestamps** in seconds (all values are integers). | (1) |
| `speaker` | String | A mapping string. Must have the same length as the number of tokens in `words`. | (2) |
| `speakers` | Dict | Dictionary mapping single characters to full speaker names. | |
| `segments` | Dict | Dictionary with two keys, `title` and `offset` naming each segment. Offsets are in seconds. | |

*Note 1*: To calculate the absolute time for word `i`: `Time[i] = Time[i-1] + start[i]`.

*Note 2*: `@` reserves "No Speaker". Other characters map to keys in `speakers`.

### D. Lemmatization (`search_data_lemma.dat`)

This file decompresses to a JSON array containing two objects. It allows the search engine to map query terms (e.g., 
"ran") to base forms or variations (e.g., "run").

```json
[
  { 
    "ran": "run",
    "mice": "mouse"
  },
  { 
    "run": ["running", "runs", "runner"]
  }
]
```

* **Index 0 (Complex):** Many-to-one or irregular mappings.
* **Index 1 (Simple):** One-to-many suffixes.

## 4. Execution Process

1. **Bootstrap:** Agent fetches first 100 bytes of `search_data_00.dat` (JSON).
2. **Batch List:** Agent fetches the batch list from `search_data_00.dat` as referenced from the bootstrap and decodes 
    it (GZip JSON)
3. **Pre Fetch (Optional):** Agent begins pre-fetching each batch item from the batch list from `search_data_XX.dat` 
    (where XX >= 1) and decodes them (GZip JSON)
4. **Search:**
    * Agent iterates through the Batch List.
    * For each batch, it requests the specific byte range from the corresponding `search_data_XX.dat` file if not 
        pre-cached.
    * It decompresses the chunk to get a list of Item Objects.
    * It performs a string scan on the data in the object.

'''

from urllib.request import Request, urlopen
import gzip, json, os, sys, textwrap

# Simple wrapper for command line parsing
_commands = []
def cmd(cmd, args, desc):
    def helper(func):
        _commands.append({"cmd": cmd, "args": args, "desc": desc, "func": func})
        def wrapper(*args2, **kwargs):
            return func(*args2, **kwargs)
        return wrapper
    return helper

def download_data(num, start, len, is_gzip=True, get_size=False, decode_json=True, ignore_cache=False):
    # Helper to download data, will use cached version if possible
    url = f"https://q726kbxun.github.io/openargs/search_data_{num:02d}.dat"
    fn = f"search_data_{num:02d}.dat"

    if os.path.isfile(fn) and not ignore_cache:
        # Use the cached version
        if get_size:
            return os.path.getsize(fn)
        else:
            with open(fn, "rb") as f:
                if start is None:
                    data = f.read()
                else:
                    f.seek(start, os.SEEK_SET)
                    data = f.read(len)
    else:
        if get_size:
            # Just a request to get the size, so go get the size
            req = Request(url, method="HEAD")
            resp = urlopen(req)
            return int(resp.headers['Content-Length'])
        else:
            if start is None:
                # Request for the full data
                req = Request(url)
            else:
                # Partial request, use byte range request to get the partial data
                req = Request(url, headers={"Range": f"bytes={start}-{start+len-1}"})
            data = urlopen(req).read()

    if is_gzip:
        # Data is compressed, decompres it
        data = gzip.decompress(data)
    if decode_json:
        # Data is JSON encoded, decode it
        data = json.loads(data)

    return data

@cmd("dl", 0, "= Download all data to speed up searches.")
def download_all():
    # Helper to download all data

    # Get the meta data
    info = download_data(0, 0, 100, is_gzip=False, ignore_cache=True)
    batches = download_data(*info['data'], ignore_cache=True)

    # Run through and get the total size so the user has some idea how much work is ahead of us
    total_size = 0
    total_files = 0
    batches.insert(0, [0,0,0])
    for cur in batches:
        total_size += download_data(*cur, get_size=True, ignore_cache=True)
        total_files += 1
    
    # Ask the user if they're ok with downloading all the data
    yn = input(f"This will download {total_files:,} files for about {total_size/1048576:.2f}mb, are you sure? [y/(n)] ")
    if yn == "y":
        for cur in batches:
            fn = f"search_data_{cur[0]:02d}.dat"
            print(f"Downloading {fn}...")
            data = download_data(cur[0], None, None, is_gzip=False, decode_json=False, ignore_cache=True)
            with open(fn, "wb") as f:
                f.write(data)
        print("Done")

def enumerate_items():
    # Helper to download (or use cached data) all items and yield
    # each one in turn
    info = download_data(0, 0, 100, is_gzip=False)
    batches = download_data(*info['data'])
    for cur in batches:
        info = download_data(*cur)
        for item in info:
            yield item

def enumerate_hits(words, search_term):
    # Helper to show each hit in a string.  This is just an example of how
    # the search can work, more complex searches are possible
    words = words.lower()
    search_term = search_term.lower()
    start_at = -1
    while True:
        try:
            hit = words.index(search_term, start_at+1)
        except ValueError:
            break
        yield hit
        start_at = hit

def create_helpers(cur):
    # Some lookup tables are useful when displaying data, so
    # create the lookup tables in each item based on existing
    # data

    if cur.get('fixed', False):
        # They've already been created, nothing to do
        return

    # First, make 'start' start by time
    temp = []
    last = 0
    for x in cur['start']:
        last += x
        temp.append(last)
    cur['start'] = temp

    # Make a lookup to map offset to word number
    temp = []
    word_num = 0
    for x in cur['words']:
        temp.append(word_num)
        if x == ' ':
            word_num += 1
    cur['index_to_word'] = temp

    # And an offset table
    temp = []
    offset = 0
    for word in cur['words'].split(' '):
        temp.append((offset, len(word)))
        offset += len(word) + 1
    cur['offset'] = temp

    # And mark this item as fixed up:
    cur['fixed'] = True

def show_transcript(cur, start_at, end_at, segments=None, summary=None, speakers={}, output=print):
    # Show a transcript, from one word to the end word
    phrase = None
    ended_sentence = False
    last_speaker = ''
    phrase_at = -1

    # If there is a summary, go ahead and show it
    if summary is not None:
        summary = "Summary: " + summary
        for row in textwrap.wrap(summary, subsequent_indent=" " * 10):
            output(row)
        output("")

    # If there are segments, pull them out into an array to 
    # track when to show the next one
    segments_to_show = []
    if segments is not None:
        for offset, title in zip(segments["offset"], segments["title"]):
            segments_to_show.append((offset, title))

    for word in range(start_at, end_at):
        offset, word_len = cur['offset'][word]
        cur_speaker = cur['speaker'][word]

        # Do we need to start a new phrase?        
        new_phrase = False
        if phrase is None:
            # First time through, start a new phrase
            new_phrase = True

        if ended_sentence:
            # Otherwise, only start one after a sentence ends
            if last_speaker != cur_speaker:
                # If the speaker changed
                new_phrase = True
            if (cur['start'][word] - phrase_at) >= 45:
                # Or, it's been more than 45 seconds
                new_phrase = True
        else:
            if cur['start'][word] - phrase_at >= 120:
                # Or, after two minutes, just start a phrase even in the middle of
                # a sentence
                new_phrase = True
        
        if new_phrase:
            if phrase is not None:
                # Show the last phrase
                for row in textwrap.wrap(phrase, subsequent_indent=" " * 10):
                    output(row)
                output("")

            # Store where this phrase starts
            phrase_at = cur['start'][word]

            # See if it's time to show the next segment title
            segment_title = None
            while len(segments_to_show) > 0 and segments_to_show[0][0] <= phrase_at:
                _, segment_title = segments_to_show.pop(0)
            if segment_title is not None:
                for row in textwrap.wrap("Segment: " + segment_title, subsequent_indent=" " * 10):
                    output(row)
                output("")

            # Show the timestamp
            phrase = f"{phrase_at//3600:2d}:{(phrase_at%3600)//60:02d}:{phrase_at%60:02d}:"

            # The speaker changed, show that
            if last_speaker != cur_speaker:
                if cur_speaker != "@":
                    # If this speaker is named, go ahead and use it
                    phrase += f" {speakers.get(cur_speaker, cur_speaker)}:"
                last_speaker = cur_speaker

        # Add the word to this phrase        
        phrase += " " + cur['words'][offset:offset+word_len]

        # And note if this phrase now marks the end of a sentence
        ended_sentence = phrase[-1] in ".?!"

    if phrase is not None:
        # Finally, show the final phrase
        for row in textwrap.wrap(phrase, subsequent_indent=" " * 10):
            output(row)

@cmd("search", 1, "<search> = Search all transcripts.")
def search_transcripts(*search):
    # Treat multiple words as one long string
    search = " ".join(search)

    for cur in enumerate_items():
        # This will return items like:
        # {
        #     'published': 'yyyy-mm-dd',    # The date this episode was published
        #     'title': 'Episode Title',     # The episode title
        #     'link': '<url>',              # The URL to the story
        #     'words': "The raw text",      # The complete transcript, splitting this by a space yields the list of words
        #     'start': [14, 1, 0, 0, 1, 0], # Where each word starts by time
        #                                   # This is cumulative number of seconds
        #     'speaker': 'AAABBBCCDDD@@@',  # One char per word for each speaker.
        #                                   # "A" == Speaker 1, etc.  "@" == No known speaker
        #     'segments': {                 # Optional section for the detected segments
        #         'title': [],              # Titles for each segment
        #         'offset': [] }            # Start offset for each segment in seconds
        #     'summary': '...'              # Optional machine generated summary of episode
        #     'speakers': {'A': 'Name'}     # Optional mapping of speaker IDs to names
        # }

        for i, hit in enumerate(enumerate_hits(cur['words'], search)):
            if i == 0:
                # Ok, we have a match, make some helpers to speed lookups
                create_helpers(cur)

                # Show a header:
                print("")
                print("Title: " + cur['title'])

            # Show each match
            print("")
            word_num = cur['index_to_word'][hit]
            show_transcript(cur, max(0, word_num - 10), min(len(cur['offset']), word_num + 10), speakers=cur.get('speakers', {}))

@cmd("list", 0, "= List all episodes")
def list_episodes():
    # Just dump out a list of all episodes
    for i, cur in enumerate(enumerate_items()):
        print(f"{i+1}: {cur['title']}")

@cmd("dump", 1, "<num> = Dump the complete transcript for an episode")
def dump_episode(num):
    num = int(num) - 1
    # Just enumerate through the episodes till we hit the target number
    for i, cur in enumerate(enumerate_items()):
        if i == num:
            create_helpers(cur)
            print(f"Title: {cur['title']}")
            print(f"Published: {cur['published']}")
            print(f"Link: {cur['link']}")
            print("")
            show_transcript(cur, 0, len(cur['start']), cur.get("segments"), cur.get("summary"), speakers=cur.get('speakers', {}))
            exit(0)

@cmd("dump_all", 1, "<dir_name> = Dump all episodes to text and JSON files")
def dump_all_episodes(dir_name):
    # Just dump all the episodes
    if not os.path.isdir(dir_name):
        print(f"ERROR: {dir_name} does not exist")
        exit(1)

    for i, cur in enumerate(enumerate_items()):
        # Normalize the data
        create_helpers(cur)

        print(f"Working on {i+1}:{cur['title']}...")

        # Write out a text version of the transcript:
        with open(os.path.join(dir_name, f"ep_{i + 1:04d}.txt"), "wt", newline="", encoding="utf-8") as f:
            def write_line(val):
                f.write(val + "\n")
            write_line(f"Title: {cur['title']}")
            write_line(f"Published: {cur['published']}")
            write_line(f"Link: {cur['link']}")
            write_line("")

            show_transcript(cur, 0, len(cur['speaker']), cur.get("segments"), cur.get("summary"), cur.get('speakers', {}), write_line)

        # Clean up the data to prepare it to be dumped:
        del cur['index_to_word']
        del cur['fixed']
        del cur['offset']

        cur['speaker'] = [(ord(x) - ord('A')) for x in cur['speaker']]
        cur['words'] = cur['words'].split(' ')

        # Just use a modified JSON dump here to prevent way too much file space being wasted
        # indenting all of the items in the lists, and keep each key/value pair on its own line
        with open(os.path.join(dir_name, f"ep_{i + 1:04d}.json"), "wt", newline="", encoding="utf-8") as f:
            f.write("{\n")
            temp = [{"key": x, "value": cur[x], "extra": ","} for x in sorted(cur)]
            temp[-1]["extra"] = ""
            for item in temp:
                f.write(" " * 4 + json.dumps(item['key']) + ": " + json.dumps(item['value'], separators=(',', ':')) + item['extra'] + "\n")
            f.write("}\n")

    print("All done.")

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
