#!/usr/bin/env python3
AGENT_HELP = """
-- Agent Help -----------------------------------------------------------------
Answering "find the episode where ..." questions, in order:
1. `rank "topic keywords" --limit 10 --summaries` finds episodes by topic
   (add `--transcripts` to rank by what was actually said, not just the
   episode blurbs).
2. `search "concrete phrase" --counts` finds exact language. Memories are
   unreliable and transcripts contain errors, so when a phrase misses, retry
   with `--similar` (other word forms: fly/flies/flew), `--fuzzy` (close
   spellings that occur in the archive), or `--any` (whole-word OR).
3. `near "fly,insect,bug" "robot,machine" --window 40 --similar` finds
   passages where some word from EVERY group lands close together. Use it for
   paraphrased claims and analogies ("the guest said X is like Y"): the
   user's exact nouns are often wrong, so list synonyms in each group.
4. `vocab robo --fuzzy` shows which related words and misspellings really
   occur in the transcripts, to steer the next query.
5. Verify before answering: `context "phrase" --episode "Title"`, or
   `transcript "Title" --start 0:17:30 --end 0:21:00 --timestamps`.
6. Report: title, date, link, a confirming quote, its timestamp, and the
   `search.html#...` anchor (see below).

Cutting down on permission prompts and process startups:
- Fold several probes into ONE invocation with `batch` (one approval, one
  archive load, shared caches; add --json for a single JSON array):
      python3 search_helper.py batch \\
          'rank "octopus camouflage" --limit 5' \\
          'search "like a robot" --similar --counts' \\
          'near "fly,insect" "robot,computer" --window 40 --similar'
- This script only reads the archive and maintains one cache file beside it,
  so it is safe to pre-approve. Claude Code `.claude/settings.json`:
      {"permissions": {"allow": ["Bash(python3 search_helper.py:*)"]}}

Fast paths:
- `--json` on most commands for machine-readable output; notes go to stderr.
- `--since/--until YYYY-MM-DD` narrow every scan.
- `search` matches substrings; `--regex` unlocks alternation and wildcards.
- Keyword hits lie. Confirm with `context` before quoting a result.
- First use builds `search_helper_cache.sqlite3` next to the data files: an
  ephemeral cache, rebuilt automatically when the archive changes, safe to
  delete, skipped with `--no-cache`. The .dat files stay the source of truth.

-- Linking back to the Web UI -------------------------------------------------
`search.html#<file>,<start>,<len>,<itemID>` opens one transcript; appending
`,<wordIndex>` scrolls to (and highlights) a specific word. This script prints
those anchors so a finding can be handed back to a human in the browser.

-------------------------------------------------------------------------------

""".strip()


"""
-- The on-disk search format --------------------------------------------------
Files are named `search_data_NN.dat` (NN = zero-padded integer) and live in
one archive directory. All reads are (file_number, byte_start, byte_length)
slices, so the browser can fetch tiny ranges over HTTP; this script just
seek()s.

1. HEADER. The first 100 bytes of `search_data_00.dat` are a JSON object,
   space-padded out to 100 bytes, e.g.:
       {"data":[0,100,79],"created":"2026-06-01 09:24:28","items":605,
        "before":15,"after":100}
   - `data`   : a (file, start, len) slice locating the BATCH INDEX (gzipped).
   - `created`: when the archive was built.
   - `items`  : total number of episodes across all batches.
   - `before`/`after`: default context-word counts used by the web UI.

2. BATCH INDEX. Decompressing the `data` slice yields a JSON list of
   (file, start, len) slices. Each one points at a BATCH.

3. BATCH. Decompressing a batch slice yields a JSON list of EPISODE objects.

4. EPISODE object fields:
   - `published` : "YYYY-MM-DD" (or "1970-01-01" if unknown).
   - `title`     : episode title.
   - `link`      : canonical URL to the episode.
   - `summary`   : short prose summary (great for cheap relevance ranking).
   - `words`     : the full transcript as a single space-joined string. Token
                   i (words.split(' ')[i]) is the i-th spoken word.
   - `start`     : list, one int per word: the PER-WORD DELTA in seconds since
                   the previous word. A running cumulative sum gives each
                   word's absolute timestamp. len(start) == number of words.
   - `speaker`   : string, one CHAR per word; equal chars == same speaker.
                   Optional `speakers` dict maps that char to a display name.
   - `segments`  : optional {offset:[secs...], title:[...]} chapter markers.
   - `group`     : optional tag (some archives partition episodes
                   into groups).

   `words`, `start`, and `speaker` are index-aligned: the same index i refers
   to the same spoken word in all three.

5. LEMMA TABLE (optional). `search_data_lemma.dat` is gzipped JSON
   [word->lemma, lemma->[other forms]] shared with the web UI's "similar
   words" feature; this script uses it for --similar expansion when present.

-- Ancillary files ------------------------------------------------------------
On first use the script builds `search_helper_cache.sqlite3` beside the data
files: transcripts plus a word-frequency table, so repeated runs skip the
decompress-everything step. It is a disposable cache — rebuilt automatically
whenever the `search_data_NN.dat` files change, safe to delete, skipped with
`--no-cache`. The .dat archive files remain the only source of truth.

-- Usage ----------------------------------------------------------------------
python3 search_helper.py                  (full help, including agent tips)
python3 search_helper.py info
python3 search_helper.py list [--limit N] [--since YYYY-MM-DD]
python3 search_helper.py search "search term" [--all|--any] [--similar] [--fuzzy] [--regex]
python3 search_helper.py near "fly,insect" "robot,machine" [--window 50] [--similar]
python3 search_helper.py vocab robo [--fuzzy]
python3 search_helper.py context "search term" --episode "Blackstock"
python3 search_helper.py transcript "Episode" [--timestamps] [--start 0:10:00 --end 0:12:00]
python3 search_helper.py summaries "search term"
python3 search_helper.py rank "search term" [--transcripts]
python3 search_helper.py cache [--rebuild|--delete]
python3 search_helper.py batch 'search "mars" --counts' 'rank "ocean" --limit 5'

Add `--json` to most commands for machine-readable output. By default the
archive directory is the script's own folder; override with
`--archive /path/to/dir`.
"""

import argparse
import bisect
import collections
import contextlib
import difflib
import gzip
import io
import json
import math
import os
import re
import shlex
import sys
import time

try:
    import sqlite3
except ImportError:
    sqlite3 = None

CACHE_NAME = "search_helper_cache.sqlite3"
CACHE_SCHEMA = "1"
LEMMA_NAME = "search_data_lemma.dat"

Row = collections.namedtuple("Row", "anchor published title link summary words episode")

class Archive:
    HEADER_LEN = 100

    def __init__(self, directory):
        self.dir = directory
        self.cache_enabled = True
        self._batch_cache = {}
        self._cache = None
        self._cache_tried = False
        self._vocab = None
        self._stems = None
        self._lemma = False
        self.header = self._load_header()
        self.batch_slices = self._read_gzip_slice(*self.header["data"])

    def _path(self, file_num):
        return os.path.join(self.dir, f"search_data_{file_num:02d}.dat")

    def _read_bytes(self, file_num, start, length):
        with open(self._path(file_num), "rb") as fh:
            fh.seek(start)
            return fh.read(length)

    def _read_gzip_slice(self, file_num, start, length):
        return json.loads(gzip.decompress(self._read_bytes(file_num, start, length)))

    def _load_header(self):
        # The 100-byte header JSON is space-padded; json.loads ignores the tail.
        return json.loads(self._read_bytes(0, 0, self.HEADER_LEN).decode("utf-8"))

    def batch(self, batch_slice):
        key = tuple(batch_slice)
        if key not in self._batch_cache:
            self._batch_cache[key] = self._read_gzip_slice(*batch_slice)
        return self._batch_cache[key]

    def episodes(self):
        """Yield (anchor, episode) for every episode.

        The anchor is the (file, start, len, itemID) tuple used to build a deep
        link back into search.html.
        """
        for batch_slice in self.batch_slices:
            for item_id, episode in enumerate(self.batch(batch_slice)):
                anchor = (batch_slice[0], batch_slice[1], batch_slice[2], item_id)
                yield anchor, episode

    def episode_at(self, anchor):
        return self.batch(list(anchor[:3]))[anchor[3]]

    def cache(self):
        if not self.cache_enabled:
            return None
        if not self._cache_tried:
            self._cache_tried = True
            self._cache = open_cache(self)
        return self._cache

    def rows(self, since=None, until=None):
        """Yield a Row per episode, from the sqlite cache when available.

        Row.episode is the full episode dict on the slow path and None on the
        cached path; use episode_at(row.anchor) when word timings or speakers
        are needed.
        """
        cache = self.cache()
        if cache:
            yield from cache.rows(since, until)
            return
        for anchor, episode in self.episodes():
            published = episode.get("published", "")
            if not in_date_range(published, since, until):
                continue
            yield Row(anchor, published, episode.get("title", ""),
                      episode.get("link", ""), episode.get("summary", "") or "",
                      episode.get("words", ""), episode)

    def vocab(self):
        """Map every transcript word to (episode count, total occurrences)."""
        if self._vocab is None:
            cache = self.cache()
            if cache:
                self._vocab = cache.vocab()
            else:
                doc_freq, totals = collections.Counter(), collections.Counter()
                for row in self.rows():
                    counts = collections.Counter(tokenize(row.words))
                    totals.update(counts)
                    doc_freq.update(counts.keys())
                self._vocab = {t: (doc_freq[t], totals[t]) for t in totals}
        return self._vocab

    def lemma_maps(self):
        if self._lemma is False:
            self._lemma = None
            path = os.path.join(self.dir, LEMMA_NAME)
            if os.path.exists(path):
                try:
                    with open(path, "rb") as fh:
                        maps = json.loads(gzip.decompress(fh.read()))
                    if isinstance(maps, list) and len(maps) == 2:
                        self._lemma = maps
                except (OSError, ValueError):
                    note(f"could not read {LEMMA_NAME}; using built-in stemming instead.")
        return self._lemma

    def stem_groups(self):
        if self._stems is None:
            self._stems = {}
            for term in self.vocab():
                self._stems.setdefault(light_stem(term), []).append(term)
        return self._stems

def note(message):
    print(f"note: {message}", file=sys.stderr)

def archive_fingerprint(directory):
    parts = []
    for name in sorted(os.listdir(directory)):
        if re.fullmatch(r"search_data_\d+\.dat", name):
            stat = os.stat(os.path.join(directory, name))
            parts.append(f"{name}:{stat.st_size}:{int(stat.st_mtime)}")
    return ";".join(parts)

class Cache:
    def __init__(self, conn, path):
        self.conn = conn
        self.path = path
        self.meta = dict(conn.execute("SELECT key, value FROM meta"))

    def rows(self, since=None, until=None):
        sql = ("SELECT file, start, length, item, published, title, link, "
               "summary, words FROM episodes")
        conds, params = [], []
        if since:
            conds.append("published >= ?")
            params.append(since)
        if until:
            conds.append("published <= ?")
            params.append(until)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        for (file_num, start, length, item, published, title, link, summary,
             words) in self.conn.execute(sql + " ORDER BY id", params):
            yield Row((file_num, start, length, item), published, title, link,
                      summary, words, None)

    def vocab(self):
        return {term: (episodes, total) for term, episodes, total
                in self.conn.execute("SELECT term, episodes, total FROM vocab")}

def open_cache(archive):
    if sqlite3 is None:
        note("python's sqlite3 module is missing, so the speed-up cache is off "
             "and every run rescans the archive. Install a standard Python "
             "build (e.g. `apt install python3` / `brew install python3`) to "
             "enable it.")
        return None
    path = os.path.join(archive.dir, CACHE_NAME)
    fingerprint = archive_fingerprint(archive.dir)
    if os.path.exists(path):
        try:
            conn = sqlite3.connect(path)
            meta = dict(conn.execute("SELECT key, value FROM meta"))
            if (meta.get("schema") == CACHE_SCHEMA
                    and meta.get("fingerprint") == fingerprint):
                return Cache(conn, path)
            conn.close()
            note("archive files changed; rebuilding the search cache ...")
        except sqlite3.Error:
            note("search cache unreadable; rebuilding ...")
    return build_cache(archive, path, fingerprint)

def build_cache(archive, path, fingerprint):
    note(f"building search cache for {archive.header.get('items', '?')} "
         "episode(s); one-time until the archive changes ...")
    started = time.monotonic()
    tmp = f"{path}.tmp{os.getpid()}"
    try:
        conn = sqlite3.connect(tmp)
        conn.executescript("""
            PRAGMA journal_mode = OFF;
            PRAGMA synchronous = OFF;
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE episodes (id INTEGER PRIMARY KEY, file INTEGER,
                start INTEGER, length INTEGER, item INTEGER, published TEXT,
                title TEXT, link TEXT, summary TEXT, words TEXT);
            CREATE TABLE vocab (term TEXT PRIMARY KEY, episodes INTEGER,
                total INTEGER);
        """)
        doc_freq, totals, episode_count = collections.Counter(), collections.Counter(), 0
        for anchor, episode in archive.episodes():
            words = episode.get("words", "")
            conn.execute(
                "INSERT INTO episodes VALUES (NULL,?,?,?,?,?,?,?,?,?)",
                (anchor[0], anchor[1], anchor[2], anchor[3],
                 episode.get("published", ""), episode.get("title", ""),
                 episode.get("link", ""), episode.get("summary", "") or "", words))
            counts = collections.Counter(tokenize(words))
            totals.update(counts)
            doc_freq.update(counts.keys())
            episode_count += 1
        conn.executemany("INSERT INTO vocab VALUES (?,?,?)",
                         ((term, doc_freq[term], totals[term]) for term in totals))
        conn.executemany("INSERT INTO meta VALUES (?,?)", [
            ("schema", CACHE_SCHEMA),
            ("fingerprint", fingerprint),
            ("built", time.strftime("%Y-%m-%d %H:%M:%S")),
            ("episodes", str(episode_count)),
        ])
        conn.commit()
        conn.close()
        os.replace(tmp, path)
    except (OSError, sqlite3.Error) as err:
        note(f"could not build the search cache ({err}); continuing with "
             "direct archive scans.")
        with contextlib.suppress(OSError):
            os.remove(tmp)
        return None
    note(f"cache built in {time.monotonic() - started:.1f}s -> {os.path.basename(path)}")
    return Cache(sqlite3.connect(path), path)

def anchor_str(anchor, word_index=None):
    frag = ",".join(str(x) for x in anchor)
    if word_index is not None:
        frag += f",{word_index}"
    return f"search.html#{frag}"

def word_timestamps(start_deltas):
    out, running = [], 0
    for delta in start_deltas:
        running += delta
        out.append(running)
    return out

def word_starts(text):
    if not text:
        return []
    starts = [0]
    pos = text.find(" ")
    while pos != -1:
        starts.append(pos + 1)
        pos = text.find(" ", pos + 1)
    return starts

def word_index_at_offset(starts, offset):
    if not starts:
        return 0
    return max(0, bisect.bisect_right(starts, offset) - 1)

def fmt_time(seconds):
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}"

def parse_clock(text):
    parts = text.split(":")
    if len(parts) > 3 or not all(part.isdigit() for part in parts):
        sys.exit(f"Bad time {text!r}; use H:MM:SS, MM:SS, or plain seconds.")
    seconds = 0
    for part in parts:
        seconds = seconds * 60 + int(part)
    return seconds

def speaker_labels(episode):
    """Map speaker chars to display labels.

    Use the episode's `speakers` dict when present. Otherwise assign stable
    Speaker A/B/... labels by first appearance.
    """
    names = episode.get("speakers", {}) or {}
    labels, next_letter = {}, 0
    for ch in episode.get("speaker", ""):
        if ch in labels:
            continue
        if ch in names:
            labels[ch] = names[ch]
        else:
            labels[ch] = "Speaker " + chr(ord("A") + next_letter)
            next_letter += 1
    return labels

Hit = collections.namedtuple("Hit", "anchor episode word_index snippet timestamp")
SearchResult = collections.namedtuple("SearchResult", "episode anchor hits hit_count")

def compile_query(query, regex, ignore_case):
    flags = re.IGNORECASE if ignore_case else 0
    pattern = query if regex else re.escape(query)
    try:
        return re.compile(pattern, flags)
    except re.error as err:
        sys.exit(f"Invalid regular expression {query!r}: {err}")

def count_hits(pattern, text):
    return sum(1 for _ in pattern.finditer(text))

def find_hits(episode, anchor, pattern, context_chars, max_per_episode):
    words = episode.get("words", "")
    starts, times = None, None
    hits = []

    for match in pattern.finditer(words):
        if starts is None:
            starts = word_starts(words)
        if times is None:
            times = word_timestamps(episode.get("start", []))

        word_index = word_index_at_offset(starts, match.start())
        timestamp = times[word_index] if word_index < len(times) else 0
        left = max(0, match.start() - context_chars)
        right = min(len(words), match.end() + context_chars)
        snippet = (
            ("..." if left > 0 else "")
            + words[left:match.start()]
            + ">>"
            + words[match.start():match.end()]
            + "<<"
            + words[match.end():right]
            + ("..." if right < len(words) else "")
        )
        hits.append(Hit(anchor, episode, word_index, snippet.replace("\n", " "), timestamp))
        if max_per_episode is not None and len(hits) >= max_per_episode:
            break
    return hits

def in_date_range(published, since, until):
    if since and published < since:
        return False
    if until and published > until:
        return False
    return True

def tokenize(text):
    return re.findall(r"[a-z0-9]+", text.lower())

def light_stem(word):
    """Crude suffix stripper, used to group word forms only when the archive
    has no lemma table; both sides of any comparison pass through it."""
    for suffix, replacement in (("ies", "y"), ("ied", "y"), ("ing", ""),
                                ("ed", ""), ("ness", ""), ("ment", ""),
                                ("ation", ""), ("ly", ""), ("es", ""), ("s", "")):
        if word.endswith(suffix) and len(word) - len(suffix) >= 3:
            word = word[: -len(suffix)] + replacement
            break
    if len(word) >= 4 and word[-1] == word[-2] and word[-1] not in "aeiousz":
        word = word[:-1]
    if len(word) >= 5 and word.endswith("e"):
        word = word[:-1]
    return word

def similar_forms(term, archive):
    """Other grammatical forms of term: via the archive's lemma table when
    present (the same data the web UI's "similar words" box uses), else by
    grouping the archive's own vocabulary with light_stem."""
    maps = archive.lemma_maps()
    if maps:
        word_to_lemma, lemma_to_forms = maps
        lemma = word_to_lemma.get(term, term)
        return {term, lemma} | set(lemma_to_forms.get(lemma, []))
    return {term} | set(archive.stem_groups().get(light_stem(term), []))

def fuzzy_forms(term, archive, count=8, cutoff=0.8):
    candidates = [word for word in archive.vocab()
                  if abs(len(word) - len(term)) <= 2]
    return {term} | set(difflib.get_close_matches(term, candidates, n=count, cutoff=cutoff))

def expand_terms(terms, archive, similar, fuzzy):
    groups = []
    for term in terms:
        variants = {term}
        if similar:
            variants |= similar_forms(term, archive)
        if fuzzy:
            variants |= fuzzy_forms(term, archive)
        groups.append((term, sorted(variants)))
    return groups

def report_expansions(groups):
    for term, variants in groups:
        if len(variants) > 1:
            note(f"{term} -> {' '.join(variants)}")

def alternation(variants):
    return "(?:" + "|".join(re.escape(v) for v in sorted(variants)) + ")"

def words_pattern(variant_sets, joined):
    """Regex for whole words. joined=True keeps the groups in sequence (a
    phrase, tolerating punctuation between words); False ORs them together."""
    if joined:
        body = r"[^A-Za-z0-9]+".join(alternation(vs) for vs in variant_sets)
    else:
        body = alternation(set().union(*variant_sets))
    return r"(?<![A-Za-z0-9])" + body + r"(?![A-Za-z0-9])"

def phrase_pattern(query, archive, similar, fuzzy, ignore_case):
    terms = tokenize(query)
    if not terms:
        return None
    groups = expand_terms(terms, archive, similar, fuzzy)
    report_expansions(groups)
    flags = re.IGNORECASE if ignore_case else 0
    return re.compile(words_pattern([set(v) for _, v in groups], joined=True), flags)

def build_search_patterns(args, archive):
    """Return (primary, requires): episodes must match every pattern in
    `requires`; snippets and counts come from `primary`."""
    expanded = args.similar or args.fuzzy or args.any
    if args.regex and expanded:
        sys.exit("--regex cannot be combined with --any, --similar, or --fuzzy.")
    if args.all and args.any:
        sys.exit("Use either --all or --any, not both.")
    ignore_case = not args.case_sensitive

    if not expanded:
        if args.all and not args.regex:
            terms = [term for term in args.query.split() if term]
            if not terms:
                return None, []
            patterns = [compile_query(term, False, ignore_case) for term in terms]
            return patterns[0], patterns
        primary = compile_query(args.query, args.regex, ignore_case)
        return primary, [primary]

    terms = tokenize(args.query)
    if not terms:
        return None, []
    flags = re.IGNORECASE if ignore_case else 0
    groups = expand_terms(terms, archive, args.similar, args.fuzzy)
    report_expansions(groups)
    variant_sets = [set(variants) for _, variants in groups]
    if args.any:
        primary = re.compile(words_pattern(variant_sets, joined=False), flags)
        return primary, [primary]
    if args.all:
        patterns = [re.compile(words_pattern([vs], joined=False), flags)
                    for vs in variant_sets]
        return patterns[0], patterns
    primary = re.compile(words_pattern(variant_sets, joined=True), flags)
    return primary, [primary]

def search_episodes(archive, primary, requires, since, until, context_chars,
                    max_per_episode, include_hits=True, need_hit_count=False):
    """Search transcripts and return SearchResult rows sorted by date.

    Snippets are collected from `primary` only, which keeps AND searches
    focused and cheap enough for exploratory agent use.
    """
    if primary is None:
        return []
    results = []
    for row in archive.rows(since, until):
        if not all(pattern.search(row.words) for pattern in requires):
            continue
        hit_count = count_hits(primary, row.words) if need_hit_count else None
        if include_hits:
            episode = row.episode or archive.episode_at(row.anchor)
            hits = find_hits(episode, row.anchor, primary, context_chars, max_per_episode)
        else:
            episode = row.episode or {"published": row.published,
                                      "title": row.title, "link": row.link}
            hits = []
        if hit_count is None:
            hit_count = len(hits)
        results.append(SearchResult(episode, row.anchor, hits, hit_count))

    results.sort(key=lambda row: row.episode.get("published", ""))
    return results

def near_clusters(words, slot_patterns, window):
    """Find word-index spans where every slot pattern matches within `window`
    words of the others; overlapping spans are merged."""
    starts = word_starts(words)
    events = []
    for slot, pattern in enumerate(slot_patterns):
        for match in pattern.finditer(words):
            events.append((word_index_at_offset(starts, match.start()), slot))
    events.sort()

    counts, have, left, spans = collections.Counter(), 0, 0, []
    for right_index, right_slot in events:
        counts[right_slot] += 1
        if counts[right_slot] == 1:
            have += 1
        while have == len(slot_patterns):
            left_index, left_slot = events[left]
            if right_index - left_index <= window:
                spans.append((left_index, right_index))
            counts[left_slot] -= 1
            if not counts[left_slot]:
                have -= 1
            left += 1

    merged = []
    for lo, hi in spans:
        if merged and lo <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], hi)
        else:
            merged.append([lo, hi])
    return merged, starts

def span_snippet(words, starts, lo, hi, context_chars, highlight):
    char_lo = starts[lo]
    char_hi = starts[hi + 1] - 1 if hi + 1 < len(starts) else len(words)
    left = max(0, char_lo - context_chars)
    right = min(len(words), char_hi + context_chars)
    text = highlight.sub(lambda m: f">>{m.group(0)}<<", words[left:right])
    return (("..." if left > 0 else "")
            + text.replace("\n", " ")
            + ("..." if right < len(words) else ""))

def rank_summaries(archive, query, title_weight, similar, fuzzy, since=None, until=None):
    """Rank episodes by how well their summary and title match the query.

    Scores with TF-IDF: each query term's weight is its inverse document
    frequency across all summaries, so common words count for little and
    distinctive ones dominate. Title hits are counted `title_weight` times.
    """
    query_terms = sorted(set(tokenize(query)))
    if not query_terms:
        return []
    if similar or fuzzy:
        groups = expand_terms(query_terms, archive, similar, fuzzy)
        report_expansions(groups)
        query_terms = sorted(set().union(*(set(v) for _, v in groups)))
    query_terms = set(query_terms)

    episode_rows = []
    document_freq = collections.Counter()
    for row in archive.rows(since, until):
        counts = collections.Counter(tokenize(row.summary))
        for token in tokenize(row.title):
            counts[token] += title_weight
        episode_rows.append((row, counts))
        for term in query_terms:
            if counts[term]:
                document_freq[term] += 1

    total_docs = len(episode_rows)
    idf = {
        term: math.log((total_docs + 1) / (document_freq[term] + 1)) + 1
        for term in query_terms
    }

    ranked = []
    for row, counts in episode_rows:
        score, matched = 0.0, []
        for term in query_terms:
            tf = counts[term]
            if tf:
                score += (1 + math.log(tf)) * idf[term]
                matched.append(term)
        if score > 0:
            episode = row.episode or {"published": row.published, "title": row.title,
                                      "link": row.link, "summary": row.summary}
            ranked.append((episode, row.anchor, score, sorted(matched)))

    ranked.sort(key=lambda row: row[2], reverse=True)
    return ranked

def rank_transcripts(archive, query, similar, fuzzy, since=None, until=None):
    """TF-IDF rank over the spoken transcripts; each query word and its
    expansions count as one term."""
    terms = sorted(set(tokenize(query)))
    if not terms:
        return []
    groups = expand_terms(terms, archive, similar, fuzzy)
    report_expansions(groups)
    patterns = [(term, re.compile(words_pattern([set(variants)], joined=False),
                                  re.IGNORECASE))
                for term, variants in groups]

    scored_rows = []
    document_freq = collections.Counter()
    total_docs = 0
    for row in archive.rows(since, until):
        total_docs += 1
        frequencies = {}
        for term, pattern in patterns:
            count = count_hits(pattern, row.words)
            if count:
                frequencies[term] = count
        if frequencies:
            scored_rows.append((row, frequencies))
            for term in frequencies:
                document_freq[term] += 1

    idf = {term: math.log((total_docs + 1) / (document_freq[term] + 1)) + 1
           for term, _ in patterns}
    ranked = []
    for row, frequencies in scored_rows:
        score = sum((1 + math.log(tf)) * idf[term]
                    for term, tf in frequencies.items())
        episode = row.episode or {"published": row.published, "title": row.title,
                                  "link": row.link, "summary": row.summary}
        ranked.append((episode, row.anchor, score, sorted(frequencies)))
    ranked.sort(key=lambda row: row[2], reverse=True)
    return ranked

def parse_anchor_selector(selector):
    selector = selector.strip()
    if "#" in selector:
        selector = selector.rsplit("#", 1)[1]
    if re.fullmatch(r"\d+,\d+,\d+,\d+(,\d+)?", selector):
        return tuple(int(part) for part in selector.split(",")[:4])
    return None

def resolve_episode(archive, selector):
    """Find one episode by title substring or transcript anchor."""
    anchor = parse_anchor_selector(selector)
    if anchor:
        try:
            return anchor, archive.episode_at(anchor)
        except (IndexError, KeyError, TypeError, FileNotFoundError) as err:
            sys.exit(f"Anchor {selector!r} does not identify an episode: {err}")

    needle = selector.lower()
    matches = [(row.anchor, row.published, row.title)
               for row in archive.rows()
               if needle in row.title.lower()]
    if not matches:
        sys.exit(f"No episode title matches {selector!r}.")
    if len(matches) > 1:
        listing = "\n  ".join(f"{published}  {title}"
                              for _, published, title in matches)
        sys.exit(f"{selector!r} is ambiguous; matches:\n  {listing}")
    return matches[0][0], archive.episode_at(matches[0][0])

def cmd_info(archive, args):
    header = archive.header
    cache = archive.cache()
    if not archive.cache_enabled:
        cache_state = "disabled (--no-cache)"
    elif cache:
        cache_state = f"{os.path.basename(cache.path)} (built {cache.meta.get('built')})"
    else:
        cache_state = "unavailable"
    if args.json:
        print(json.dumps(dict(header, batches=len(archive.batch_slices),
                              cache=cache_state), indent=2))
        return
    print(f"Archive directory : {archive.dir}")
    print(f"Built             : {header.get('created')}")
    print(f"Episodes          : {header.get('items')}")
    print(f"Batches           : {len(archive.batch_slices)}")
    print(
        f"Default context   : {header.get('before')} words before / "
        f"{header.get('after')} after"
    )
    print(f"Cache             : {cache_state}")

def cmd_list(archive, args):
    rows = []
    for row in archive.rows(args.since, args.until):
        rows.append((row.published, row.title, row.anchor))
    rows.sort()
    if args.limit:
        rows = rows[: args.limit]
    if args.json:
        print(json.dumps([
            {"published": published, "title": title, "anchor": anchor_str(anchor)}
            for published, title, anchor in rows
        ], indent=2))
        return
    for published, title, _ in rows:
        print(f"{published}  {title}")
    print(f"\n{len(rows)} episode(s).", file=sys.stderr)

def cmd_search(archive, args):
    primary, requires = build_search_patterns(args, archive)
    results = search_episodes(
        archive,
        primary,
        requires,
        args.since,
        args.until,
        args.context,
        args.max_snippets,
        include_hits=not args.counts,
        need_hit_count=args.counts or args.json,
    )
    if args.limit:
        results = results[: args.limit]

    if args.json:
        payload = []
        for result in results:
            episode, anchor, hits = result.episode, result.anchor, result.hits
            payload.append({
                "published": episode.get("published"),
                "title": episode.get("title"),
                "link": episode.get("link"),
                "hit_count": result.hit_count,
                "anchor": anchor_str(anchor),
                "snippets": [{
                    "timestamp": fmt_time(hit.timestamp),
                    "word_index": hit.word_index,
                    "text": hit.snippet,
                    "anchor": anchor_str(anchor, hit.word_index),
                } for hit in hits],
            })
        print(json.dumps(payload, indent=2))
        return

    if not results:
        print("No matches.", file=sys.stderr)
        return

    hit_total, shown_total = 0, 0
    for result in results:
        episode, anchor, hits = result.episode, result.anchor, result.hits
        hit_total += result.hit_count
        shown_total += len(hits)
        print(f"\n{'=' * 78}\n{episode.get('published')}  {episode.get('title')}")
        print(f"  link   : {episode.get('link')}")
        print(f"  open   : {anchor_str(anchor)}")
        if args.counts:
            print(f"  hits   : {result.hit_count}")
            continue
        for hit in hits:
            print(f"  [{fmt_time(hit.timestamp)}] {hit.snippet}")
            print(f"           -> {anchor_str(anchor, hit.word_index)}")

    if args.counts:
        print(f"\n{len(results)} episode(s), {hit_total} hit(s).", file=sys.stderr)
    else:
        print(f"\n{len(results)} episode(s), {shown_total} snippet(s) shown.", file=sys.stderr)

def cmd_near(archive, args):
    if len(args.terms) < 2:
        sys.exit('Give at least two word groups, e.g.: near "fly,insect" "robot,machine"')
    flags = 0 if args.case_sensitive else re.IGNORECASE
    slot_patterns, every_variant = [], set()
    for raw in args.terms:
        alternatives = tokenize(raw.replace(",", " "))
        if not alternatives:
            sys.exit(f"No searchable words in group {raw!r}.")
        groups = expand_terms(alternatives, archive, args.similar, args.fuzzy)
        report_expansions(groups)
        variants = set().union(*(set(v) for _, v in groups))
        every_variant |= variants
        slot_patterns.append(re.compile(words_pattern([variants], joined=False), flags))
    highlight = re.compile(words_pattern([every_variant], joined=False), flags)

    found = []
    for row in archive.rows(args.since, args.until):
        if not all(pattern.search(row.words) for pattern in slot_patterns):
            continue
        clusters, starts = near_clusters(row.words, slot_patterns, args.window)
        if not clusters:
            continue
        hits = []
        if not args.counts:
            episode = row.episode or archive.episode_at(row.anchor)
            times = word_timestamps(episode.get("start", []))
            for lo, hi in clusters[: args.max_snippets]:
                timestamp = times[lo] if lo < len(times) else 0
                snippet = span_snippet(row.words, starts, lo, hi, args.context, highlight)
                hits.append(Hit(row.anchor, None, lo, snippet, timestamp))
        found.append((row, len(clusters), hits))

    found.sort(key=lambda item: item[0].published)
    if args.limit:
        found = found[: args.limit]

    if args.json:
        print(json.dumps([{
            "published": row.published,
            "title": row.title,
            "link": row.link,
            "cluster_count": cluster_count,
            "anchor": anchor_str(row.anchor),
            "snippets": [{
                "timestamp": fmt_time(hit.timestamp),
                "word_index": hit.word_index,
                "text": hit.snippet,
                "anchor": anchor_str(row.anchor, hit.word_index),
            } for hit in hits],
        } for row, cluster_count, hits in found], indent=2))
        return

    if not found:
        print("No passages where every group occurs together.", file=sys.stderr)
        return

    cluster_total = 0
    for row, cluster_count, hits in found:
        cluster_total += cluster_count
        print(f"\n{'=' * 78}\n{row.published}  {row.title}")
        print(f"  link   : {row.link}")
        print(f"  open   : {anchor_str(row.anchor)}")
        if args.counts:
            print(f"  passages: {cluster_count}")
            continue
        for hit in hits:
            print(f"  [{fmt_time(hit.timestamp)}] {hit.snippet}")
            print(f"           -> {anchor_str(row.anchor, hit.word_index)}")
    print(f"\n{len(found)} episode(s), {cluster_total} passage(s).", file=sys.stderr)

def cmd_vocab(archive, args):
    vocab = archive.vocab()
    if args.pattern:
        needle = args.pattern.lower()
        terms = {term for term in vocab if needle in term}
        if args.similar:
            terms |= {term for term in similar_forms(needle, archive) if term in vocab}
        if args.fuzzy:
            terms |= {term for term in fuzzy_forms(needle, archive, count=25, cutoff=0.74)
                      if term in vocab}
    else:
        terms = set(vocab)

    ranked = sorted(((vocab[term][1], vocab[term][0], term)
                     for term in terms if vocab[term][1] >= args.min_count),
                    reverse=True)
    if args.limit:
        ranked = ranked[: args.limit]
    if args.json:
        print(json.dumps([
            {"term": term, "occurrences": total, "episodes": episodes}
            for total, episodes, term in ranked
        ], indent=2))
        return
    if not ranked:
        print("No matching words in the archive.", file=sys.stderr)
        return
    print(f"{'occurrences':>11}  {'episodes':>8}  word")
    for total, episodes, term in ranked:
        print(f"{total:>11}  {episodes:>8}  {term}")
    print(f"\n{len(ranked)} word(s) shown; the archive has {len(vocab)} distinct words.",
          file=sys.stderr)

def cmd_context(archive, args):
    anchor, episode = resolve_episode(archive, args.episode)
    if args.similar or args.fuzzy:
        if args.regex:
            sys.exit("--regex cannot be combined with --similar or --fuzzy.")
        pattern = phrase_pattern(args.query, archive, args.similar, args.fuzzy,
                                 not args.case_sensitive)
        if pattern is None:
            sys.exit("No searchable words in query.")
    else:
        pattern = compile_query(args.query, args.regex, not args.case_sensitive)
    hits = find_hits(episode, anchor, pattern, args.context, None)
    if args.json:
        print(json.dumps({
            "published": episode.get("published"),
            "title": episode.get("title"),
            "link": episode.get("link"),
            "hits": [{
                "timestamp": fmt_time(hit.timestamp),
                "word_index": hit.word_index,
                "text": hit.snippet,
                "anchor": anchor_str(anchor, hit.word_index),
            } for hit in hits],
        }, indent=2))
        return
    print(f"{episode.get('published')}  {episode.get('title')}\n{episode.get('link')}\n")
    for hit in hits:
        print(f"[{fmt_time(hit.timestamp)}] {hit.snippet}")
        print(f"    -> {anchor_str(anchor, hit.word_index)}\n")
    print(f"{len(hits)} match(es).", file=sys.stderr)

def cmd_transcript(archive, args):
    anchor, episode = resolve_episode(archive, args.episode)
    words = episode.get("words", "").split(" ")
    chars = episode.get("speaker", "")
    times = word_timestamps(episode.get("start", []))
    labels = speaker_labels(episode)

    first, last = 0, len(words)
    if args.start:
        first = bisect.bisect_left(times, parse_clock(args.start))
    if args.end:
        last = bisect.bisect_right(times, parse_clock(args.end))

    if args.json:
        print(json.dumps({
            "published": episode.get("published"),
            "title": episode.get("title"),
            "link": episode.get("link"),
            "summary": episode.get("summary"),
            "transcript": " ".join(words[first:last]),
        }, indent=2))
        return

    print(f"{episode.get('published')}  {episode.get('title')}\n{episode.get('link')}\n")
    if args.summary and episode.get("summary"):
        print(f"SUMMARY:\n{episode['summary']}\n")
    line, last_char = [], None
    for i in range(first, last):
        ch = chars[i] if i < len(chars) else last_char
        if ch != last_char:
            if line:
                print(" ".join(line))
            last_char = ch
            stamp = f"[{fmt_time(times[i])}] " if args.timestamps and i < len(times) else ""
            line = [f"\n{stamp}{labels.get(ch, 'Speaker')}:"]
        line.append(words[i])
    if line:
        print(" ".join(line))

def cmd_summaries(archive, args):
    needle = (args.query or "").lower()
    rows = []
    for row in archive.rows(args.since, args.until):
        if needle and needle not in row.summary.lower() and needle not in row.title.lower():
            continue
        rows.append(row)
    rows.sort(key=lambda row: row.published)
    if args.limit:
        rows = rows[: args.limit]
    if args.json:
        print(json.dumps([
            {
                "published": row.published,
                "title": row.title,
                "link": row.link,
                "anchor": anchor_str(row.anchor),
                "summary": row.summary,
            }
            for row in rows
        ], indent=2))
        return
    for row in rows:
        print(f"\n{row.published}  {row.title}\n  {anchor_str(row.anchor)}\n  {row.summary}")
    print(f"\n{len(rows)} episode(s).", file=sys.stderr)

def cmd_rank(archive, args):
    if args.transcripts:
        ranked = rank_transcripts(archive, args.query, args.similar, args.fuzzy,
                                  args.since, args.until)
    else:
        ranked = rank_summaries(archive, args.query, args.title_weight,
                                args.similar, args.fuzzy, args.since, args.until)
    if args.limit:
        ranked = ranked[: args.limit]
    if args.json:
        print(json.dumps([
            {
                "score": round(score, 3),
                "published": episode.get("published"),
                "title": episode.get("title"),
                "link": episode.get("link"),
                "matched_terms": matched,
                "anchor": anchor_str(anchor),
                "summary": episode.get("summary"),
            }
            for episode, anchor, score, matched in ranked
        ], indent=2))
        return
    if not ranked:
        print("No episode matched any query term.", file=sys.stderr)
        return
    for episode, anchor, score, matched in ranked:
        print(f"\n[{score:6.2f}] {episode.get('published')}  {episode.get('title')}")
        print(f"         terms: {', '.join(matched)}")
        print(f"         open : {anchor_str(anchor)}")
        if args.summaries:
            print(f"         {episode.get('summary', '') or ''}")
    print(f"\n{len(ranked)} episode(s) ranked.", file=sys.stderr)

def cmd_cache(archive, args):
    path = os.path.join(archive.dir, CACHE_NAME)
    if args.delete:
        if os.path.exists(path):
            os.remove(path)
            print(f"Deleted {path}")
        else:
            print("No cache file to delete.")
        return
    if args.rebuild and os.path.exists(path):
        os.remove(path)
    cache = archive.cache()
    payload = {"path": path, "present": cache is not None,
               "enabled": archive.cache_enabled}
    if cache:
        payload.update({
            "built": cache.meta.get("built"),
            "episodes": int(cache.meta.get("episodes", 0)),
            "words": cache.conn.execute("SELECT COUNT(*) FROM vocab").fetchone()[0],
            "bytes": os.path.getsize(path),
        })
    if args.json:
        print(json.dumps(payload, indent=2))
        return
    print(f"Cache file : {path}")
    if not archive.cache_enabled:
        print("Status     : disabled (--no-cache)")
    elif cache:
        print(f"Status     : ready (built {payload['built']})")
        print(f"Episodes   : {payload['episodes']}")
        print(f"Words      : {payload['words']} distinct")
        print(f"Size       : {payload['bytes']:,} bytes")
    else:
        print("Status     : unavailable (see notes above); scans read the archive directly")

def run_batch_line(archive, parser, sub_argv):
    sub_args = parser.parse_args(sub_argv)
    if sub_args.command == "batch":
        raise SystemExit("batch cannot run inside batch")
    explicit = any(arg == "--archive" or arg.startswith("--archive=") for arg in sub_argv)
    if explicit and os.path.abspath(sub_args.archive) != os.path.abspath(archive.dir):
        raise SystemExit("--archive cannot change inside batch")
    sub_args.func(archive, sub_args)

def cmd_batch(archive, args):
    lines = list(args.commands)
    if not lines:
        lines = [line.strip() for line in sys.stdin
                 if line.strip() and not line.strip().startswith("#")]
    if not lines:
        sys.exit("No commands given (pass them as arguments, or one per line on stdin).")

    parser = build_parser()
    results, failures = [], 0
    for number, line in enumerate(lines, 1):
        error = None
        try:
            sub_argv = shlex.split(line)
        except ValueError as err:
            sub_argv, error = [], f"unparsable command: {err}"
        while sub_argv and (sub_argv[0] in ("python", "python3")
                            or sub_argv[0].endswith(".py")):
            sub_argv.pop(0)

        if args.json:
            out_buffer, err_buffer = io.StringIO(), io.StringIO()
            if error is None:
                try:
                    with contextlib.redirect_stdout(out_buffer), \
                            contextlib.redirect_stderr(err_buffer):
                        run_batch_line(archive, parser, sub_argv)
                except SystemExit as exc:
                    if exc.code not in (0, None):
                        error = exc.code if isinstance(exc.code, str) else f"exit status {exc.code}"
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
            output = out_buffer.getvalue()
            try:
                output = json.loads(output)
            except ValueError:
                pass
            entry = {"command": line, "ok": error is None, "output": output}
            if err_buffer.getvalue().strip():
                entry["notes"] = err_buffer.getvalue().strip()
            if error is not None:
                entry["error"] = str(error)
            results.append(entry)
        else:
            print(f"\n### [{number}/{len(lines)}] {line}")
            if error is None:
                try:
                    run_batch_line(archive, parser, sub_argv)
                except SystemExit as exc:
                    if exc.code not in (0, None):
                        error = exc.code if isinstance(exc.code, str) else f"exit status {exc.code}"
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
            sys.stdout.flush()
            if error is not None:
                print(f"### error: {error}", file=sys.stderr)
        if error is not None:
            failures += 1

    if args.json:
        print(json.dumps(results, indent=2))
    if failures:
        print(f"\n{failures} of {len(lines)} command(s) failed.", file=sys.stderr)

def build_parser():
    parser = argparse.ArgumentParser(
        description=f"Search a compressed podcast-transcript archive (search_data_NN.dat).\n\n{AGENT_HELP}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Run a subcommand with --help for command-specific options.",
    )
    parser.add_argument(
        "--archive",
        default=os.path.dirname(os.path.abspath(__file__)),
        help="Directory holding the search_data_NN.dat files (default: this script's folder).",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Skip the sqlite cache and read the archive directly.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("info", help="Print archive metadata.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_info)

    p = sub.add_parser("list", help="List episodes (date + title).")
    p.add_argument("--limit", type=int, default=0, help="Show at most N episodes.")
    p.add_argument("--since", help="Only episodes on/after YYYY-MM-DD.")
    p.add_argument("--until", help="Only episodes on/before YYYY-MM-DD.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("search", help="Full-text search transcripts; show snippets.")
    p.add_argument(
        "query",
        help="Text to find: phrase text, a regex with --regex, AND terms with --all, OR terms with --any.",
    )
    p.add_argument("--regex", action="store_true", help="Treat query as a regular expression.")
    p.add_argument("--all", action="store_true", help="Require ALL whitespace-separated terms in an episode.")
    p.add_argument("--any", action="store_true", help="Match episodes containing ANY query word (whole-word OR).")
    p.add_argument("--similar", action="store_true", help="Also match other grammatical forms of each word (fly/flies/flew).")
    p.add_argument("--fuzzy", action="store_true", help="Also match close spellings from the archive (catches transcription errors).")
    p.add_argument("--case-sensitive", action="store_true")
    p.add_argument("--counts", action="store_true", help="Only report per-episode hit counts, not snippets.")
    p.add_argument("--max-snippets", type=int, default=3, help="Max snippets shown per episode (default 3).")
    p.add_argument("--context", type=int, default=200, help="Characters of context on each side of a hit (default 200).")
    p.add_argument("--limit", type=int, default=0, help="Show at most N matching episodes after date sorting.")
    p.add_argument("--since", help="Only episodes on/after YYYY-MM-DD.")
    p.add_argument("--until", help="Only episodes on/before YYYY-MM-DD.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_search)

    p = sub.add_parser("near", help="Find passages where a word from EVERY group occurs close together.")
    p.add_argument(
        "terms",
        nargs="+",
        metavar="GROUP",
        help="Word group; commas separate alternatives, e.g. 'fly,insect' 'robot,machine'.",
    )
    p.add_argument("--window", type=int, default=50, help="Max words between the first and last matched word (default 50).")
    p.add_argument("--similar", action="store_true", help="Also match other grammatical forms of each word.")
    p.add_argument("--fuzzy", action="store_true", help="Also match close spellings from the archive.")
    p.add_argument("--case-sensitive", action="store_true")
    p.add_argument("--counts", action="store_true", help="Only report per-episode passage counts, not snippets.")
    p.add_argument("--max-snippets", type=int, default=3, help="Max passages shown per episode (default 3).")
    p.add_argument("--context", type=int, default=150, help="Characters of context around each passage (default 150).")
    p.add_argument("--limit", type=int, default=0, help="Show at most N matching episodes after date sorting.")
    p.add_argument("--since", help="Only episodes on/after YYYY-MM-DD.")
    p.add_argument("--until", help="Only episodes on/before YYYY-MM-DD.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_near)

    p = sub.add_parser("vocab", help="List words that actually occur in the transcripts.")
    p.add_argument("pattern", nargs="?", help="Substring to look for; omit to list the most common words.")
    p.add_argument("--similar", action="store_true", help="Include other grammatical forms of the pattern.")
    p.add_argument("--fuzzy", action="store_true", help="Include close spellings of the pattern.")
    p.add_argument("--limit", type=int, default=30, help="Show at most N words (default 30, 0=all).")
    p.add_argument("--min-count", type=int, default=1, help="Hide words occurring fewer than N times.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_vocab)

    p = sub.add_parser("context", help="Show all matches of a query within ONE episode.")
    p.add_argument("query")
    p.add_argument(
        "--episode",
        required=True,
        help="Title substring, 'file,start,len,itemID', or search.html# anchor.",
    )
    p.add_argument("--regex", action="store_true")
    p.add_argument("--similar", action="store_true", help="Also match other grammatical forms of each word.")
    p.add_argument("--fuzzy", action="store_true", help="Also match close spellings from the archive.")
    p.add_argument("--case-sensitive", action="store_true")
    p.add_argument("--context", type=int, default=300, help="Characters of context on each side (default 300).")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_context)

    p = sub.add_parser("transcript", help="Print one episode's transcript (optionally a time slice).")
    p.add_argument("episode", help="Title substring, 'file,start,len,itemID', or search.html# anchor.")
    p.add_argument("--timestamps", action="store_true", help="Prefix each speaker turn with a time.")
    p.add_argument("--summary", action="store_true", help="Print the episode summary first.")
    p.add_argument("--start", help="Only print words spoken at/after this time (H:MM:SS, MM:SS, or seconds).")
    p.add_argument("--end", help="Only print words spoken at/before this time.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_transcript)

    p = sub.add_parser("summaries", help="List/filter episode summaries by substring.")
    p.add_argument("query", nargs="?", help="Optional substring to filter summaries/titles.")
    p.add_argument("--limit", type=int, default=0, help="Show at most N matching episodes.")
    p.add_argument("--since", help="Only episodes on/after YYYY-MM-DD.")
    p.add_argument("--until", help="Only episodes on/before YYYY-MM-DD.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_summaries)

    p = sub.add_parser("rank", help="Relevance-rank episodes against a query (TF-IDF).")
    p.add_argument("query", help="Topic words; matching is per-word, not an exact phrase.")
    p.add_argument("--transcripts", action="store_true", help="Rank by words spoken in transcripts instead of titles/summaries.")
    p.add_argument("--similar", action="store_true", help="Also count other grammatical forms of each word.")
    p.add_argument("--fuzzy", action="store_true", help="Also count close spellings from the archive.")
    p.add_argument("--limit", type=int, default=15, help="Show top N episodes (default 15, 0=all).")
    p.add_argument(
        "--title-weight",
        type=int,
        default=3,
        help="How many times to count a title word vs a summary word (default 3; summary mode only).",
    )
    p.add_argument("--since", help="Only episodes on/after YYYY-MM-DD.")
    p.add_argument("--until", help="Only episodes on/before YYYY-MM-DD.")
    p.add_argument("--summaries", action="store_true", help="Print each episode's summary too.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_rank)

    p = sub.add_parser("cache", help="Build/refresh the sqlite speed-up cache and report its status.")
    p.add_argument("--rebuild", action="store_true", help="Force a full rebuild.")
    p.add_argument("--delete", action="store_true", help="Delete the cache file and exit.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_cache)

    p = sub.add_parser("batch", help="Run several commands in one invocation (one approval, shared caches).")
    p.add_argument(
        "commands",
        nargs="*",
        metavar="CMD",
        help="Each CMD is one full command line, e.g. 'search \"mars\" --counts'. "
             "With no CMDs, commands are read one per line from stdin "
             "(blank lines and # comments are skipped).",
    )
    p.add_argument("--json", action="store_true", help="Emit one JSON array of per-command results.")
    p.set_defaults(func=cmd_batch)

    return parser

def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    parser = build_parser()
    if not argv:
        parser.print_help(sys.stdout)
        return

    args = parser.parse_args(argv)
    try:
        archive = Archive(args.archive)
    except FileNotFoundError as err:
        sys.exit(f"Could not open archive in {args.archive!r}: {err}")
    archive.cache_enabled = not args.no_cache
    try:
        args.func(archive, args)
    except BrokenPipeError:
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(1)

if __name__ == "__main__":
    main()
