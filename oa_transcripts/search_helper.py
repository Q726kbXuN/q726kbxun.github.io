#!/usr/bin/env python3
"""
-- The on-disk search format ----------------------------------------
Files are named `search_data_NN.dat` (NN = zero-padded integer) and live in one
directory. All reads are (file_number, byte_start, byte_length) slices, so the
browser can fetch tiny ranges over HTTP; we just seek() instead.

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
   - `words`     : the full transcript as a single space-joined string. Token i
                   (words.split(' ')[i]) is the i-th spoken word.
   - `start`     : list, one int per word: the PER-WORD DELTA in seconds since
                   the previous word. A running cumulative sum gives each word's
                   absolute timestamp. len(start) == number of words.
   - `speaker`   : string, one CHAR per word; equal chars == same speaker.
                   Optional `speakers` dict maps that char -> a display name.
   - `segments`  : optional {offset:[secs...], title:[...]} chapter markers.
   - `group`     : optional tag (some archives partition episodes into groups).

   `words`, `start`, and `speaker` are index-aligned: the same index i refers to
   the same spoken word in all three.

-- Linking back to the Web UI ---------------------------------------
`search.html#<file>,<start>,<len>,<itemID>` opens one transcript; appending
`,<wordIndex>` scrolls to (and highlights) a specific word. This script prints
those anchors so a finding can be handed back to a human in the browser.

-- Usage (run --help on any subcommand for details) -----------------
python3 podcast_archive.py info
python3 podcast_archive.py list [--limit N] [--since YYYY-MM-DD]
python3 podcast_archive.py search "search term" [--regex] [--all]
python3 podcast_archive.py context "search term" --episode "Blackstock"
python3 podcast_archive.py transcript "Episode" [--timestamps]
python3 podcast_archive.py summaries "search term"
python3 podcast_archive.py rank "search term"

Add `--json` to most commands for machine-readable output. By default the 
archive directory is the script's own folder; override with
`--archive /path/to/dir`.

-- Agents: Read this! -----------------------------------------------
How to answer a natural-language question with this tool:

1. `rank "<the whole question in keywords>"` to get the most topically
    relevant episodes by summary, even when no single exact phrase appears.
2. `search "<key phrase>" --counts` to see which episodes mention a concrete
    phrase most; use --regex for synonym alternation, --all for AND-of-terms.
3. `context "<phrase>" --episode <title-or-id>` on the top candidates to read
    the surrounding dialogue and CONFIRM the host actually discusses the topic
    (don't trust a raw keyword hit -- read it).
4. Report the title, date, link, the confirming quotes, and the
    `search.html#...` anchor from step 3 so a human can jump straight there.
"""

import argparse
import collections
import gzip
import json
import math
import os
import re
import sys

class Archive:
    HEADER_LEN = 100

    def __init__(self, directory):
        self.dir = directory
        self._batch_cache = {}
        self.header = self._load_header()
        self.batch_slices = self._read_gzip_slice(*self.header["data"])

    def _path(self, file_num):
        return os.path.join(self.dir, "search_data_%02d.dat" % file_num)

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
        """Yield (anchor, episode) for every episode, where anchor is the
        (file, start, len, itemID) tuple used to build a deep link."""
        for batch_slice in self.batch_slices:
            for item_id, episode in enumerate(self.batch(batch_slice)):
                anchor = (batch_slice[0], batch_slice[1], batch_slice[2], item_id)
                yield anchor, episode

def anchor_str(anchor, word_index=None):
    frag = ",".join(str(x) for x in anchor)
    if word_index is not None:
        frag += ",%d" % word_index
    return "search.html#" + frag

def word_timestamps(start_deltas):
    out, running = [], 0
    for delta in start_deltas:
        running += delta
        out.append(running)
    return out

def fmt_time(seconds):
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    return "%d:%02d:%02d" % (h, m, s)

def speaker_labels(episode):
    """Map each speaker-char to a display label, using the episode's `speakers`
    dict when present, otherwise stable 'Speaker A/B/...' by first appearance."""
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

def compile_query(query, regex, ignore_case):
    flags = re.IGNORECASE if ignore_case else 0
    return re.compile(query if regex else re.escape(query), flags)

def find_hits(episode, anchor, pattern, context_chars, max_per_episode):
    words = episode.get("words", "")
    times = word_timestamps(episode.get("start", []))

    hits = []
    for m in pattern.finditer(words):
        # Words are single-space separated, so spaces before the match == index.
        word_index = words.count(" ", 0, m.start())
        timestamp = times[word_index] if word_index < len(times) else 0
        left = max(0, m.start() - context_chars)
        right = min(len(words), m.end() + context_chars)
        snippet = (("..." if left > 0 else "")
                   + words[left:m.start()] + ">>" + words[m.start():m.end()] + "<<"
                   + words[m.end():right]
                   + ("..." if right < len(words) else ""))
        hits.append(Hit(anchor, episode, word_index, snippet.replace("\n", " "), timestamp))
        if max_per_episode is not None and len(hits) >= max_per_episode:
            break
    return hits

def search_episodes(archive, query, regex, match_all, ignore_case, since, until,
                    context_chars, max_per_episode):
    """Search every episode's transcript, returning (episode, anchor, hits)
    sorted by published date. With match_all, every whitespace-separated term
    must appear in the episode (snippets are collected for the first term)."""
    if match_all and not regex:
        terms = [t for t in query.split() if t]
        primary = compile_query(terms[0], False, ignore_case) if terms else None
        term_patterns = [compile_query(t, False, ignore_case) for t in terms]
    else:
        primary = compile_query(query, regex, ignore_case)
        term_patterns = [primary]

    results = []
    for anchor, episode in archive.episodes():
        published = episode.get("published", "")
        if since and published < since:
            continue
        if until and published > until:
            continue
        words = episode.get("words", "")
        if match_all and not regex:
            if not all(p.search(words) for p in term_patterns):
                continue
        elif not primary.search(words):
            continue
        results.append((episode, anchor,
                        find_hits(episode, anchor, primary, context_chars, max_per_episode)))

    results.sort(key=lambda row: row[0].get("published", ""))
    return results

def tokenize(text):
    return re.findall(r"[a-z0-9]+", text.lower())

def rank_summaries(archive, query, title_weight):
    """Rank episodes by how well their summary (and title) match the query.

    Scores with TF-IDF: each query term's weight is its inverse document
    frequency across all summaries, so common words count for little and
    distinctive ones dominate; term frequency within an episode is dampened
    with a log so a single repeated word can't run away with the ranking.
    Title hits are counted `title_weight` times to favor on-topic episodes.
    Returns (episode, anchor, score, matched_terms) sorted by score desc.
    """
    query_terms = set(tokenize(query))
    if not query_terms:
        return []

    episode_rows = []
    document_freq = collections.Counter()
    for anchor, episode in archive.episodes():
        counts = collections.Counter(tokenize(episode.get("summary", "")))
        for token in tokenize(episode.get("title", "")):
            counts[token] += title_weight
        episode_rows.append((anchor, episode, counts))
        for term in query_terms:
            if counts[term]:
                document_freq[term] += 1

    total_docs = len(episode_rows)
    idf = {term: math.log((total_docs + 1) / (document_freq[term] + 1)) + 1
           for term in query_terms}

    ranked = []
    for anchor, episode, counts in episode_rows:
        score, matched = 0.0, []
        for term in query_terms:
            tf = counts[term]
            if tf:
                score += (1 + math.log(tf)) * idf[term]
                matched.append(term)
        if score > 0:
            ranked.append((episode, anchor, score, sorted(matched)))

    ranked.sort(key=lambda row: row[2], reverse=True)
    return ranked

def resolve_episode(archive, selector):
    """Find one episode by case-insensitive title substring, or by a
    `file,start,len,itemID` anchor string. Exits if ambiguous or absent."""
    if re.fullmatch(r"\d+,\d+,\d+,\d+", selector):
        file_num, start, length, item_id = (int(x) for x in selector.split(","))
        return (file_num, start, length, item_id), archive.batch([file_num, start, length])[item_id]
    needle = selector.lower()
    matches = [(anchor, episode) for anchor, episode in archive.episodes()
               if needle in episode.get("title", "").lower()]
    if not matches:
        sys.exit("No episode title matches %r." % selector)
    if len(matches) > 1:
        listing = "\n  ".join("%s  %s" % (ep["published"], ep["title"]) for _, ep in matches)
        sys.exit("%r is ambiguous; matches:\n  %s" % (selector, listing))
    return matches[0]

def cmd_info(archive, args):
    header = archive.header
    if args.json:
        print(json.dumps(dict(header, batches=len(archive.batch_slices)), indent=2))
        return
    print("Archive directory : %s" % archive.dir)
    print("Built             : %s" % header.get("created"))
    print("Episodes          : %s" % header.get("items"))
    print("Batches           : %s" % len(archive.batch_slices))
    print("Default context   : %s words before / %s after"
          % (header.get("before"), header.get("after")))

def cmd_list(archive, args):
    rows = []
    for anchor, episode in archive.episodes():
        published = episode.get("published", "")
        if args.since and published < args.since:
            continue
        if args.until and published > args.until:
            continue
        rows.append((published, episode.get("title", ""), anchor))
    rows.sort()
    if args.limit:
        rows = rows[: args.limit]
    if args.json:
        print(json.dumps([{"published": p, "title": t, "anchor": anchor_str(a)}
                          for p, t, a in rows], indent=2))
        return
    for published, title, _ in rows:
        print("%s  %s" % (published, title))
    print("\n%d episode(s)." % len(rows), file=sys.stderr)

def cmd_search(archive, args):
    results = search_episodes(
        archive, args.query, args.regex, args.all, not args.case_sensitive,
        args.since, args.until, args.context,
        None if args.counts else args.max_snippets)
    if args.json:
        payload = [{
            "published": episode.get("published"),
            "title": episode.get("title"),
            "link": episode.get("link"),
            "hit_count": len(hits) if args.regex
            else episode.get("words", "").lower().count(args.query.lower()),
            "anchor": anchor_str(anchor),
            "snippets": [{"timestamp": fmt_time(h.timestamp),
                          "word_index": h.word_index,
                          "text": h.snippet,
                          "anchor": anchor_str(anchor, h.word_index)} for h in hits],
        } for episode, anchor, hits in results]
        print(json.dumps(payload, indent=2))
        return

    if not results:
        print("No matches.", file=sys.stderr)
        return
    total = 0
    for episode, anchor, hits in results:
        total += len(hits)
        print("\n%s\n%s  %s" % ("=" * 78, episode.get("published"), episode.get("title")))
        print("  link   : %s" % episode.get("link"))
        print("  open   : %s" % anchor_str(anchor))
        if args.counts:
            print("  hits   : %d" % len(hits))
            continue
        for h in hits:
            print("  [%s] %s" % (fmt_time(h.timestamp), h.snippet))
            print("           -> %s" % anchor_str(anchor, h.word_index))
    print("\n%d episode(s), %d snippet(s) shown." % (len(results), total), file=sys.stderr)

def cmd_context(archive, args):
    anchor, episode = resolve_episode(archive, args.episode)
    pattern = compile_query(args.query, args.regex, not args.case_sensitive)
    hits = find_hits(episode, anchor, pattern, args.context, None)
    if args.json:
        print(json.dumps({
            "published": episode.get("published"), "title": episode.get("title"),
            "link": episode.get("link"),
            "hits": [{"timestamp": fmt_time(h.timestamp), "word_index": h.word_index,
                      "text": h.snippet, "anchor": anchor_str(anchor, h.word_index)}
                     for h in hits]}, indent=2))
        return
    print("%s  %s\n%s\n" % (episode.get("published"), episode.get("title"), episode.get("link")))
    for h in hits:
        print("[%s] %s" % (fmt_time(h.timestamp), h.snippet))
        print("    -> %s\n" % anchor_str(anchor, h.word_index))
    print("%d match(es)." % len(hits), file=sys.stderr)

def cmd_transcript(archive, args):
    anchor, episode = resolve_episode(archive, args.episode)
    words = episode.get("words", "").split(" ")
    chars = episode.get("speaker", "")
    times = word_timestamps(episode.get("start", []))
    labels = speaker_labels(episode)

    if args.json:
        print(json.dumps({"published": episode.get("published"), "title": episode.get("title"),
                          "link": episode.get("link"), "summary": episode.get("summary"),
                          "transcript": episode.get("words")}, indent=2))
        return

    print("%s  %s\n%s\n" % (episode.get("published"), episode.get("title"), episode.get("link")))
    if args.summary and episode.get("summary"):
        print("SUMMARY:\n" + episode["summary"] + "\n")
    line, last_char = [], None
    for i, word in enumerate(words):
        ch = chars[i] if i < len(chars) else last_char
        if ch != last_char:
            if line:
                print(" ".join(line))
            last_char = ch
            stamp = "[%s] " % fmt_time(times[i]) if args.timestamps and i < len(times) else ""
            line = ["\n%s%s:" % (stamp, labels.get(ch, "Speaker"))]
        line.append(word)
    if line:
        print(" ".join(line))

def cmd_summaries(archive, args):
    needle = (args.query or "").lower()
    rows = []
    for anchor, episode in archive.episodes():
        summary = episode.get("summary", "") or ""
        if needle and needle not in summary.lower() and needle not in episode.get("title", "").lower():
            continue
        rows.append((episode.get("published", ""), episode, anchor, summary))
    rows.sort(key=lambda row: row[0])
    if args.json:
        print(json.dumps([{"published": p, "title": ep.get("title"),
                           "link": ep.get("link"), "anchor": anchor_str(a),
                           "summary": s} for p, ep, a, s in rows], indent=2))
        return
    for published, episode, anchor, summary in rows:
        print("\n%s  %s\n  %s\n  %s" % (published, episode.get("title"), anchor_str(anchor), summary))
    print("\n%d episode(s)." % len(rows), file=sys.stderr)

def cmd_rank(archive, args):
    ranked = rank_summaries(archive, args.query, args.title_weight)
    if args.limit:
        ranked = ranked[: args.limit]
    if args.json:
        print(json.dumps([{"score": round(score, 3), "published": ep.get("published"),
                           "title": ep.get("title"), "link": ep.get("link"),
                           "matched_terms": matched, "anchor": anchor_str(anchor),
                           "summary": ep.get("summary")}
                          for ep, anchor, score, matched in ranked], indent=2))
        return
    if not ranked:
        print("No episode summary matched any query term.", file=sys.stderr)
        return
    for episode, anchor, score, matched in ranked:
        print("\n[%6.2f] %s  %s" % (score, episode.get("published"), episode.get("title")))
        print("         terms: %s" % ", ".join(matched))
        print("         open : %s" % anchor_str(anchor))
        if args.summaries:
            print("         %s" % (episode.get("summary", "") or ""))
    print("\n%d episode(s) ranked." % len(ranked), file=sys.stderr)

def build_parser():
    parser = argparse.ArgumentParser(
        description="Search a compressed podcast-transcript archive (search_data_NN.dat).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Run a subcommand with --help for its options. See the module docstring "
               "for the file format and an agent playbook.")
    parser.add_argument("--archive", default=os.path.dirname(os.path.abspath(__file__)),
                        help="Directory holding the search_data_NN.dat files "
                             "(default: this script's folder).")
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
    p.add_argument("query", help="Text to find (a phrase, or with --regex a pattern, "
                                 "or with --all a set of words that must all appear).")
    p.add_argument("--regex", action="store_true", help="Treat query as a regular expression.")
    p.add_argument("--all", action="store_true",
                   help="Require ALL whitespace-separated terms in an episode (AND).")
    p.add_argument("--case-sensitive", action="store_true")
    p.add_argument("--counts", action="store_true",
                   help="Only report per-episode hit counts, not snippets.")
    p.add_argument("--max-snippets", type=int, default=3,
                   help="Max snippets shown per episode (default 3).")
    p.add_argument("--context", type=int, default=200,
                   help="Characters of context on each side of a hit (default 200).")
    p.add_argument("--since", help="Only episodes on/after YYYY-MM-DD.")
    p.add_argument("--until", help="Only episodes on/before YYYY-MM-DD.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_search)

    p = sub.add_parser("context", help="Show all matches of a query within ONE episode.")
    p.add_argument("query")
    p.add_argument("--episode", required=True,
                   help="Title substring or a 'file,start,len,itemID' anchor.")
    p.add_argument("--regex", action="store_true")
    p.add_argument("--case-sensitive", action="store_true")
    p.add_argument("--context", type=int, default=300,
                   help="Characters of context on each side (default 300).")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_context)

    p = sub.add_parser("transcript", help="Print one episode's full transcript.")
    p.add_argument("episode", help="Title substring or a 'file,start,len,itemID' anchor.")
    p.add_argument("--timestamps", action="store_true", help="Prefix each speaker turn with a time.")
    p.add_argument("--summary", action="store_true", help="Print the episode summary first.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_transcript)

    p = sub.add_parser("summaries", help="List/filter episode summaries by substring.")
    p.add_argument("query", nargs="?", help="Optional substring to filter summaries/titles.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_summaries)

    p = sub.add_parser("rank", help="Relevance-rank episode summaries against a query (TF-IDF).")
    p.add_argument("query", help="Topic words; matching is per-word, not an exact phrase.")
    p.add_argument("--limit", type=int, default=15, help="Show top N episodes (default 15, 0=all).")
    p.add_argument("--title-weight", type=int, default=3,
                   help="How many times to count a title word vs a summary word (default 3).")
    p.add_argument("--summaries", action="store_true", help="Print each episode's summary too.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_rank)

    return parser

def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        archive = Archive(args.archive)
    except FileNotFoundError as err:
        sys.exit("Could not open archive in %r: %s" % (args.archive, err))
    args.func(archive, args)

if __name__ == "__main__":
    main()
