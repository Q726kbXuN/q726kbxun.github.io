#!/usr/bin/env python3
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

-- Usage ----------------------------------------------------------------------
python3 podcast_archive.py
python3 podcast_archive.py --help
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
"""

import argparse
import bisect
import collections
import gzip
import json
import math
import os
import re
import sys


AGENT_HELP = """
-- Agent Help -----------------------------------------------------------------
Use this order for natural-language questions:
1. `rank "question keywords" --limit 10 --summaries` to find episodes 
   by topic.
2. `search "concrete phrase" --counts --limit 20` to find where exact 
   language appears.
3. `context "phrase" --episode "title or search.html#anchor" --context 500` 
   to confirm the dialogue.
4. Report the title, date, link, confirming quote, timestamp, and 
   `search.html#...` anchor described below.

Fast paths:
- Use `--json` when another program will parse the result.
- Use `--since YYYY-MM-DD` and `--until YYYY-MM-DD` before scanning 
  long archives.
- Use `--regex` for synonym alternation and `--all` for AND-of-terms 
  phrase hunting.
- Do not trust raw keyword hits; use `context` or `transcript --summary 
  --timestamps` to verify.

-- Linking back to the Web UI -------------------------------------------------
`search.html#<file>,<start>,<len>,<itemID>` opens one transcript; appending
`,<wordIndex>` scrolls to (and highlights) a specific word. This script prints
those anchors so a finding can be handed back to a human in the browser.

-------------------------------------------------------------------------------

""".strip()


class Archive:
    HEADER_LEN = 100

    def __init__(self, directory):
        self.dir = directory
        self._batch_cache = {}
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


def search_episodes(archive, query, regex, match_all, ignore_case, since, until,
                    context_chars, max_per_episode, include_hits=True,
                    need_hit_count=False):
    """Search transcripts and return SearchResult rows sorted by date.

    With match_all, every whitespace-separated term must appear in the episode.
    Snippets are collected for the first term, which keeps AND searches focused
    and cheap enough for exploratory agent use.
    """
    if match_all and not regex:
        terms = [term for term in query.split() if term]
        if not terms:
            return []
        primary = compile_query(terms[0], False, ignore_case)
        term_patterns = [compile_query(term, False, ignore_case) for term in terms]
    else:
        primary = compile_query(query, regex, ignore_case)
        term_patterns = [primary]

    results = []
    for anchor, episode in archive.episodes():
        published = episode.get("published", "")
        if not in_date_range(published, since, until):
            continue

        words = episode.get("words", "")
        hit_count = None
        if match_all and not regex:
            if not all(pattern.search(words) for pattern in term_patterns):
                continue
            if need_hit_count:
                hit_count = count_hits(primary, words)
        elif need_hit_count:
            hit_count = count_hits(primary, words)
            if not hit_count:
                continue
        elif not primary.search(words):
            continue

        hits = find_hits(episode, anchor, primary, context_chars, max_per_episode) if include_hits else []
        if hit_count is None:
            hit_count = len(hits)
        results.append(SearchResult(episode, anchor, hits, hit_count))

    results.sort(key=lambda row: row.episode.get("published", ""))
    return results


def tokenize(text):
    return re.findall(r"[a-z0-9]+", text.lower())


def rank_summaries(archive, query, title_weight, since=None, until=None):
    """Rank episodes by how well their summary and title match the query.

    Scores with TF-IDF: each query term's weight is its inverse document
    frequency across all summaries, so common words count for little and
    distinctive ones dominate. Title hits are counted `title_weight` times.
    """
    query_terms = set(tokenize(query))
    if not query_terms:
        return []

    episode_rows = []
    document_freq = collections.Counter()
    for anchor, episode in archive.episodes():
        published = episode.get("published", "")
        if not in_date_range(published, since, until):
            continue

        counts = collections.Counter(tokenize(episode.get("summary", "")))
        for token in tokenize(episode.get("title", "")):
            counts[token] += title_weight
        episode_rows.append((anchor, episode, counts))
        for term in query_terms:
            if counts[term]:
                document_freq[term] += 1

    total_docs = len(episode_rows)
    idf = {
        term: math.log((total_docs + 1) / (document_freq[term] + 1)) + 1
        for term in query_terms
    }

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
        file_num, start, length, item_id = anchor
        try:
            return anchor, archive.batch([file_num, start, length])[item_id]
        except (IndexError, KeyError, TypeError, FileNotFoundError) as err:
            sys.exit(f"Anchor {selector!r} does not identify an episode: {err}")

    needle = selector.lower()
    matches = [
        (anchor, episode)
        for anchor, episode in archive.episodes()
        if needle in episode.get("title", "").lower()
    ]
    if not matches:
        sys.exit(f"No episode title matches {selector!r}.")
    if len(matches) > 1:
        listing = "\n  ".join(
            f"{ep.get('published', '')}  {ep.get('title', '')}"
            for _, ep in matches
        )
        sys.exit(f"{selector!r} is ambiguous; matches:\n  {listing}")
    return matches[0]


def cmd_info(archive, args):
    header = archive.header
    if args.json:
        print(json.dumps(dict(header, batches=len(archive.batch_slices)), indent=2))
        return
    print(f"Archive directory : {archive.dir}")
    print(f"Built             : {header.get('created')}")
    print(f"Episodes          : {header.get('items')}")
    print(f"Batches           : {len(archive.batch_slices)}")
    print(
        f"Default context   : {header.get('before')} words before / "
        f"{header.get('after')} after"
    )


def cmd_list(archive, args):
    rows = []
    for anchor, episode in archive.episodes():
        published = episode.get("published", "")
        if not in_date_range(published, args.since, args.until):
            continue
        rows.append((published, episode.get("title", ""), anchor))
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
    results = search_episodes(
        archive,
        args.query,
        args.regex,
        args.all,
        not args.case_sensitive,
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


def cmd_context(archive, args):
    anchor, episode = resolve_episode(archive, args.episode)
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

    if args.json:
        print(json.dumps({
            "published": episode.get("published"),
            "title": episode.get("title"),
            "link": episode.get("link"),
            "summary": episode.get("summary"),
            "transcript": episode.get("words"),
        }, indent=2))
        return

    print(f"{episode.get('published')}  {episode.get('title')}\n{episode.get('link')}\n")
    if args.summary and episode.get("summary"):
        print(f"SUMMARY:\n{episode['summary']}\n")
    line, last_char = [], None
    for i, word in enumerate(words):
        ch = chars[i] if i < len(chars) else last_char
        if ch != last_char:
            if line:
                print(" ".join(line))
            last_char = ch
            stamp = f"[{fmt_time(times[i])}] " if args.timestamps and i < len(times) else ""
            line = [f"\n{stamp}{labels.get(ch, 'Speaker')}:"]
        line.append(word)
    if line:
        print(" ".join(line))


def cmd_summaries(archive, args):
    needle = (args.query or "").lower()
    rows = []
    for anchor, episode in archive.episodes():
        published = episode.get("published", "")
        if not in_date_range(published, args.since, args.until):
            continue
        summary = episode.get("summary", "") or ""
        title = episode.get("title", "")
        if needle and needle not in summary.lower() and needle not in title.lower():
            continue
        rows.append((published, episode, anchor, summary))
    rows.sort(key=lambda row: row[0])
    if args.limit:
        rows = rows[: args.limit]
    if args.json:
        print(json.dumps([
            {
                "published": published,
                "title": episode.get("title"),
                "link": episode.get("link"),
                "anchor": anchor_str(anchor),
                "summary": summary,
            }
            for published, episode, anchor, summary in rows
        ], indent=2))
        return
    for published, episode, anchor, summary in rows:
        print(f"\n{published}  {episode.get('title')}\n  {anchor_str(anchor)}\n  {summary}")
    print(f"\n{len(rows)} episode(s).", file=sys.stderr)


def cmd_rank(archive, args):
    ranked = rank_summaries(archive, args.query, args.title_weight, args.since, args.until)
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
        print("No episode summary matched any query term.", file=sys.stderr)
        return
    for episode, anchor, score, matched in ranked:
        print(f"\n[{score:6.2f}] {episode.get('published')}  {episode.get('title')}")
        print(f"         terms: {', '.join(matched)}")
        print(f"         open : {anchor_str(anchor)}")
        if args.summaries:
            print(f"         {episode.get('summary', '') or ''}")
    print(f"\n{len(ranked)} episode(s) ranked.", file=sys.stderr)


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
        help="Text to find: phrase text, a regex with --regex, or AND terms with --all.",
    )
    p.add_argument("--regex", action="store_true", help="Treat query as a regular expression.")
    p.add_argument("--all", action="store_true", help="Require ALL whitespace-separated terms in an episode.")
    p.add_argument("--case-sensitive", action="store_true")
    p.add_argument("--counts", action="store_true", help="Only report per-episode hit counts, not snippets.")
    p.add_argument("--max-snippets", type=int, default=3, help="Max snippets shown per episode (default 3).")
    p.add_argument("--context", type=int, default=200, help="Characters of context on each side of a hit (default 200).")
    p.add_argument("--limit", type=int, default=0, help="Show at most N matching episodes after date sorting.")
    p.add_argument("--since", help="Only episodes on/after YYYY-MM-DD.")
    p.add_argument("--until", help="Only episodes on/before YYYY-MM-DD.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_search)

    p = sub.add_parser("context", help="Show all matches of a query within ONE episode.")
    p.add_argument("query")
    p.add_argument(
        "--episode",
        required=True,
        help="Title substring, 'file,start,len,itemID', or search.html# anchor.",
    )
    p.add_argument("--regex", action="store_true")
    p.add_argument("--case-sensitive", action="store_true")
    p.add_argument("--context", type=int, default=300, help="Characters of context on each side (default 300).")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_context)

    p = sub.add_parser("transcript", help="Print one episode's full transcript.")
    p.add_argument("episode", help="Title substring, 'file,start,len,itemID', or search.html# anchor.")
    p.add_argument("--timestamps", action="store_true", help="Prefix each speaker turn with a time.")
    p.add_argument("--summary", action="store_true", help="Print the episode summary first.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_transcript)

    p = sub.add_parser("summaries", help="List/filter episode summaries by substring.")
    p.add_argument("query", nargs="?", help="Optional substring to filter summaries/titles.")
    p.add_argument("--limit", type=int, default=0, help="Show at most N matching episodes.")
    p.add_argument("--since", help="Only episodes on/after YYYY-MM-DD.")
    p.add_argument("--until", help="Only episodes on/before YYYY-MM-DD.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_summaries)

    p = sub.add_parser("rank", help="Relevance-rank episode summaries against a query (TF-IDF).")
    p.add_argument("query", help="Topic words; matching is per-word, not an exact phrase.")
    p.add_argument("--limit", type=int, default=15, help="Show top N episodes (default 15, 0=all).")
    p.add_argument(
        "--title-weight",
        type=int,
        default=3,
        help="How many times to count a title word vs a summary word (default 3).",
    )
    p.add_argument("--since", help="Only episodes on/after YYYY-MM-DD.")
    p.add_argument("--until", help="Only episodes on/before YYYY-MM-DD.")
    p.add_argument("--summaries", action="store_true", help="Print each episode's summary too.")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_rank)

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
    args.func(archive, args)


if __name__ == "__main__":
    main()
