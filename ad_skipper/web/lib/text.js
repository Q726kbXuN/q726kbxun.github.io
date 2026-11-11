// Cut recognizer words into sentence-sized segments (port of transcribe.split_words).

const ABBREVIATIONS = new Set(['mr.', 'mrs.', 'ms.', 'dr.', 'st.', 'jr.', 'sr.', 'vs.', 'etc.', 'inc.', 'co.', 'no.',
  'u.s.', 'u.k.', 'e.g.', 'i.e.', 'a.m.', 'p.m.', 'mt.', 'ft.', 'approx.']);
const SENTENCE_END = /[.?!]["')\]]*$/;
const MAX_SEGMENT_S = 15;

const isSentenceEnd = (word) => {
  const w = word.trim().toLowerCase();
  return SENTENCE_END.test(w) && !ABBREVIATIONS.has(w);
};

/** words: [{start, end, text}] in time order -> [{start, end, text, n_words}] */
export function splitWords(words, maxLen = MAX_SEGMENT_S) {
  const out = [];
  let cur = [];
  const flush = (upto = cur.length) => {
    const take = cur.slice(0, upto);
    cur = cur.slice(upto);
    if (take.length) {
      out.push({ start: take[0].start, end: take[take.length - 1].end,
                 text: take.map((w) => w.text).join('').trim(), n_words: take.length });
    }
  };
  for (const w of words) {
    cur.push(w);
    if (isSentenceEnd(w.text)) flush();
    else if (cur[cur.length - 1].end - cur[0].start > maxLen && cur.length > 2) {
      let best = 0, at = 1;
      for (let i = 0; i + 1 < cur.length; i++) {
        const gap = cur[i + 1].start - cur[i].end;
        if (gap > best || i === 0) { best = gap; at = i + 1; }
      }
      flush(at);
    }
  }
  flush();
  for (const s of out) if (s.end <= s.start) s.end = s.start + 0.01;
  return out;
}
