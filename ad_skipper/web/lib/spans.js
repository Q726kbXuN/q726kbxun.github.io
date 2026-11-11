// Per-segment probabilities -> ad spans and "maybe an ad" candidates (port of tagger.py).

export const CLASSES = ['content', 'sponsor_ad', 'house_promo', 'membership_pitch', 'boilerplate'];
const MIN_SEGMENTS = 2, MAX_GAP = 2, CAND_MIN_P = 0.25, CAND_MIN_SEGMENTS = 2;
const round = (v, d) => Math.round(v * 10 ** d) / 10 ** d;

function argmaxRows(probs, n) {
  const out = new Int8Array(n);
  for (let i = 0; i < n; i++) {
    let best = 0;
    for (let c = 1; c < 5; c++) if (probs[i * 5 + c] > probs[i * 5 + best]) best = c;
    out[i] = best;
  }
  return out;
}

/** probs: Float32Array(n * 5) row-major.  -> [{start, end, type, p, first_segment, last_segment}] */
export function findSpans(probs, segments) {
  const n = segments.length, labels = argmaxRows(probs, n), spans = [];
  let i = 0;
  while (i < n) {
    const c = labels[i];
    if (c === 0) { i++; continue; }
    let j = i;
    while (j + 1 < n && (labels[j + 1] === c ||
           (j + 1 + MAX_GAP < n && labels[j + 1] === 0 &&
            [...Array(MAX_GAP).keys()].some((k) => labels[j + 2 + k] === c)))) j++;
    if (j - i + 1 >= MIN_SEGMENTS) {
      let p = 0;
      for (let k = i; k <= j; k++) p += probs[k * 5 + c];
      spans.push({ start: round(+segments[i].start, 2), end: round(+segments[j].end, 2), type: CLASSES[c],
                   p: round(p / (j - i + 1), 3), first_segment: i, last_segment: j });
    }
    i = j + 1;
  }
  return spans;
}

/** Stretches outside every span where the sponsor probability stays up: the near misses. */
export function findCandidates(probs, spans, segments) {
  const n = segments.length, inside = new Uint8Array(n);
  for (const sp of spans) inside.fill(1, sp.first_segment, sp.last_segment + 1);
  const hot = Array.from({ length: n }, (_, i) => !inside[i] && probs[i * 5 + 1] >= CAND_MIN_P);
  const out = [];
  let i = 0;
  while (i < n) {
    if (!hot[i]) { i++; continue; }
    let j = i;
    while (j + 1 < n && (hot[j + 1] || (j + 2 < n && hot[j + 2] && !inside[j + 1]))) j++;
    if (j - i + 1 >= CAND_MIN_SEGMENTS) {
      let p = 0;
      for (let k = i; k <= j; k++) p += probs[k * 5 + 1];
      out.push({ start: round(+segments[i].start, 2), end: round(+segments[j].end, 2), p: round(p / (j - i + 1), 3) });
    }
    i = j + 1;
  }
  return out;
}
