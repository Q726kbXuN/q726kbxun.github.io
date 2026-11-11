// Batched Whisper with word timestamps, the way WhisperX speeds things up: cut the audio at pauses
// into chunks of up to 30 s (no overlap, so nothing is transcribed twice), decode several chunks at
// once, and time each word from the decoder's cross-attention (dynamic time warping, as in Whisper
// itself).  transformers.js's own pipeline does one overlapping chunk at a time.

const SR = 16000;
const EOT = 50256;              // <|endoftext|>
const NO_TIMESTAMPS = 50362;    // <|notimestamps|>, the last of the prompt tokens for the English-only models
const PROMPT = [50257, NO_TIMESTAMPS];   // <|startoftranscript|><|notimestamps|> (English-only models)

/** 100 ms loudness (dB) -> chunks [startFrame, endFrame) of at most maxS seconds, each cut at the
 *  quietest moment between minS and maxS into the chunk. */
export function chunkAtPauses(rmsDb, nFrames, minS = 20, maxS = 29.5) {
  const out = [];
  let start = 0;
  const lo = Math.round(minS * 10), hi = Math.round(maxS * 10);
  while (nFrames - start > hi) {
    let best = start + hi, bestV = Infinity;
    for (let f = start + lo; f <= start + hi; f++) {
      const v = Math.max(rmsDb[f - 1], rmsDb[f], rmsDb[f + 1] ?? rmsDb[f]);   // quiet for ~300 ms
      if (v < bestV) { bestV = v; best = f; }
    }
    out.push([start, best]);
    start = best;
  }
  if (nFrames > start) out.push([start, nFrames]);
  return out;
}

function medianFilter(row, width) {                  // along time, reflect padding (as Whisper)
  const half = width >> 1, n = row.length, out = new Float32Array(n), win = new Float32Array(width);
  for (let i = 0; i < n; i++) {
    for (let k = -half; k <= half; k++) {
      let j = i + k;
      if (j < 0) j = -j; else if (j >= n) j = 2 * n - j - 2;
      win[k + half] = row[Math.min(Math.max(j, 0), n - 1)];
    }
    win.sort();
    out[i] = win[half];
  }
  return out;
}

function dtw(cost, T, F) {                           // cost is row-major [T, F]; Whisper's backtrace rules
  const C = new Float64Array((T + 1) * (F + 1)).fill(Infinity), tr = new Int8Array((T + 1) * (F + 1)).fill(-1);
  const W = F + 1;
  C[0] = 0;
  for (let j = 1; j <= F; j++) {
    for (let i = 1; i <= T; i++) {
      const c0 = C[(i - 1) * W + j - 1], c1 = C[(i - 1) * W + j], c2 = C[i * W + j - 1];
      let c, t;
      if (c0 < c1 && c0 < c2) { c = c0; t = 0; } else if (c1 < c0 && c1 < c2) { c = c1; t = 1; } else { c = c2; t = 2; }
      C[i * W + j] = cost[(i - 1) * F + j - 1] + c;
      tr[i * W + j] = t;
    }
  }
  for (let j = 0; j <= F; j++) tr[j] = 2;
  for (let i = 0; i <= T; i++) tr[i * W] = 1;
  const text = [], time = [];
  let i = T, j = F;
  while (i > 0 || j > 0) {
    text.push(i - 1); time.push(j - 1);
    const t = tr[i * W + j];
    if (t === 0) { i--; j--; } else if (t === 1) i--; else j--;
  }
  return [text.reverse(), time.reverse()];
}

/** Token start times for one batch item, from the alignment heads' cross-attention.
 *  att: per layer, Float32Array views [B, H, T, 1500]; returns seconds per token, length T. */
function tokenTimes(att, dims, b, heads, nTok, nFrames, nPrompt, medianWidth) {
  const [, H, Tall, P] = dims;
  const F = Math.min(nFrames, P), T = nTok;
  const avg = new Float32Array(T * F);
  for (const [l, h] of heads) {
    const base = ((b * H + h) * Tall) * P, a = att[l];
    const w = new Float32Array(T * F);
    for (let t = 0; t < T; t++) for (let f = 0; f < F; f++) w[t * F + f] = a[base + t * P + f];
    for (let f = 0; f < F; f++) {                       // normalise each frame over the tokens
      let m = 0, s = 0;
      for (let t = 0; t < T; t++) m += w[t * F + f];
      m /= T;
      for (let t = 0; t < T; t++) s += (w[t * F + f] - m) ** 2;
      s = Math.sqrt(s / T) || 1;
      for (let t = 0; t < T; t++) w[t * F + f] = (w[t * F + f] - m) / s;
    }
    for (let t = 0; t < T; t++) {
      const row = medianFilter(w.subarray(t * F, (t + 1) * F), medianWidth);
      for (let f = 0; f < F; f++) avg[t * F + f] += row[f] / heads.length;
    }
  }
  const Tt = T - nPrompt, cost = new Float32Array(Tt * F);
  for (let i = 0; i < Tt * F; i++) cost[i] = -avg[nPrompt * F + i];
  const [text, time] = dtw(cost, Tt, F);
  const out = new Array(T).fill(0);
  let k = nPrompt;
  for (let i = 0; i < text.length; i++) if (i === 0 || text[i] !== text[i - 1]) out[k++] = time[i] * 0.02;
  return out;
}

/** pcm: 16 kHz mono Float32Array.  chunks: [[startSample, endSample], ...].  tf: the transformers.js
 *  module; model/processor/tokenizer loaded from a *_timestamped Whisper export.
 *  Returns words [{start, end, text}] in seconds. */
export async function transcribeBatched({ tf, model, processor, tokenizer }, pcm, chunks, { batchSize = 8, maxNewTokens = 224, onProgress } = {}) {
  const heads = model.generation_config.alignment_heads;
  const medianWidth = model.config.median_filter_width ?? 7;
  const words = [];
  const decodeTok = new Map();
  const piece = (id) => { if (!decodeTok.has(id)) decodeTok.set(id, tokenizer.decode([id])); return decodeTok.get(id); };
  for (let i = 0; i < chunks.length; i += batchSize) {
    onProgress?.(i / chunks.length);
    const batch = chunks.slice(i, i + batchSize);
    const feats = [];
    for (const [a, b] of batch) feats.push((await processor(pcm.subarray(a, b))).input_features);
    let attention = null;
    // the prompt, one row per chunk; Whisper's own begin-suppression would count the rows as the prompt
    // length, so it's switched off and added back with the right index
    const logits = new tf.LogitsProcessorList();
    logits.push(new tf.SuppressTokensAtBeginLogitsProcessor(model.generation_config.begin_suppress_tokens ?? [220, EOT], PROMPT.length));
    const out = await model.generate({
      inputs: tf.cat(feats, 0), decoder_input_ids: batch.map(() => PROMPT), max_new_tokens: maxNewTokens,
      return_timestamps: false, begin_suppress_tokens: null, logits_processor: logits,
      output_attentions: true, return_dict_in_generate: true,
    });
    // cross_attentions: one entry per generation step, each a list over layers of [B, H, s, 1500]
    const steps = out.cross_attentions;
    const layers = steps[0].length;
    const cat = Array.from({ length: layers }, (_, l) => tf.cat(steps.map((s) => s[l]), 2));
    attention = cat.map((t) => t.data);
    const dims = cat[0].dims;
    const seqs = out.sequences.tolist();
    batch.forEach(([a, b], k) => {
      const seq = seqs[k].map(Number);
      const nPrompt = seq.indexOf(NO_TIMESTAMPS) + 1;
      let end = seq.indexOf(EOT, nPrompt);
      if (end < 0) end = seq.length;
      // the attention rows are for the tokens fed to the decoder, i.e. all but the last one
      const nTok = Math.min(end + 1, dims[2]);
      if (end <= nPrompt) return;
      const times = tokenTimes(attention, dims, k, heads, nTok, Math.ceil((b - a) / 320), nPrompt, medianWidth);
      const off = a / SR, tEnd = (b - a) / SR;
      let cur = null;
      for (let t = nPrompt; t < end; t++) {
        const txt = piece(seq[t]);
        const start = off + times[t], stop = off + Math.min(t + 1 < nTok ? times[t + 1] : tEnd, tEnd);
        if (!cur || txt.startsWith(' ')) {
          if (cur) words.push(cur);
          cur = { start, end: stop, ids: [seq[t]] };
        } else {
          cur.ids.push(seq[t]);
          // trailing punctuation marks where the word stopped; otherwise run to the next token
          cur.end = /[A-Za-z0-9]/.test(txt) ? stop : Math.max(cur.start, start);
        }
      }
      if (cur) words.push(cur);
    });
    for (const w of words) if (w.ids) { w.text = tokenizer.decode(w.ids); delete w.ids; }
  }
  return words.filter((w) => w.text.trim());
}
