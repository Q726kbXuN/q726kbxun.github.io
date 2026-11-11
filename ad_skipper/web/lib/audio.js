// The audio side, in plain JS: resampling, the 100 ms frame measurements and the 28 per-segment
// features.  A port of python/podcast_ad_tagger/audio.py and features.py (the spec is
// docs/FEATURES.md); test/reference.test.mjs checks it against ../testdata.

export const SR = 24000;            // analysis rate
const HOP = 2400;                   // 100 ms frame
const SUB = 240;                    // 10 ms sub-frames
const NFFT = 2048;
const SHOP = 1200;                  // two spectra per frame
const DB_FLOOR = -200;
const Q_LO = 60, Q_HI = 401;        // cepstral pitch range, 60-400 Hz
const BAND_EDGES = [0, 60, 120, 250, 400, 630, 800, 1000, 1250, 1600,
                    2000, 2500, 3150, 4000, 5000, 6300, 8000, 10000, 12000];
const N_BANDS = BAND_EDGES.length - 1;
export const FRAME_NAMES = ['lk_db', 'rms_db', 'rms10max_db', 'rms10min_db', 'zcr', 'centroid_hz',
                            'flatness_db', 'flux_db', 'f0_hz', 'cpp_db', 'side_db'];
export const FEATURE_NAMES = [
  'lk_mean', 'lk_std', 'rms10max', 'rms10min', 'flat_mean', 'side_mean', 'zcr_mean',
  'cpp_mean', 'f0_std', 'centroid_mean', 'gap_before_s', 'gap_after_s', 'lk_delta_prev',
  'stage0_overlap', 'rel_pos', 'seg_dur', 'n_words', 'spk_change',
  'lk_vs_ep', 'lk_win_delta', 'lra_vs_ep', 'side_win_delta', 'flux_mean',
  'voiced_frac', 'band_dist', 'bass_delta', 'edge_before', 'edge_after',
];

const db10 = (p) => (p > 0 ? Math.max(10 * Math.log10(p), DB_FLOOR) : DB_FLOOR);
const db20 = (a) => db10(a * a);
const signbit = (x) => x < 0 || (x === 0 && 1 / x < 0);

// ---------------------------------------------------------------------------------------------
// Resampling, the way ffmpeg's default resampler does it (that's what made the training audio):
// a Kaiser-windowed sinc (beta 9) cut off at 0.97 of the lower Nyquist, an exact polyphase bank,
// and mirror padding at both ends.

export function resample(x, inRate, outRate) {
  if (inRate === outRate) return Float32Array.from(x);
  const factor = Math.min(outRate * 0.97 / inRate, 1);
  let len = Math.max(Math.ceil(32 / factor), 1);
  if (len > 1) len += len % 2;
  const center = (len - 1) >> 1;
  let a = outRate, b = inRate;
  while (b) [a, b] = [b, a % b];
  const phases = outRate / a, step = inRate / a;
  if (phases > 1024) throw new Error(`unsupported rate pair ${inRate} -> ${outRate}`);
  const i0 = (v) => {
    let s = 1, t = 1;
    for (let k = 1; ; k++) { t *= (v / (2 * k)) ** 2; s += t; if (t < 1e-17 * s) return s; }
  };
  const phNb = phases % 2 ? phases : phases / 2 + 1;
  const tabs = [];
  let norm = 0;
  for (let ph = 0; ph < phNb; ph++) {
    const tab = new Float64Array(len);
    for (let i = 0; i < len; i++) {
      const xv = Math.PI * ((i - center) - ph / phases) * factor;
      let y = xv === 0 ? 1 : Math.sin(xv) / xv;
      const w = 2 * xv / (factor * len * Math.PI);
      y *= i0(9 * Math.sqrt(Math.max(1 - w * w, 0)));
      tab[i] = y;
      if (ph === 0) norm += y;
    }
    tabs.push(tab);
  }
  const bank = new Array(phases);
  for (let ph = 0; ph < phNb; ph++) {
    bank[ph] = Float32Array.from(tabs[ph], (v) => v / norm);
    if (phases % 2 === 0 && ph > 0) bank[phases - ph] = Float32Array.from(bank[ph]).reverse();
  }
  // padded input: the opening mirrored in front, the tail mirrored after
  const n = x.length;
  const r = (Math.min(n, len) + 1) >> 1;
  const p = new Float32Array(len + n + r);
  for (let k = 1; k <= len; k++) if (k < n) p[len - k] = x[k];
  p.set(x, len);
  for (let k = 0; k < r; k++) p[len + n + k] = x[n - 1 - k];
  const out = [];
  const chunk = new Float32Array(1 << 16);
  let used = 0;
  for (let i = 0; ; i++) {
    const s = (len - center) + Math.floor(i * step / phases);
    if (s + len > p.length) break;
    const h = bank[(i * step) % phases];
    let acc = 0;
    for (let j = 0; j < len; j++) acc += p[s + j] * h[j];
    chunk[used++] = acc;
    if (used === chunk.length) { out.push(chunk.slice()); used = 0; }
  }
  out.push(chunk.slice(0, used));
  const res = new Float32Array(out.reduce((t, c) => t + c.length, 0));
  let o = 0;
  for (const c of out) { res.set(c, o); o += c.length; }
  return res;
}

// ---------------------------------------------------------------------------------------------
// FFT (radix 2, complex, in place)

class FFT {
  constructor(n) {
    this.n = n;
    this.rev = new Uint32Array(n);
    const bits = Math.log2(n);
    for (let i = 0; i < n; i++) {
      let r = 0;
      for (let b = 0; b < bits; b++) r |= ((i >> b) & 1) << (bits - 1 - b);
      this.rev[i] = r;
    }
    this.cos = new Float64Array(n / 2);
    this.sin = new Float64Array(n / 2);
    for (let i = 0; i < n / 2; i++) { this.cos[i] = Math.cos(2 * Math.PI * i / n); this.sin[i] = Math.sin(2 * Math.PI * i / n); }
  }

  transform(re, im, inverse = false) {
    const n = this.n, rev = this.rev;
    for (let i = 0; i < n; i++) {
      const j = rev[i];
      if (j > i) { let t = re[i]; re[i] = re[j]; re[j] = t; t = im[i]; im[i] = im[j]; im[j] = t; }
    }
    const sgn = inverse ? 1 : -1;
    for (let size = 2; size <= n; size <<= 1) {
      const half = size >> 1, stride = n / size;
      for (let start = 0; start < n; start += size) {
        for (let k = 0; k < half; k++) {
          const wr = this.cos[k * stride], wi = sgn * this.sin[k * stride];
          const a = start + k, b = a + half;
          const tr = re[b] * wr - im[b] * wi, ti = re[b] * wi + im[b] * wr;
          re[b] = re[a] - tr; im[b] = im[a] - ti;
          re[a] += tr; im[a] += ti;
        }
      }
    }
  }
}

function kweighting(sr) {
  const f0 = 1681.974450955533, gainDb = 3.999843853973347, q = 0.7071752369554196;
  const k = Math.tan(Math.PI * f0 / sr);
  const vh = 10 ** (gainDb / 20), vb = vh ** 0.4996667741545416;
  const a0 = 1 + k / q + k * k;
  const shelf = [(vh + vb * k / q + k * k) / a0, 2 * (k * k - vh) / a0, (vh - vb * k / q + k * k) / a0,
                 2 * (k * k - 1) / a0, (1 - k / q + k * k) / a0];
  const f0b = 38.13547087602444, qb = 0.5003270373238773;
  const kb = Math.tan(Math.PI * f0b / sr);
  const a0b = 1 + kb / qb + kb * kb;
  const hpf = [1, -2, 1, 2 * (kb * kb - 1) / a0b, (1 - kb / qb + kb * kb) / a0b];
  return [shelf, hpf];      // [b0, b1, b2, a1, a2], a0 normalised to 1
}

// ---------------------------------------------------------------------------------------------
// 100 ms frames

/** mid (and side, for stereo) at 24 kHz -> { n, stereo, cols: {name: Float32Array}, bands: Float32Array(n * 18) } */
export function analyzeFrames(mid, side = null, onProgress = null) {
  const len = mid.length;
  const n = Math.floor(len / HOP) + 1;
  const total = n * HOP;
  const cols = Object.fromEntries(FRAME_NAMES.map((k) => [k, new Float32Array(n)]));
  const bands = new Float32Array(n * N_BANDS);
  const at = (i) => (i >= 0 && i < len ? mid[i] : 0);

  // K-weighted loudness: one continuous filter over the whole (zero-padded) signal
  const [s1, s2] = kweighting(SR);
  let z10 = 0, z11 = 0, z20 = 0, z21 = 0, acc = 0;
  for (let i = 0; i < total; i++) {
    const x = at(i);
    const y1 = s1[0] * x + z10; z10 = s1[1] * x - s1[3] * y1 + z11; z11 = s1[2] * x - s1[4] * y1;
    const y2 = s2[0] * y1 + z20; z20 = s2[1] * y1 - s2[3] * y2 + z21; z21 = s2[2] * y1 - s2[4] * y2;
    acc += y2 * y2;
    if ((i + 1) % HOP === 0) { cols.lk_db[(i + 1) / HOP - 1] = db10(acc / HOP); acc = 0; }
  }

  for (let f = 0; f < n; f++) {
    const base = f * HOP;
    let ms = 0, subMax = -Infinity, subMin = Infinity, zc = 0, sideMs = 0;
    for (let s = 0; s < HOP / SUB; s++) {
      let sm = 0;
      for (let i = 0; i < SUB; i++) { const v = at(base + s * SUB + i); sm += v * v; }
      ms += sm; sm /= SUB;
      if (sm > subMax) subMax = sm;
      if (sm < subMin) subMin = sm;
    }
    for (let i = 1; i < HOP; i++) if (signbit(at(base + i)) !== signbit(at(base + i - 1))) zc++;
    if (side) for (let i = 0; i < HOP; i++) { const v = base + i < len ? side[base + i] : 0; sideMs += v * v; }
    cols.rms_db[f] = db10(ms / HOP);
    cols.rms10max_db[f] = db10(subMax);
    cols.rms10min_db[f] = db10(subMin);
    cols.zcr[f] = zc / (HOP - 1);
    cols.side_db[f] = side ? db10(sideMs / HOP) : DB_FLOOR;
  }

  // spectra
  const fft = new FFT(NFFT);
  const nb = NFFT / 2 + 1;
  const win = new Float64Array(NFFT);
  let w2 = 0;
  for (let i = 0; i < NFFT; i++) { win[i] = 0.5 - 0.5 * Math.cos(2 * Math.PI * i / (NFFT - 1)); w2 += win[i] * win[i]; }
  const scale = 1 / (NFFT * NFFT * (w2 / NFFT));
  const binNorm = new Float64Array(nb).fill(2 * scale);
  binNorm[0] = scale; binNorm[nb - 1] = scale;
  const freqs = Float64Array.from({ length: nb }, (_, b) => b * SR / NFFT);
  const bandOf = Int8Array.from(freqs, (fq) => BAND_EDGES.findIndex((lo, i) => i < N_BANDS && fq >= lo && fq < BAND_EDGES[i + 1]));
  const re = new Float64Array(NFFT), im = new Float64Array(NFFT);
  const power = new Float64Array(nb), mag = new Float64Array(nb);
  let prevMag = null;
  const win341 = new Float64Array(Q_HI - Q_LO);
  for (let f = 0; f < n; f++) {
    let cen = 0, flat = 0, flux = 0, bestCpp = -Infinity, bestF0 = 0;
    const bsum = new Float64Array(N_BANDS);
    for (let k = 0; k < 2; k++) {
      const start = f * HOP + k * SHOP + SHOP / 2 - NFFT / 2;
      for (let i = 0; i < NFFT; i++) { re[i] = at(start + i) * win[i]; im[i] = 0; }
      fft.transform(re, im);
      let tot = 0, wsum = 0, lsum = 0;
      for (let b = 0; b < nb; b++) {
        const p = (re[b] * re[b] + im[b] * im[b]) * binNorm[b] + 1e-20;
        power[b] = p; tot += p; wsum += p * freqs[b]; lsum += Math.log(p);
        if (bandOf[b] >= 0) bsum[bandOf[b]] += p / 2;
      }
      cen += wsum / tot / 2;
      flat += Math.exp(lsum / nb) / (tot / nb) / 2;
      let fl = 0;
      for (let b = 0; b < nb; b++) {
        mag[b] = Math.sqrt(power[b]);
        if (prevMag) fl += Math.max(mag[b] - prevMag[b], 0);
      }
      flux += fl / 2;
      prevMag = Float64Array.from(mag);
      // cepstrum: inverse real FFT of the log power spectrum
      for (let b = 0; b < nb; b++) { re[b] = Math.log(power[b]); im[b] = 0; }
      for (let b = nb; b < NFFT; b++) { re[b] = re[NFFT - b]; im[b] = 0; }
      fft.transform(re, im, true);
      let q = Q_LO;
      for (let i = Q_LO; i < Q_HI; i++) { win341[i - Q_LO] = re[i] / NFFT; if (re[i] > re[q]) q = i; }
      const cpp = (re[q] / NFFT - median(win341)) * (10 / Math.LN10);
      if (cpp > bestCpp) { bestCpp = cpp; bestF0 = SR / q; }
    }
    cols.centroid_hz[f] = cen;
    cols.flatness_db[f] = db10(flat);
    cols.flux_db[f] = db20(flux);
    cols.f0_hz[f] = bestF0;
    cols.cpp_db[f] = bestCpp;
    for (let i = 0; i < N_BANDS; i++) bands[f * N_BANDS + i] = db10(bsum[i]);
    if (onProgress && f % 2000 === 0) onProgress(f / n);
  }
  return { n, stereo: !!side, cols, bands };
}

// ---------------------------------------------------------------------------------------------
// numpy-compatible statistics

function median(a) {
  const s = Float64Array.from(a).sort();
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

function percentile(a, q) {            // numpy's default, linear interpolation
  const s = Float64Array.from(a).sort();
  const pos = (q / 100) * (s.length - 1), lo = Math.floor(pos), hi = Math.min(lo + 1, s.length - 1);
  return s[lo] + (s[hi] - s[lo]) * (pos - lo);
}

const mean = (a) => a.reduce((t, v) => t + v, 0) / a.length;
const std = (a) => { const m = mean(a); return Math.sqrt(a.reduce((t, v) => t + (v - m) ** 2, 0) / a.length); };
const valid = (a) => a.filter((v) => v > -190);

// ---------------------------------------------------------------------------------------------
// The 28 per-segment features

/** frames: from analyzeFrames (or null for text only).  segments: [{start, end, n_words?, speaker?, text?}].
 *  Returns Float32Array(segments.length * 28), row-major. */
export function segmentFeatures(frames, segments) {
  const ns = segments.length, F = new Float32Array(ns * 28);
  if (!ns) return F;
  const have = frames && frames.n > 0;
  let c, nfr, lkMed, lra, act, sideC, sideMed, prof, profMed, bass, bassMed;
  if (have) {
    c = frames.cols; nfr = frames.n;
    const lk = valid(Array.from(c.lk_db));
    lkMed = lk.length ? median(lk) : -70;
    lra = lk.length > 10 ? percentile(lk, 95) - percentile(lk, 10) : 0;
    act = Array.from(c.rms_db, (v) => v > -80);
    const nAct = act.filter(Boolean).length;
    sideC = Float64Array.from(c.side_db, (v) => Math.max(v, -120));
    sideMed = frames.stereo && nAct > 10 ? median(sideC.filter((_, i) => act[i])) : null;
    prof = new Float64Array(nfr * N_BANDS);
    bass = new Float64Array(nfr);
    for (let f = 0; f < nfr; f++) {
      let m = 0, lin = 0, low = 0;
      for (let b = 0; b < N_BANDS; b++) m += Math.max(frames.bands[f * N_BANDS + b], -120);
      m /= N_BANDS;
      for (let b = 0; b < N_BANDS; b++) {
        const v = frames.bands[f * N_BANDS + b];
        prof[f * N_BANDS + b] = Math.max(v, -120) - m;
        const p = 10 ** (Math.min(Math.max(v, -120), 0) / 10);
        lin += p; if (b < 4) low += p;
      }
      bass[f] = low / (lin + 1e-12);
    }
    const actIdx = act.flatMap((a, i) => (a ? [i] : []));
    profMed = new Float64Array(N_BANDS);
    if (nAct > 10) for (let b = 0; b < N_BANDS; b++) profMed[b] = median(actIdx.map((i) => prof[i * N_BANDS + b]));
    bassMed = nAct > 10 ? median(actIdx.map((i) => bass[i])) : 0;
  }
  const edge = (i) => {
    const pre = valid(Array.from(c.lk_db.subarray(Math.max(0, i - 10), i)));
    const post = valid(Array.from(c.lk_db.subarray(i, i + 10)));
    return pre.length >= 3 && post.length >= 3 ? Math.abs(mean(post) - mean(pre)) : 0;
  };
  const dur = +segments[ns - 1].end;
  let prevLk = null;
  for (let s = 0; s < ns; s++) {
    const seg = segments[s], a = +seg.start, e = +seg.end;
    const row = new Float64Array(28);
    if (have) {
      const f0i = Math.max(0, Math.trunc(a * 10)), f1i = Math.max(Math.trunc(a * 10) + 1, Math.trunc(e * 10));
      const lo = Math.min(f0i, nfr), hi = Math.min(f1i, nfr);
      const sl = (arr) => Array.from(arr.subarray(lo, Math.max(lo, hi)));
      const lk = valid(sl(c.lk_db));
      row[0] = lk.length ? mean(lk) : -100;
      row[1] = lk.length ? std(lk) : 0;
      row[2] = hi > lo ? Math.max(...sl(c.rms10max_db)) : -100;
      row[3] = hi > lo ? Math.min(...sl(c.rms10min_db)) : -100;
      [['flatness_db', 4], ['side_db', 5], ['zcr', 6], ['cpp_db', 7], ['centroid_hz', 9]].forEach(([k, i]) => {
        const v = valid(sl(c[k])); row[i] = v.length ? mean(v) : 0;
      });
      const cpp = sl(c.cpp_db), f0v = sl(c.f0_hz).filter((_, i) => cpp[i] > 1);
      row[8] = f0v.length > 3 ? std(f0v) : 0;
      row[10] = Array.from(c.rms_db.subarray(Math.max(0, f0i - 30), f0i)).filter((v) => v < -60).length / 10;
      row[11] = Array.from(c.rms_db.subarray(hi, hi + 30)).filter((v) => v < -60).length / 10;
      row[12] = prevLk !== null ? row[0] - prevLk : 0;
      prevLk = row[0];
      if (lk.length) row[18] = row[0] - lkMed;
      const c0 = (f0i + Math.min(f1i, nfr)) >> 1;
      const w0 = Math.max(0, c0 - 50), w1 = Math.min(nfr, c0 + 50);
      const wlk = valid(Array.from(c.lk_db.subarray(w0, Math.max(w0, w1))));
      if (wlk.length > 10) {
        row[19] = median(wlk) - lkMed;
        row[20] = percentile(wlk, 95) - percentile(wlk, 10) - lra;
      }
      if (sideMed !== null && w1 > w0) row[21] = median(sideC.subarray(w0, w1)) - sideMed;
      const fx = valid(sl(c.flux_db));
      row[22] = fx.length ? mean(fx) : -90;
      if (hi > lo) row[23] = cpp.filter((v) => v > 1).length / cpp.length;
      const segAct = [];
      for (let f = lo; f < hi; f++) if (act[f]) segAct.push(f);
      if (segAct.length >= 3) {
        let d = 0;
        for (let b = 0; b < N_BANDS; b++) d += Math.abs(median(segAct.map((f) => prof[f * N_BANDS + b])) - profMed[b]);
        row[24] = d / N_BANDS;
        row[25] = median(segAct.map((f) => bass[f])) - bassMed;
      }
      row[26] = edge(f0i);
      row[27] = edge(hi);
    }
    // row[13], stage0_overlap (repeated-audio history), stays 0 as it does in production
    row[14] = a / Math.max(dur, 1);
    row[15] = e - a;
    row[16] = seg.n_words ?? String(seg.text ?? '').split(/\s+/).filter(Boolean).length;
    const spk = seg.speaker ?? null, prev = s ? segments[s - 1].speaker ?? null : spk;
    row[17] = spk !== null && prev !== null && spk !== prev ? 1 : 0;
    F.set(row, s * 28);
  }
  return F;
}
