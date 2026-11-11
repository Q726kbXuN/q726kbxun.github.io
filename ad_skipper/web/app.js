// The page: decode the file, hand it to worker.js, then play it back with the transcript and the
// ads marked.  window.adtag.analyze(file, {segments}) runs the same thing from a script.

const $ = (id) => document.getElementById(id);
const LABELS = { sponsor_ad: 'Sponsor', house_promo: 'Promo', membership_pitch: 'Membership', boilerplate: 'Boilerplate' };
const STAGES = ['measure', 'transcribe', 'embed', 'tag'];
const skip = new Set(['sponsor_ad']);
let result = null, segEls = [], lastSkipped = null, current = -1;

const hms = (t) => {
  t = Math.max(0, Math.round(t));
  const h = Math.floor(t / 3600), m = Math.floor(t / 60) % 60, s = String(t % 60).padStart(2, '0');
  return h ? `${h}:${String(m).padStart(2, '0')}:${s}` : `${m}:${s}`;
};

// ---- environment line --------------------------------------------------------------------------
$('env').textContent = [
  crossOriginIsolated ? `multi-threaded (${navigator.hardwareConcurrency} cores)` : 'single-threaded (page isn\'t cross-origin isolated)',
  navigator.gpu ? 'WebGPU available' : 'no WebGPU, so speech to text runs on the CPU and is slow',
].join(' · ');
if (!navigator.gpu) $('gpu').disabled = true;
navigator.gpu?.requestAdapter().then((a) => {
  if (!a || (a.info?.isFallbackAdapter ?? a.isFallbackAdapter)) {
    $('gpu').disabled = true;
    $('env').textContent = $('env').textContent.replace('WebGPU available', 'no usable GPU, so speech to text runs on the CPU and is slow');
  }
}).catch(() => {});

// ---- picking a file ----------------------------------------------------------------------------
$('file').addEventListener('change', (e) => e.target.files[0] && analyze(e.target.files[0]));
const drop = $('drop');
drop.addEventListener('dragover', (e) => { e.preventDefault(); drop.classList.add('over'); });
drop.addEventListener('dragleave', () => drop.classList.remove('over'));
drop.addEventListener('drop', (e) => {
  e.preventDefault(); drop.classList.remove('over');
  if (e.dataTransfer.files[0]) analyze(e.dataTransfer.files[0]);
});

/** The file's own sample rate, so the browser decodes without resampling (our resampler matches
 *  the one the model was trained with; browsers' don't).  MP3 frame header or MP4 mdhd. */
function nativeRate(b) {
  let i = 0;
  if (b[0] === 0x49 && b[1] === 0x44 && b[2] === 0x33) {        // ID3v2 tag
    i = 10 + ((b[6] & 0x7f) << 21 | (b[7] & 0x7f) << 14 | (b[8] & 0x7f) << 7 | (b[9] & 0x7f)) + (b[5] & 0x10 ? 10 : 0);
  }
  const RATES = { 3: [44100, 48000, 32000], 2: [22050, 24000, 16000], 0: [11025, 12000, 8000] };
  for (const end = Math.min(b.length - 4, i + 65536); i < end; i++) {
    if (b[i] !== 0xff || (b[i + 1] & 0xe0) !== 0xe0) continue;
    const ver = (b[i + 1] >> 3) & 3, layer = (b[i + 1] >> 1) & 3, br = b[i + 2] >> 4, sr = (b[i + 2] >> 2) & 3;
    if (ver !== 1 && layer && br && br !== 15 && sr !== 3) return RATES[ver][sr];
  }
  for (let j = 4; j < Math.min(b.length - 32, 4 << 20); j++) {   // MP4: first mdhd's timescale
    if (b[j] === 0x6d && b[j + 1] === 0x64 && b[j + 2] === 0x68 && b[j + 3] === 0x64) {
      const o = j + 4 + (b[j + 4] === 1 ? 20 : 12);
      const ts = (b[o] << 24 | b[o + 1] << 16 | b[o + 2] << 8 | b[o + 3]) >>> 0;
      if (ts >= 8000 && ts <= 96000) return ts;
      break;
    }
  }
  return 0;
}

let worker = null;
async function analyze(file, opts = {}) {
  $('pick').hidden = true; $('work').hidden = false; $('player').hidden = true;
  $('title').textContent = file.name || 'Episode';
  setStage('measure', 0, 'Decoding');
  $('audio').src = URL.createObjectURL(file);
  const bytes = await file.arrayBuffer();
  const rate = nativeRate(new Uint8Array(bytes)) || 44100;
  const ab = await new OfflineAudioContext(1, 1, rate).decodeAudioData(bytes);
  const n = ab.length;
  let mid, side = null;
  if (ab.numberOfChannels >= 2) {
    const l = ab.getChannelData(0), r = ab.getChannelData(1);
    mid = new Float32Array(n); side = new Float32Array(n);
    for (let i = 0; i < n; i++) { mid[i] = (l[i] + r[i]) * 0.5; side[i] = (l[i] - r[i]) * 0.5; }
  } else {
    mid = ab.getChannelData(0).slice();
  }
  worker?.terminate();
  worker = new Worker(new URL('worker.js', import.meta.url), { type: 'module' });
  const started = performance.now();
  return new Promise((resolve, reject) => {
    worker.onmessage = ({ data: m }) => {
      if (m.type === 'progress') setStage(m.stage, m.frac, m.text);
      else if (m.type === 'error') { $('status').textContent = `Something went wrong: ${m.message}`; console.error(m.stack); reject(new Error(m.message)); }
      else if (m.type === 'done') {
        m.result.timings.total = Math.round(performance.now() - started) / 1000;
        m.result.file = { name: file.name, sample_rate: rate, channels: ab.numberOfChannels };
        show(m.result); resolve(m.result);
      }
    };
    worker.onerror = (e) => { $('status').textContent = `Something went wrong: ${e.message}`; reject(e); };
    worker.postMessage({ mid, side, rate, segments: opts.segments || null, gpu: $('gpu').checked && !$('gpu').disabled },
                       side ? [mid.buffer, side.buffer] : [mid.buffer]);
  });
}

function setStage(stage, frac, text) {
  const k = STAGES.indexOf(stage);
  document.querySelectorAll('#stages li').forEach((li, i) => {
    li.className = i < k ? 'done' : i === k ? 'active' : '';
  });
  $('bar').style.width = `${Math.round(((k + Math.min(1, frac || 0)) / STAGES.length) * 100)}%`;
  if (text) $('status').textContent = text;
}

// ---- the player --------------------------------------------------------------------------------
function show(r) {
  result = r; window.adtagResult = r;
  $('work').hidden = true; $('player').hidden = false;
  const dur = r.duration;
  const tl = $('timeline');
  tl.querySelectorAll('.region, .cand').forEach((x) => x.remove());
  for (const s of r.spans) {
    const d = document.createElement('div');
    d.className = 'region';
    d.style.cssText = `left:${s.start / dur * 100}%;width:${(s.end - s.start) / dur * 100}%;background:var(--${s.type})`;
    d.title = `${LABELS[s.type]} ${hms(s.start)}-${hms(s.end)}`;
    tl.appendChild(d);
  }
  for (const c of r.candidates) {
    const d = document.createElement('div');
    d.className = 'cand';
    d.style.cssText = `left:${c.start / dur * 100}%;width:${(c.end - c.start) / dur * 100}%`;
    d.title = `Maybe an ad ${hms(c.start)}-${hms(c.end)}`;
    tl.appendChild(d);
  }
  $('legend').innerHTML = Object.entries(LABELS).map(([k, v]) =>
    `<span><i class="swatch" style="background:var(--${k})"></i>${v}</span>`).join('') +
    '<span><i class="swatch" style="border:2px dashed var(--sponsor_ad)"></i>Maybe</span>';
  $('skips').innerHTML = 'Skip: ' + Object.entries(LABELS).map(([k, v]) =>
    `<label><input type="checkbox" data-type="${k}" ${skip.has(k) ? 'checked' : ''}> ${v}</label>`).join('');
  $('skips').querySelectorAll('input').forEach((i) => i.addEventListener('change', () => {
    i.checked ? skip.add(i.dataset.type) : skip.delete(i.dataset.type);
  }));
  const ads = r.spans.filter((s) => s.type === 'sponsor_ad');
  const t = r.timings;
  $('summary').textContent = `${r.segments.length} sentences, ${ads.length} sponsor read${ads.length === 1 ? '' : 's'} ` +
    `(${hms(ads.reduce((s, a) => s + a.end - a.start, 0))}), ${r.candidates.length} maybe. ` +
    `Took ${hms(t.total)}: audio ${t.audio}s` + (t.transcribe ? `, speech to text ${t.transcribe}s` : '') +
    `, embeddings ${t.embed}s, tagging ${t.tag}s (${r.engine}).`;

  // transcript, with each span's segments tinted and labelled
  const spanOf = new Array(r.segments.length).fill(null), candOf = new Array(r.segments.length).fill(false);
  r.spans.forEach((s) => { for (let i = s.first_segment; i <= s.last_segment; i++) spanOf[i] = s; });
  r.segments.forEach((s, i) => { candOf[i] = r.candidates.some((c) => s.start >= c.start && s.start < c.end); });
  const box = $('transcript');
  box.textContent = '';
  segEls = r.segments.map((s, i) => {
    const el = document.createElement('div');
    el.className = 'seg' + (candOf[i] ? ' cand' : '');
    const sp = spanOf[i];
    if (sp) el.style.cssText = `border-left-color:var(--${sp.type});background:color-mix(in srgb, var(--${sp.type}) 14%, transparent)`;
    const time = document.createElement('time');
    time.textContent = hms(s.start);
    const text = document.createElement('div');
    if (sp && sp.first_segment === i) {
      const tag = document.createElement('span');
      tag.className = 'tag';
      tag.style.color = `var(--${sp.type})`;
      tag.textContent = `${LABELS[sp.type]} · ${hms(sp.end - sp.start)} · p ${sp.p.toFixed(2)}`;
      text.appendChild(tag);
    } else if (candOf[i] && !candOf[i - 1]) {
      const tag = document.createElement('span');
      tag.className = 'tag';
      tag.style.color = 'var(--sponsor_ad)';
      tag.textContent = 'Maybe an ad';
      text.appendChild(tag);
    }
    text.appendChild(document.createTextNode(s.text));
    el.append(time, text);
    el.addEventListener('click', () => { lastSkipped = spanOf[i]; seek(s.start); });
    box.appendChild(el);
    return el;
  });
}

function seek(t) { const a = $('audio'); a.currentTime = t; if (a.paused) a.play().catch(() => {}); }

$('timeline').addEventListener('click', (e) => {
  if (!result) return;
  const box = e.currentTarget.getBoundingClientRect();
  lastSkipped = null;
  seek((e.clientX - box.left) / box.width * result.duration);
});

$('audio').addEventListener('timeupdate', () => {
  if (!result) return;
  const t = $('audio').currentTime;
  $('playhead').style.left = `${t / result.duration * 100}%`;
  if (lastSkipped && t < lastSkipped.start - 1) lastSkipped = null;
  for (const s of result.spans) {
    if (skip.has(s.type) && t >= s.start && t < s.end - 0.3 && lastSkipped !== s) {
      lastSkipped = s;
      $('audio').currentTime = s.end;
      $('toast').textContent = `Skipped a ${LABELS[s.type].toLowerCase()} (${hms(s.end - s.start)}). Click it in the transcript to hear it.`;
      return;
    }
  }
  // highlight and follow the current sentence
  const segs = result.segments;
  let lo = 0, hi = segs.length - 1, at = -1;
  while (lo <= hi) { const m = (lo + hi) >> 1; if (segs[m].start <= t) { at = m; lo = m + 1; } else hi = m - 1; }
  if (at !== current) {
    segEls[current]?.classList.remove('now');
    current = at;
    if (at >= 0) { segEls[at].classList.add('now'); segEls[at].scrollIntoView({ block: 'nearest' }); }
  }
});

$('save').addEventListener('click', () => {
  const blob = new Blob([JSON.stringify(result, null, 1)], { type: 'application/json' });
  const a = Object.assign(document.createElement('a'), { href: URL.createObjectURL(blob),
    download: (result.file?.name || 'episode').replace(/\.[^.]+$/, '') + '.ads.json' });
  a.click();
});

window.adtag = { analyze };
