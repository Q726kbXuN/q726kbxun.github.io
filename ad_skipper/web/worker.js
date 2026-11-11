// The whole pipeline, off the main thread: resample, measure, transcribe, embed, tag.
// The page decodes the audio (workers can't) and hands over mid/side at the file's own rate.
import { resample, analyzeFrames, segmentFeatures, SR } from './lib/audio.js';
import { splitWords } from './lib/text.js';
import { findSpans, findCandidates } from './lib/spans.js';
import { chunkAtPauses, transcribeBatched } from './lib/whisper.js';

const TRANSFORMERS = 'https://cdn.jsdelivr.net/npm/@huggingface/transformers@4.3.1/dist/transformers.min.js';
const WLLAMA = 'https://cdn.jsdelivr.net/npm/@wllama/wllama@3.8.1/esm/index.js';
const WLLAMA_WASM = 'https://cdn.jsdelivr.net/npm/@wllama/wllama@3.8.1/esm/wasm/wllama.wasm';
const ORT_DIST = 'https://cdn.jsdelivr.net/npm/onnxruntime-web@1.31.0-dev.20260914-8d85527a0/dist/';
const WHISPER = 'onnx-community/whisper-base.en_timestamped';
const MODEL_DIR = new URL('../model/', import.meta.url);
const THREADS = Math.max(1, Math.min(8, (self.navigator.hardwareConcurrency || 4) - 1));

const progress = (stage, frac, text) => self.postMessage({ type: 'progress', stage, frac, text });
const mb = (n) => `${Math.round(n / 1e6)} MB`;

self.onmessage = async (e) => {
  const { mid, side, rate, segments: given, gpu } = e.data;
  try {
    const timings = {};
    let t = performance.now();
    const lap = (k) => { const now = performance.now(); timings[k] = Math.round(now - t) / 1000; t = now; };
    const info = await (await fetch(new URL('ad_tagger.json', MODEL_DIR))).json();

    progress('measure', 0, 'Resampling');
    const m24 = resample(mid, rate, SR), s24 = side ? resample(side, rate, SR) : null;
    const frames = analyzeFrames(m24, s24, (f) => progress('measure', f, 'Measuring the audio'));
    lap('audio');

    // a real GPU only: a software (fallback) adapter would be far slower than the CPU path
    const adapter = gpu && self.navigator.gpu ? await self.navigator.gpu.requestAdapter().catch(() => null) : null;
    const useGpu = !!adapter && !(adapter.info?.isFallbackAdapter ?? adapter.isFallbackAdapter);
    let segments = given, engine = given ? 'transcript supplied' : '';
    if (!segments) {
      engine = useGpu ? 'whisper on WebGPU' : 'whisper on CPU (WebAssembly)';
      segments = await transcribe(resample(mid, rate, 16000), frames, useGpu);
      lap('transcribe');
    }
    if (!segments.length) throw new Error('No speech found in this file.');

    const { emb, gpu: embGpu } = await embed(segments.map((s) => s.text), info, useGpu);
    engine += `${engine ? ', ' : ''}embeddings on ${embGpu ? 'the GPU' : 'the CPU'}`;
    lap('embed');

    progress('tag', 0, 'Tagging');
    const feats = segmentFeatures(frames, segments);
    const probs = await tag(emb, feats, segments.length);
    const spans = findSpans(probs, segments);
    lap('tag');
    self.postMessage({ type: 'done', result: {
      duration: frames.n / 10, segments, spans, candidates: findCandidates(probs, spans, segments),
      labels: Array.from({ length: segments.length }, (_, i) => Array.from(probs.subarray(i * 5, i * 5 + 5)).map((p) => Math.round(p * 1000) / 1000)),
      timings, engine } });
  } catch (err) {
    self.postMessage({ type: 'error', message: err?.message || String(err), stack: err?.stack });
  }
};

async function transcribe(pcm, frames, useGpu) {
  const tf = await import(TRANSFORMERS);
  tf.env.allowLocalModels = false;
  if (tf.env.backends?.onnx?.wasm) tf.env.backends.onnx.wasm.numThreads = THREADS;
  const files = {};
  const progress_callback = (p) => {
    if (p.status !== 'progress' || !p.total) return;
    files[p.file] = p;
    const got = Object.values(files).reduce((s, f) => s + f.loaded, 0), all = Object.values(files).reduce((s, f) => s + f.total, 0);
    progress('transcribe', 0, `Loading the speech model (${mb(got)} of ${mb(all)}; only the first time downloads it)`);
  };
  // fp16 weights measured no faster than these on Apple and cost more memory
  const dtype = useGpu ? { encoder_model: 'fp32', decoder_model_merged: 'q4' }
                       : { encoder_model: 'q8', decoder_model_merged: 'q8' };
  const device = useGpu ? 'webgpu' : 'wasm';
  const [processor, tokenizer, model] = await Promise.all([
    tf.AutoProcessor.from_pretrained(WHISPER), tf.AutoTokenizer.from_pretrained(WHISPER),
    tf.WhisperForConditionalGeneration.from_pretrained(WHISPER, { device, dtype, progress_callback })]);
  // chunks cut at pauses (from the 100 ms loudness already measured), decoded several at a time
  const chunks = chunkAtPauses(frames.cols.rms_db, frames.n).map(([a, b]) => [a * 1600, Math.min(pcm.length, b * 1600)]);
  const words = await transcribeBatched({ tf, model, processor, tokenizer }, pcm, chunks, {
    // measured: 4 at a time on a GPU, 2 on the CPU (larger batches gained little and cost memory)
    batchSize: useGpu ? 4 : 2, onProgress: (f) => progress('transcribe', f, 'Transcribing') });
  await model.dispose?.();
  return splitWords(words);
}

async function embed(texts, info, useGpu = false) {
  const { Wllama } = await import(WLLAMA);
  const load = async (gpu) => {
    const w = new Wllama({ default: WLLAMA_WASM }, { suppressNativeLog: true });
    await w.loadModelFromUrl(info.embedder.url, {
      embeddings: true, pooling_type: 'last', n_ctx: 2048, n_batch: 2048, n_ubatch: 2048,
      n_threads: Math.max(1, (self.navigator.hardwareConcurrency || 4) - 1),
      n_gpu_layers: gpu ? 99 : 0,
      progressCallback: ({ loaded, total }) => progress('embed', 0, `Loading the text model (${mb(loaded)} of ${mb(total)}; only the first time downloads it)`),
    });
    return w;
  };
  // one text per call (wllama only answers for the first of a list), and the exact input the
  // model was trained on: trimmed, first 2000 characters, never empty
  const one = async (w, text) => {
    const v = (await w.createEmbedding({ input: (text || '').trim().slice(0, 2000) || ' ' })).data[0].embedding;
    const n = Math.sqrt(v.reduce((s, x) => s + x * x, 0)) || 1;
    return v.map((x) => x / n);
  };
  let gpu = useGpu, w = await load(gpu);
  if (gpu) {
    // GPUs differ in their arithmetic: make sure this one reproduces the reference vectors, else use the CPU
    const ref = await fetch(new URL('../testdata/embedding_reference.json', MODEL_DIR))
      .then((r) => (r.ok ? r.json() : null)).catch(() => null);
    let ok = false;
    if (ref) {
      const want = new Float32Array(Uint8Array.from(atob(ref.embeddings), (c) => c.charCodeAt(0)).buffer);
      ok = true;
      for (const i of [0, 1]) {
        const v = await one(w, ref.texts[i]);
        ok &&= v.reduce((s, x, k) => s + x * want[i * 1024 + k], 0) >= 0.999;
      }
    }
    if (!ok) { await w.exit(); gpu = false; w = await load(false); }
  }
  const D = 1024, out = new Float32Array(texts.length * D);
  for (let i = 0; i < texts.length; i++) {
    if (i % 16 === 0) progress('embed', i / texts.length, `Embedding the transcript${gpu ? ' (GPU)' : ''}`);
    out.set(await one(w, texts[i]), i * D);
  }
  await w.exit();
  return { emb: out, gpu };
}

async function tag(emb, feats, n) {
  const ort = await import(ORT_DIST + 'ort.wasm.min.mjs');
  ort.env.wasm.wasmPaths = ORT_DIST;
  ort.env.wasm.numThreads = THREADS;
  const session = await ort.InferenceSession.create(new URL('ad_tagger.onnx', MODEL_DIR).href);
  const out = await session.run({ emb: new ort.Tensor('float32', emb, [1, n, 1024]),
                                  feats: new ort.Tensor('float32', feats, [1, n, 28]) });
  await session.release();
  return out.probs.data;
}
