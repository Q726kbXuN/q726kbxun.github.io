"use strict";

// In-browser port of clue_model.py, solving clues against the static bundle
// written by ./export_static.py.  The math mirrors the Python module step for
// step (including the float32 accumulation in the similarity stage) so both
// versions produce the same rankings; ./verify_static.py checks that.
//
// The host supplies loader(name) -> Promise<Uint8Array> returning one
// decompressed bundle object.  Everything here is plain ES2020, usable from
// a browser <script> tag (window.ClueEngine) or node (module.exports).

(function (root) {

var BLANK_RE = /_+|-{2,}|\.{3,}|\u2014|\u2013|\u2026/g;
var WORD_RE = /[a-z0-9]+/g;
var LAYOUT_RE = /^[A-Za-z.?_]+$/;
var FEATURES = ["exact_share", "exact_log", "similar", "best_sim", "seen_log"];
var MAX_DF_SHARE = 0.2;
var MIN_SIM = 0.05;

var FNV_OFFSET = 0xcbf29ce484222325n;
var FNV_PRIME = 0x100000001b3n;
var FNV_MASK = 0xffffffffffffffffn;

// -- Text handling, identical to clue_model.py ------------------------------

function normalize(text) {
  var cleaned = text.toLowerCase().replace(BLANK_RE, " zblankz ");
  return cleaned.match(WORD_RE) || [];
}

function termsOf(tokens) {
  var ret = tokens.slice();
  for (var i = 0; i + 1 < tokens.length; i++) {
    ret.push(tokens[i] + " " + tokens[i + 1]);
  }
  return ret;
}

function parsePattern(layout) {
  layout = (layout || "").replace(/ /g, "");
  if (!layout) return null;
  if (!LAYOUT_RE.test(layout)) {
    throw new Error("Layouts use letters for known cells and . ? _ for unknown ones");
  }
  var src = "";
  for (var i = 0; i < layout.length; i++) {
    var ch = layout.charAt(i);
    src += (ch === "." || ch === "?" || ch === "_") ? "[A-Z]" : ch.toUpperCase();
  }
  return { length: layout.length, regex: new RegExp("^" + src + "$") };
}

function fnv1a64(text) {
  var bytes = new TextEncoder().encode(text);
  var h = FNV_OFFSET;
  for (var i = 0; i < bytes.length; i++) {
    h = ((h ^ BigInt(bytes[i])) * FNV_PRIME) & FNV_MASK;
  }
  return h;
}

// -- Bundle plumbing --------------------------------------------------------

// Typed array views need alignment, so slice out a fresh (aligned) buffer
function sliceBuf(bytes, off, len) {
  return bytes.buffer.slice(bytes.byteOffset + off, bytes.byteOffset + off + len);
}
function asU32(bytes, off, count) { return new Uint32Array(sliceBuf(bytes, off, count * 4)); }
function asF32(bytes, off, count) { return new Float32Array(sliceBuf(bytes, off, count * 4)); }
function asU64(bytes, off, count) { return new BigUint64Array(sliceBuf(bytes, off, count * 8)); }

// A bundle container is: uint32 header length, JSON header, raw payload
function parseContainer(bytes) {
  var hlen = new DataView(bytes.buffer, bytes.byteOffset, 4).getUint32(0, true);
  var header = JSON.parse(new TextDecoder().decode(bytes.subarray(4, 4 + hlen)));
  return { header: header, payload: bytes.subarray(4 + hlen) };
}

function pad4(n) {
  var s = String(n);
  while (s.length < 4) s = "0" + s;
  return s;
}

function Lru(cap) {
  this.cap = cap;
  this.map = new Map();
}
Lru.prototype.get = function (key) {
  if (!this.map.has(key)) return undefined;
  var val = this.map.get(key);
  this.map.delete(key);
  this.map.set(key, val);
  return val;
};
Lru.prototype.set = function (key, val) {
  this.map.delete(key);
  this.map.set(key, val);
  while (this.map.size > this.cap) {
    this.map.delete(this.map.keys().next().value);
  }
};
Lru.prototype.drop = function (key) { this.map.delete(key); };

// -- The engine -------------------------------------------------------------

function Engine(loader) {
  this.loader = loader;
  this.meta = null;
  this.weights = null;
  this.bias = 0;
  this.clueN = null;        // uint32 per clue id, times the clue was seen
  this.caIndptr = null;     // clue id -> range in caPairs
  this.caPairs = null;      // (answer id, times) pairs, interleaved
  this.answers = null;      // {count, indptr, n, text, lengths}
  this.scores = null;       // reused similarity accumulator, one per clue
  this.termCache = new Lru(128);
  this.exactCache = new Lru(64);
  this.pastCache = new Lru(128);
  this.loading = null;
}

Engine.prototype.load = function (progress) {
  if (this.loading === null) {
    this.loading = this._load(progress || function () {});
  }
  return this.loading;
};

Engine.prototype._load = async function (progress) {
  progress("Loading model data (1/5)...");
  var meta = JSON.parse(new TextDecoder().decode(await this.loader("static_meta.json.gz")));
  if (meta.version !== 1) {
    throw new Error("Bundle is version " + meta.version + ", this engine expects 1");
  }
  this.meta = meta;
  this.weights = meta.weights;
  this.bias = meta.bias;

  progress("Loading model data (2/5)...");
  var bytes = await this.loader("pre_clue_n.gz");
  this.clueN = asU32(bytes, 0, meta.clues);
  progress("Loading model data (3/5)...");
  bytes = await this.loader("pre_ca_indptr.gz");
  this.caIndptr = asU32(bytes, 0, meta.clues + 1);
  progress("Loading model data (4/5)...");
  bytes = await this.loader("pre_ca_pairs.gz");
  this.caPairs = asU32(bytes, 0, meta.pairs * 2);

  progress("Loading model data (5/5)...");
  var box = parseContainer(await this.loader("pre_answers.gz"));
  var count = box.header.count;
  this.answers = {
    count: count,
    lengths: box.header.lengths,
    indptr: asU32(box.payload, 0, count + 1),
    n: asU32(box.payload, (count + 1) * 4, count),
    text: new TextDecoder().decode(box.payload.subarray((count + 1) * 4 + count * 4)),
  };
  this.scores = new Float32Array(meta.clues);
};

Engine.prototype._answer = function (id) {
  // Byte offsets equal character offsets: export_static.py enforces ASCII
  return this.answers.text.slice(this.answers.indptr[id], this.answers.indptr[id + 1]);
};

Engine.prototype._answerId = function (answer) {
  var range = this.answers.lengths[String(answer.length)];
  if (!range) return null;
  var lo = range[0], hi = range[1];
  while (lo < hi) {
    var mid = (lo + hi) >> 1;
    if (this._answer(mid) < answer) lo = mid + 1;
    else hi = mid;
  }
  return (lo < range[1] && this._answer(lo) === answer) ? lo : null;
};

Engine.prototype._shard = function (cache, name, parse) {
  var hit = cache.get(name);
  if (hit !== undefined) return hit;
  var prom = this.loader(name).then(parse, function (err) {
    cache.drop(name);
    throw err;
  });
  cache.set(name, prom);
  return prom;
};

Engine.prototype._termShard = function (index) {
  return this._shard(this.termCache, "terms_" + pad4(index) + ".gz", function (bytes) {
    var box = parseContainer(bytes);
    var ret = new Map();
    for (var term in box.header.terms) {
      var info = box.header.terms[term];  // [df, count, offset]
      var indices = asU32(box.payload, info[2], info[1]);
      for (var i = 1; i < indices.length; i++) indices[i] += indices[i - 1];
      ret.set(term, {
        df: info[0],
        indices: indices,
        data: asF32(box.payload, info[2] + info[1] * 4, info[1]),
      });
    }
    return ret;
  });
};

Engine.prototype._exactShard = function (index) {
  return this._shard(this.exactCache, "exact_" + pad4(index) + ".gz", function (bytes) {
    var count = new DataView(bytes.buffer, bytes.byteOffset, 4).getUint32(0, true);
    return {
      hashes: asU64(bytes, 4, count),
      ids: asU32(bytes, 4 + count * 8, count),
    };
  });
};

Engine.prototype._pastShard = function (index) {
  return this._shard(this.pastCache, "past_" + pad4(index) + ".gz", parseContainer);
};

Engine.prototype._exactId = async function (key) {
  var h = fnv1a64(key);
  var shard = await this._exactShard(Number(h % BigInt(this.meta.exact_shards)));
  var lo = 0, hi = shard.hashes.length;
  while (lo < hi) {
    var mid = (lo + hi) >> 1;
    if (shard.hashes[mid] < h) lo = mid + 1;
    else hi = mid;
  }
  return (lo < shard.hashes.length && shard.hashes[lo] === h) ? shard.ids[lo] : null;
};

Engine.prototype._pastList = async function (answerId) {
  var shard = await this._pastShard(answerId % this.meta.past_shards);
  var off = shard.header[String(answerId)];
  if (off === undefined) return [];
  var view = new DataView(shard.payload.buffer, shard.payload.byteOffset, shard.payload.byteLength);
  var decoder = new TextDecoder();
  var count = view.getUint32(off, true);
  var ret = [];
  off += 4;
  for (var i = 0; i < count; i++) {
    var n = view.getUint32(off, true);
    var len = view.getUint32(off + 4, true);
    ret.push({ n: n, display: decoder.decode(shard.payload.subarray(off + 8, off + 8 + len)) });
    off += 8 + len;
  }
  return ret;
};

// -- Solving, mirroring ClueModel.candidates/_gather/_nearest ---------------

Engine.prototype._nearest = async function (tokens, pool, skip) {
  if (!tokens.length || pool <= 0) return [];
  var counts = new Map();
  termsOf(tokens).forEach(function (term) {
    counts.set(term, (counts.get(term) || 0) + 1);
  });

  var order = Array.from(counts.keys());
  var self = this;
  var shardOf = function (term) {
    return Number(fnv1a64(term) % BigInt(self.meta.term_shards));
  };
  var needed = Array.from(new Set(order.map(shardOf)));
  await Promise.all(needed.map(function (i) { return self._termShard(i); }));

  // sqlite hands the Python module each IN () chunk in term order, so read
  // the shards the same way to keep the floating point sums aligned
  var found = [];
  for (var i = 0; i < order.length; i += 500) {
    var chunk = order.slice(i, i + 500).sort();
    for (var j = 0; j < chunk.length; j++) {
      var entry = (await this._termShard(shardOf(chunk[j]))).get(chunk[j]);
      if (entry !== undefined) found.push([chunk[j], entry]);
    }
  }

  var maxDf = Math.floor(this.meta.clues * MAX_DF_SHARE);
  var weights = [];
  for (i = 0; i < found.length; i++) {
    var df = found[i][1].df;
    if (df <= maxDf) {
      var idf = Math.log((1 + this.meta.clues) / (1 + df)) + 1;
      weights.push([found[i][1], counts.get(found[i][0]) * idf]);
    }
  }
  if (!weights.length) return [];
  var norm = 0;
  for (i = 0; i < weights.length; i++) norm += weights[i][1] * weights[i][1];
  norm = Math.sqrt(norm);

  var scores = this.scores;
  scores.fill(0);
  for (i = 0; i < weights.length; i++) {
    var indices = weights[i][0].indices, data = weights[i][0].data;
    var wf = Math.fround(weights[i][1] / norm);
    for (j = 0; j < indices.length; j++) {
      // float32 multiply then a float32 accumulate, same as numpy
      scores[indices[j]] += Math.fround(data[j] * wf);
    }
  }

  var kept = [];
  for (i = 0; i < scores.length; i++) {
    if (scores[i] >= MIN_SIM) kept.push(i);
  }
  kept.sort(function (a, b) { return scores[b] - scores[a] || a - b; });
  var top = kept.slice(0, Math.min(pool, this.meta.clues));
  var ret = [];
  for (i = 0; i < top.length; i++) {
    if (top[i] !== skip) ret.push([top[i], scores[top[i]]]);
  }
  return ret;
};

Engine.prototype.candidates = async function (clue, pattern, pool) {
  await this.load();
  if (pool === undefined) pool = 400;
  var constraint = (typeof pattern === "string") ? parsePattern(pattern) : (pattern || null);
  var tokens = normalize(clue);
  var self = this;
  var cands = new Map();

  function cand(answerId) {
    var cur = cands.get(answerId);
    if (cur === undefined) {
      cur = {
        answer: self._answer(answerId), answer_id: answerId,
        seen: self.answers.n[answerId],
        exact: 0, exact_share: 0.0, similar: 0.0, best_sim: 0.0,
      };
      cands.set(answerId, cur);
    }
    return cur;
  }

  var exactId = null;
  if (tokens.length) {
    var foundId = await this._exactId(tokens.join(" "));
    if (foundId !== null) {
      exactId = foundId;
      var clueN = this.clueN[exactId];
      for (var i = this.caIndptr[exactId]; i < this.caIndptr[exactId + 1]; i++) {
        var cur = cand(this.caPairs[i * 2]);
        cur.exact = this.caPairs[i * 2 + 1];
        cur.exact_share = cur.exact / clueN;
      }
    }
  }

  var nearest = await this._nearest(tokens, pool, exactId);
  for (var k = 0; k < nearest.length; k++) {
    var clueId = nearest[k][0], sim = nearest[k][1];
    var cn = this.clueN[clueId];
    for (i = this.caIndptr[clueId]; i < this.caIndptr[clueId + 1]; i++) {
      cur = cand(this.caPairs[i * 2]);
      cur.similar += sim * (this.caPairs[i * 2 + 1] / cn);
      if (sim > cur.best_sim) cur.best_sim = sim;
    }
  }

  if (constraint !== null) {
    var range = this.answers.lengths[String(constraint.length)];
    if (range) {
      for (var id = range[0]; id < range[1]; id++) {
        if (constraint.regex.test(this._answer(id))) cand(id);
      }
    }
    cands.forEach(function (cur, key) {
      if (cur.answer.length !== constraint.length || !constraint.regex.test(cur.answer)) {
        cands.delete(key);
      }
    });
  }

  var ret = [];
  cands.forEach(function (cur) {
    cur.exact_log = Math.log1p(cur.exact) / 8.0;
    if (cur.similar > 3.0) cur.similar = 3.0;
    cur.seen_log = Math.log1p(cur.seen) / 12.0;
    cur.score = self._score(cur);
    ret.push(cur);
  });
  ret.sort(function (a, b) {
    return b.score - a.score || (a.answer < b.answer ? -1 : a.answer > b.answer ? 1 : 0);
  });
  return ret;
};

Engine.prototype._score = function (features) {
  var total = 0;
  for (var i = 0; i < FEATURES.length; i++) {
    total += this.weights[FEATURES[i]] * features[FEATURES[i]];
  }
  return this.bias + total;
};

function round4(x) {
  return Math.round(x * 10000) / 10000;
}

Engine.prototype.solve = async function (clue, pattern, limit, opts) {
  opts = opts || {};
  var ranked = (await this.candidates(clue, pattern, opts.pool)).slice(0, limit || 10);
  var self = this;
  var exclude = normalize(clue).join(" ");
  return Promise.all(ranked.map(async function (cur) {
    var hit = {
      answer: cur.answer, score: round4(cur.score),
      exact: cur.exact, similar: round4(cur.similar), seen: cur.seen,
    };
    if (opts.raw) {
      hit.score_raw = cur.score;
      hit.similar_raw = cur.similar;
    }
    if (opts.examples !== false) {
      hit.example = await self._example(cur.answer_id, exclude);
    }
    return hit;
  }));
};

Engine.prototype._example = async function (answerId, exclude) {
  var list = await this._pastList(answerId);
  for (var i = 0; i < list.length; i++) {
    if (normalize(list[i].display).join(" ") !== exclude) return list[i].display;
  }
  return null;
};

Engine.prototype.pastClues = async function (answer, limit) {
  await this.load();
  var id = this._answerId(answer.toUpperCase());
  if (id === null) return [];
  var list = await this._pastList(id);
  return list.slice(0, limit || 10).map(function (cur) { return [cur.display, cur.n]; });
};

Engine.prototype.stats = function () {
  var ret = {};
  for (var key in this.meta) ret[key] = this.meta[key];
  return ret;
};

var api = {
  createEngine: function (loader) { return new Engine(loader); },
  normalize: normalize,
  termsOf: termsOf,
  parsePattern: parsePattern,
  fnv1a64: fnv1a64,
};
if (typeof module !== "undefined" && module.exports) {
  module.exports = api;
} else {
  root.ClueEngine = api;
}

})(this);
