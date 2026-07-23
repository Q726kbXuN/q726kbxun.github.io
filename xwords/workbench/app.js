"use strict";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

var STORAGE_KEY = "xword_workbench_v1";
var MIN_SIZE = 3;
var MAX_SIZE = 25;
var HINT_LIMIT = 20;

var state = null;    // persisted: size, blocks, letters, clue text, ui prefs
var sel = null;      // {r, c, dir} with dir "A" or "D", or null
var layout = null;   // computed numbering, rebuilt after any block/size edit

var gridEl = document.getElementById("grid");
var hintStatus = document.getElementById("hint-status");
var hintList = document.getElementById("hint-list");

function freshState(width, height) {
  var blocks = [], letters = [];
  for (var r = 0; r < height; r++) {
    blocks.push(new Array(width).fill(false));
    letters.push(new Array(width).fill(""));
  }
  return {
    width: width, height: height, blocks: blocks, letters: letters,
    clues: {}, symmetry: true, mode: "blocks"
  };
}

function normalizeState(raw) {
  if (!raw || typeof raw !== "object" || !raw.width || !raw.height) return null;
  var w = Math.min(Math.max(raw.width | 0, MIN_SIZE), MAX_SIZE);
  var h = Math.min(Math.max(raw.height | 0, MIN_SIZE), MAX_SIZE);
  var st = freshState(w, h);
  for (var r = 0; r < h; r++) {
    for (var c = 0; c < w; c++) {
      st.blocks[r][c] = !!(raw.blocks && raw.blocks[r] && raw.blocks[r][c]);
      var ch = raw.letters && raw.letters[r] ? raw.letters[r][c] : "";
      st.letters[r][c] = (typeof ch === "string" && /^[A-Za-z]$/.test(ch)) ? ch.toUpperCase() : "";
      if (st.blocks[r][c]) st.letters[r][c] = "";
    }
  }
  if (raw.clues && typeof raw.clues === "object") {
    for (var key in raw.clues) {
      if (/^[AD]:\d+,\d+$/.test(key) && typeof raw.clues[key] === "string") {
        st.clues[key] = raw.clues[key];
      }
    }
  }
  st.symmetry = raw.symmetry !== false;
  st.mode = raw.mode === "letters" ? "letters" : "blocks";
  return st;
}

function save() {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch (e) { /* storage blocked or full, keep going without autosave */ }
}

function load() {
  try {
    var st = normalizeState(JSON.parse(localStorage.getItem(STORAGE_KEY)));
    if (st) return st;
  } catch (e) { }
  return freshState(15, 15);
}

// ---------------------------------------------------------------------------
// Numbering: a cell starts a slot if it is white, closed on one side, and
// open on the other (so every slot is at least two cells long)
// ---------------------------------------------------------------------------

function computeLayout() {
  var w = state.width, h = state.height, b = state.blocks;
  var slots = [], numbers = {}, slotAt = {};
  var num = 0;
  for (var r = 0; r < h; r++) {
    for (var c = 0; c < w; c++) {
      if (b[r][c]) continue;
      var startA = (c === 0 || b[r][c - 1]) && c + 1 < w && !b[r][c + 1];
      var startD = (r === 0 || b[r - 1][c]) && r + 1 < h && !b[r + 1][c];
      if (startA || startD) numbers[r + "," + c] = ++num;
      if (startA) slots.push(makeSlot(num, "A", r, c, 0, 1));
      if (startD) slots.push(makeSlot(num, "D", r, c, 1, 0));
    }
  }
  slots.forEach(function (slot, i) {
    slot.cells.forEach(function (cell) {
      slotAt[slot.dir + ":" + cell[0] + "," + cell[1]] = i;
    });
  });
  layout = { slots: slots, numbers: numbers, slotAt: slotAt };
}

function makeSlot(num, dir, r, c, dr, dc) {
  var cells = [];
  while (r < state.height && c < state.width && !state.blocks[r][c]) {
    cells.push([r, c]);
    r += dr; c += dc;
  }
  return { num: num, dir: dir, cells: cells, key: dir + ":" + cells[0][0] + "," + cells[0][1] };
}

function slotFor(r, c, dir) {
  var i = layout.slotAt[dir + ":" + r + "," + c];
  return i === undefined ? null : layout.slots[i];
}

function activeSlot() {
  return sel ? slotFor(sel.r, sel.c, sel.dir) : null;
}

function setSel(r, c, dir) {
  if (state.blocks[r][c]) return;
  dir = dir || (sel ? sel.dir : "A");
  if (!slotFor(r, c, dir) && slotFor(r, c, dir === "A" ? "D" : "A")) {
    dir = dir === "A" ? "D" : "A";
  }
  sel = { r: r, c: c, dir: dir };
}

function slotPattern(slot) {
  return slot.cells.map(function (cell) {
    return state.letters[cell[0]][cell[1]] || ".";
  }).join("");
}

function slotLabel(slot) {
  return slot.num + "-" + (slot.dir === "A" ? "Across" : "Down");
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

function refreshAll() {
  document.getElementById("width-input").value = state.width;
  document.getElementById("height-input").value = state.height;
  document.getElementById("symmetry").checked = state.symmetry;
  setMode(state.mode);
  clearHints();
  computeLayout();
  renderGrid();
  renderClues();
}

function clearHints() {
  // Hint rows capture the slot they were built for, so they must go
  // whenever the slots themselves may have changed
  hintStatus.textContent = "";
  hintList.innerHTML = "";
}

function renderGrid() {
  var w = state.width, h = state.height;
  var slot = activeSlot();
  var inSlot = {};
  if (slot) slot.cells.forEach(function (cell) { inSlot[cell[0] + "," + cell[1]] = true; });
  gridEl.style.setProperty("--cell", (w <= 15 ? 36 : w <= 20 ? 32 : 27) + "px");
  gridEl.style.gridTemplateColumns = "repeat(" + w + ", var(--cell))";
  gridEl.innerHTML = "";
  for (var r = 0; r < h; r++) {
    for (var c = 0; c < w; c++) {
      var cell = document.createElement("div");
      cell.className = "cell";
      cell.dataset.r = r;
      cell.dataset.c = c;
      if (state.blocks[r][c]) {
        cell.className += " block";
      } else {
        var key = r + "," + c;
        if (inSlot[key]) cell.className += " in-slot";
        if (sel && sel.r === r && sel.c === c) cell.className += " cursor";
        if (layout.numbers[key]) {
          var numEl = document.createElement("span");
          numEl.className = "num";
          numEl.textContent = layout.numbers[key];
          cell.appendChild(numEl);
        }
        if (state.letters[r][c]) {
          var letEl = document.createElement("span");
          letEl.className = "letter";
          letEl.textContent = state.letters[r][c];
          cell.appendChild(letEl);
        }
      }
      gridEl.appendChild(cell);
    }
  }
  updateStatus();
  highlightClueEntries();
}

function renderClues() {
  renderClueColumn("A", document.getElementById("across-list"));
  renderClueColumn("D", document.getElementById("down-list"));
  highlightClueEntries();
}

function renderClueColumn(dir, box) {
  box.innerHTML = "";
  layout.slots.forEach(function (slot) {
    if (slot.dir !== dir) return;
    var row = document.createElement("div");
    row.className = "clue-entry";
    row.dataset.key = slot.key;
    var num = document.createElement("span");
    num.className = "clue-num";
    num.textContent = slot.num + ".";
    var input = document.createElement("input");
    input.type = "text";
    input.className = "clue-text";
    input.placeholder = "(" + slot.cells.length + ")";
    input.value = state.clues[slot.key] || "";
    input.addEventListener("input", function () {
      state.clues[slot.key] = input.value;
      save();
    });
    input.addEventListener("focus", function () {
      setSel(slot.cells[0][0], slot.cells[0][1], slot.dir);
      renderGrid();
    });
    input.addEventListener("keydown", function (ev) {
      if (ev.key === "Enter") {
        ev.preventDefault();
        requestHint();
      }
    });
    row.appendChild(num);
    row.appendChild(input);
    row.addEventListener("click", function (ev) {
      if (ev.target !== input) {
        setSel(slot.cells[0][0], slot.cells[0][1], slot.dir);
        renderGrid();
      }
    });
    box.appendChild(row);
  });
}

function highlightClueEntries() {
  var slot = activeSlot();
  var key = slot ? slot.key : null;
  document.querySelectorAll(".clue-entry").forEach(function (el) {
    el.classList.toggle("active", el.dataset.key === key);
  });
}

function updateStatus() {
  var box = document.getElementById("grid-status");
  var slot = activeSlot();
  if (slot) {
    box.textContent = slotLabel(slot) + "  [" + slotPattern(slot) + "]";
  } else if (state.mode === "blocks") {
    box.textContent = "Blocks mode: click cells to toggle black squares.";
  } else {
    box.textContent = "Click a white cell to select a slot.";
  }
}

// ---------------------------------------------------------------------------
// Grid editing
// ---------------------------------------------------------------------------

gridEl.addEventListener("click", function (ev) {
  var cell = ev.target.closest(".cell");
  if (!cell) return;
  var r = +cell.dataset.r, c = +cell.dataset.c;
  if (state.mode === "blocks") {
    toggleBlock(r, c);
  } else if (!state.blocks[r][c]) {
    if (sel && sel.r === r && sel.c === c) {
      setSel(r, c, sel.dir === "A" ? "D" : "A");
    } else {
      setSel(r, c);
    }
    renderGrid();
  }
  gridEl.focus();
});

function toggleBlock(r, c) {
  var value = !state.blocks[r][c];
  setBlock(r, c, value);
  if (state.symmetry) {
    var mr = state.height - 1 - r, mc = state.width - 1 - c;
    if (mr !== r || mc !== c) setBlock(mr, mc, value);
  }
  if (sel && state.blocks[sel.r][sel.c]) sel = null;
  clearHints();
  computeLayout();
  if (sel) setSel(sel.r, sel.c, sel.dir);  // re-anchor onto a live slot
  renderGrid();
  renderClues();
  save();
}

function setBlock(r, c, value) {
  state.blocks[r][c] = value;
  if (value) state.letters[r][c] = "";
}

document.addEventListener("keydown", function (ev) {
  var tag = ev.target.tagName;
  if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
  if (ev.ctrlKey || ev.metaKey || ev.altKey || !sel) return;
  var key = ev.key;
  if (/^[a-zA-Z]$/.test(key)) {
    ev.preventDefault();
    typeLetter(key.toUpperCase());
  } else if (key === "Backspace") {
    ev.preventDefault();
    backspace();
  } else if (key === "Delete") {
    ev.preventDefault();
    state.letters[sel.r][sel.c] = "";
    renderGrid();
    save();
  } else if (key === " ") {
    ev.preventDefault();
    setSel(sel.r, sel.c, sel.dir === "A" ? "D" : "A");
    renderGrid();
  } else if (key === "ArrowLeft") { ev.preventDefault(); moveSel(0, -1); }
  else if (key === "ArrowRight") { ev.preventDefault(); moveSel(0, 1); }
  else if (key === "ArrowUp") { ev.preventDefault(); moveSel(-1, 0); }
  else if (key === "ArrowDown") { ev.preventDefault(); moveSel(1, 0); }
});

function selIndexIn(slot) {
  for (var i = 0; i < slot.cells.length; i++) {
    if (slot.cells[i][0] === sel.r && slot.cells[i][1] === sel.c) return i;
  }
  return -1;
}

function typeLetter(ch) {
  state.letters[sel.r][sel.c] = ch;
  var slot = activeSlot();
  if (slot) {
    var i = selIndexIn(slot);
    if (i >= 0 && i + 1 < slot.cells.length) {
      sel.r = slot.cells[i + 1][0];
      sel.c = slot.cells[i + 1][1];
    }
  }
  renderGrid();
  save();
}

function backspace() {
  if (state.letters[sel.r][sel.c]) {
    state.letters[sel.r][sel.c] = "";
  } else {
    var slot = activeSlot();
    var i = slot ? selIndexIn(slot) : -1;
    if (i > 0) {
      sel.r = slot.cells[i - 1][0];
      sel.c = slot.cells[i - 1][1];
      state.letters[sel.r][sel.c] = "";
    }
  }
  renderGrid();
  save();
}

function moveSel(dr, dc) {
  var r = sel.r + dr, c = sel.c + dc;
  while (r >= 0 && r < state.height && c >= 0 && c < state.width) {
    if (!state.blocks[r][c]) {
      setSel(r, c, sel.dir);
      renderGrid();
      return;
    }
    r += dr; c += dc;
  }
}

// ---------------------------------------------------------------------------
// Toolbar
// ---------------------------------------------------------------------------

function setMode(mode) {
  state.mode = mode;
  document.getElementById("mode-blocks").classList.toggle("active", mode === "blocks");
  document.getElementById("mode-letters").classList.toggle("active", mode === "letters");
  updateStatus();
  save();
}

function clampSize(value) {
  var n = parseInt(value, 10);
  return Math.min(Math.max(isNaN(n) ? 15 : n, MIN_SIZE), MAX_SIZE);
}

function hasContent() {
  for (var r = 0; r < state.height; r++) {
    for (var c = 0; c < state.width; c++) {
      if (state.blocks[r][c] || state.letters[r][c]) return true;
    }
  }
  for (var key in state.clues) {
    if (state.clues[key]) return true;
  }
  return false;
}

document.getElementById("mode-blocks").addEventListener("click", function () { setMode("blocks"); });
document.getElementById("mode-letters").addEventListener("click", function () { setMode("letters"); });

document.getElementById("symmetry").addEventListener("change", function () {
  state.symmetry = document.getElementById("symmetry").checked;
  save();
});

document.getElementById("new-grid").addEventListener("click", function () {
  var w = clampSize(document.getElementById("width-input").value);
  var h = clampSize(document.getElementById("height-input").value);
  if (hasContent() && !confirm("Start a new " + w + "x" + h + " grid? The current puzzle is discarded.")) return;
  var symmetry = document.getElementById("symmetry").checked;
  state = freshState(w, h);
  state.symmetry = symmetry;
  sel = null;
  refreshAll();
  save();
});

document.getElementById("clear-btn").addEventListener("click", function () {
  if (hasContent() && !confirm("Clear the whole puzzle?")) return;
  state = freshState(state.width, state.height);
  sel = null;
  refreshAll();
  save();
});

document.getElementById("export-btn").addEventListener("click", function () {
  var blob = new Blob([JSON.stringify(state, null, 2)], { type: "application/json" });
  var a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "crossword-" + state.width + "x" + state.height + ".json";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(a.href);
});

var importFile = document.getElementById("import-file");
document.getElementById("import-btn").addEventListener("click", function () { importFile.click(); });
importFile.addEventListener("change", function () {
  var file = importFile.files[0];
  importFile.value = "";
  if (!file) return;
  var reader = new FileReader();
  reader.onload = function () {
    var st = null;
    try { st = normalizeState(JSON.parse(reader.result)); } catch (e) { }
    if (!st) {
      alert("That file does not look like an exported crossword.");
      return;
    }
    state = st;
    sel = null;
    refreshAll();
    save();
  };
  reader.readAsText(file);
});

// ---------------------------------------------------------------------------
// Hints
// ---------------------------------------------------------------------------

document.getElementById("hint-btn").addEventListener("click", requestHint);

var hintGen = 0;  // stamps each request so a slow reply cannot clobber a newer one

function requestHint() {
  var slot = activeSlot();
  if (!slot) {
    hintStatus.textContent = "Select a slot first.";
    return;
  }
  var gen = ++hintGen;
  hintStatus.innerHTML = "";
  var spin = document.createElement("span");
  spin.className = "spinner";
  hintStatus.appendChild(spin);
  hintStatus.appendChild(document.createTextNode(" Solving " + slotLabel(slot) + "..."));
  hintList.innerHTML = "";
  Backend.solve(state.clues[slot.key] || "", slotPattern(slot), HINT_LIMIT).then(function (matches) {
    if (gen !== hintGen) return;
    showHints(slot, matches);
  }).catch(function (err) {
    if (gen !== hintGen) return;
    hintStatus.textContent = "Error: " + err.message;
  });
}

function showHints(slot, matches) {
  if (!matches.length) {
    hintStatus.textContent = "No suggestions for " + slotLabel(slot) + ".";
    return;
  }
  hintStatus.textContent = slotLabel(slot) + " [" + slotPattern(slot) + "], " +
    matches.length + " suggestion" + (matches.length === 1 ? "" : "s") + ":";
  var best = Math.max(matches[0].score, 0.001);
  matches.forEach(function (m) {
    hintList.appendChild(renderHint(slot, m, best));
  });
}

function renderHint(slot, m, best) {
  var row = document.createElement("div");
  row.className = "hint";
  row.title = "Click to write " + m.answer + " into the grid";

  var head = document.createElement("div");
  head.className = "hint-answer";
  var word = document.createElement("span");
  word.className = "hint-word";
  word.textContent = m.answer;
  var meta = document.createElement("span");
  meta.className = "hint-meta";
  meta.textContent = "score " + m.score.toFixed(2) + ", seen " + m.seen;
  head.appendChild(word);
  head.appendChild(meta);
  row.appendChild(head);

  var bar = document.createElement("div");
  bar.className = "bar";
  var fill = document.createElement("div");
  fill.className = "bar-fill";
  fill.style.width = Math.max(4, Math.min(100, 100 * m.score / best)) + "%";
  bar.appendChild(fill);
  row.appendChild(bar);

  if (m.example) {
    var ex = document.createElement("div");
    ex.className = "hint-example";
    ex.textContent = m.example;
    row.appendChild(ex);
  }

  var links = document.createElement("div");
  links.className = "hint-links";
  var hist = document.createElement("a");
  hist.textContent = "past clues";
  hist.href = "#";
  hist.addEventListener("click", function (ev) {
    ev.preventDefault();
    ev.stopPropagation();
    toggleHistory(row, m.answer);
  });
  links.appendChild(hist);
  row.appendChild(links);

  row.addEventListener("click", function () { applyHint(slot, m.answer); });
  return row;
}

function applyHint(slot, answer) {
  // Re-resolve the slot in the current layout in case the grid changed
  // between asking for the hint and clicking it
  var at = layout.slotAt[slot.key];
  var live = at === undefined ? null : layout.slots[at];
  if (!live || live.key !== slot.key || live.cells.length !== answer.length) {
    hintStatus.textContent = "The grid changed; ask for a fresh hint.";
    return;
  }
  live.cells.forEach(function (cell, i) {
    state.letters[cell[0]][cell[1]] = answer[i];
  });
  setSel(live.cells[0][0], live.cells[0][1], live.dir);
  renderGrid();
  save();
}

function toggleHistory(row, answer) {
  var box = row.querySelector(".hint-history");
  if (box) {
    box.remove();
    return;
  }
  box = document.createElement("div");
  box.className = "hint-history";
  box.textContent = "Loading...";
  box.addEventListener("click", function (ev) { ev.stopPropagation(); });
  row.appendChild(box);
  Backend.history(answer)
    .then(function (clues) {
      box.innerHTML = "";
      if (!clues.length) {
        box.textContent = "No past clues.";
        return;
      }
      clues.forEach(function (pair) {
        var line = document.createElement("div");
        line.textContent = pair[0] + " (x" + pair[1] + ")";
        box.appendChild(line);
      });
    })
    .catch(function () { box.textContent = "Failed to load history."; });
}

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

state = load();
refreshAll();
if (Backend.init) {
  // The static backend downloads the model data up front; surface progress
  Backend.init(function (msg) { hintStatus.textContent = msg; });
}
