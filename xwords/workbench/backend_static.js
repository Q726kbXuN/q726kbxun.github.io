"use strict";

// Backend for the static flavor of the workbench: no server code at all.
// engine.js runs the clue model right in the browser, reading the bundle
// objects written by ./export_static.py from the same static host as the
// page.  Objects are gzip blobs, decompressed with DecompressionStream the
// same way the crossword archive viewer loads its data.

var Backend = (function () {
  function loader(name) {
    return fetch(name).then(function (resp) {
      if (!resp.ok) throw new Error("Failed to fetch " + name + " (HTTP " + resp.status + ")");
      var stream = resp.body.pipeThrough(new DecompressionStream("gzip"));
      return new Response(stream).arrayBuffer();
    }).then(function (buf) {
      return new Uint8Array(buf);
    });
  }

  var engine = ClueEngine.createEngine(loader);

  return {
    init: function (onStatus) {
      engine.load(onStatus).then(function () {
        onStatus("");
      }, function (err) {
        onStatus("Model failed to load: " + err.message);
      });
    },

    solve: function (clue, pattern, limit) {
      return engine.solve(clue, pattern, limit);
    },

    history: function (answer) {
      return engine.pastClues(answer, 10);
    }
  };
})();
