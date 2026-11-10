/* The spatial index for the field, built off the main thread.
 *
 * A hundred thousand points is a real amount of work: built inline it stalls
 * the first frames of the field, which is the one moment a reader is deciding
 * whether the thing is alive. So it is built here, on another thread, and the
 * finished index is handed back as bytes rather than rebuilt on arrival.
 *
 * This worker reads coordinates and returns an index over them. It reaches
 * nothing, fetches nothing and knows nothing about what the points mean.
 */
importScripts("/assets/vendor-comlink.min.js", "/assets/vendor-flatbush.min.js");

Comlink.expose({
  index: function (xy, n) {
    var tree = new Flatbush(n);
    for (var i = 0; i < n; i++) {
      var x = xy[i * 2], y = xy[i * 2 + 1];
      /* A point is a box with no width. Flatbush wants boxes. */
      tree.add(x, y, x, y);
    }
    tree.finish();
    return Comlink.transfer(tree.data, [tree.data]);
  },
});
