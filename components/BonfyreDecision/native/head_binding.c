/* SPDX-License-Identifier: Apache-2.0
 * Stable Node-API binding. No Python, experimental Bun FFI, subprocess, or
 * network in inference. All typed-buffer lengths checked before native use.
 */
#include "bf_decision_head.h"
#include <math.h>
#include <node_api.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
  BfDecisionHeadConfig config;
  BfDecisionHeadModel *head;
} Model;
static const napi_type_tag MODEL_TAG = {0x2bcc364e591b4387ULL,
                                        0x9f408361104af67dULL};
static napi_value error(napi_env env, const char *message) {
  napi_throw_error(env, NULL, message);
  return NULL;
}
#define CHECK(call)                                                            \
  do {                                                                         \
    if ((call) != napi_ok)                                                     \
      return error(env, "decision head: Node-API failure");                    \
  } while (0)
static int integer(napi_env env, napi_value obj, const char *key,
                   uint32_t *out) {
  napi_value v;
  double d;
  if (napi_get_named_property(env, obj, key, &v) != napi_ok ||
      napi_get_value_double(env, v, &d) != napi_ok || !isfinite(d) || d < 0 ||
      d > UINT32_MAX || floor(d) != d)
    return 0;
  *out = (uint32_t)d;
  return 1;
}
static int config(napi_env env, napi_value value, BfDecisionHeadConfig *c) {
  return integer(env, value, "hidden_size", &c->hidden_size) &&
         integer(env, value, "width", &c->width) &&
         integer(env, value, "heads", &c->heads) &&
         integer(env, value, "feedforward", &c->feedforward) &&
         integer(env, value, "routing_layers", &c->routing_layers) &&
         integer(env, value, "layers", &c->layers) &&
         bf_decision_head_layout(c, NULL, NULL) > 0;
}
typedef struct {
  void *data;
  size_t length;
} BufferView;
typedef struct {
  napi_env env;
  napi_ref arraybuffer_byte_length;
} BufferGuard;
static void release_guard(void *data) {
  BufferGuard *guard = data;
  napi_delete_reference(guard->env, guard->arraybuffer_byte_length);
  free(guard);
}
static int typed(napi_env env, napi_value value, napi_typedarray_type expected,
                 BufferView *b, const BufferGuard *guard) {
  napi_typedarray_type type;
  napi_value buffer, getter, length;
  size_t offset;
  bool ordinary_buffer = false;
  /* Only engine-owned buffer metadata. Reading value.buffer or constructors
   * invokes replaceable JS getters while earlier arguments' data pointers are
   * live: a later getter can detach/resize one of those earlier buffers.
   * N-API returns the real backing buffer. Bun's napi_is_arraybuffer also
   * accepts shared storage, so use the captured intrinsic byteLength getter as
   * a brand check. It cannot execute user getters or Symbol.hasInstance hooks.
   * The intrinsic Object descriptor helper must be trusted at addon load;
   * runtime-global changes after initialization do not affect this reference.
   */
  if (napi_get_typedarray_info(env, value, &type, &b->length, &b->data,
                               &buffer, &offset) != napi_ok ||
      type != expected || !b->data || !b->length ||
      napi_is_arraybuffer(env, buffer, &ordinary_buffer) != napi_ok ||
      !ordinary_buffer ||
      napi_get_reference_value(env, guard->arraybuffer_byte_length, &getter) != napi_ok)
    return 0;
  napi_status status = napi_call_function(env, buffer, getter, 0, NULL, &length);
  if (status == napi_pending_exception) {
    napi_value exception;
    napi_get_and_clear_last_exception(env, &exception);
  }
  return status == napi_ok;
}
typedef struct {
  napi_env env;
  napi_value array;
  uint32_t index;
} LayoutContext;
static int layout_item(void *ctx, const char *name, size_t rows, size_t cols,
                       size_t rank, size_t offset) {
  LayoutContext *l = ctx;
  napi_value obj, key, v;
  if (napi_create_object(l->env, &obj) != napi_ok ||
      napi_create_string_utf8(l->env, name, NAPI_AUTO_LENGTH, &key) !=
          napi_ok ||
      napi_set_named_property(l->env, obj, "name", key) != napi_ok)
    return 0;
  const char *keys[] = {"rows", "columns", "rank", "offset"};
  size_t values[] = {rows, cols, rank, offset};
  for (size_t i = 0; i < 4; i++)
    if (napi_create_double(l->env, (double)values[i], &v) != napi_ok ||
        napi_set_named_property(l->env, obj, keys[i], v) != napi_ok)
      return 0;
  return napi_set_element(l->env, l->array, l->index++, obj) == napi_ok;
}
static napi_value layout(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  BfDecisionHeadConfig c;
  CHECK(napi_get_cb_info(env, info, &argc, argv, NULL, NULL));
  if (argc != 1 || !config(env, argv[0], &c))
    return error(env, "decision head: invalid config");
  LayoutContext l = {env, NULL, 0};
  CHECK(napi_create_array(env, &l.array));
  if (!bf_decision_head_layout(&c, layout_item, &l))
    return error(env, "decision head: layout failed");
  return l.array;
}
static void dispose(napi_env env, void *data, void *hint) {
  (void)env;
  (void)hint;
  Model *m = data;
  bf_decision_head_model_destroy(m->head);
  free(m);
}
static napi_value compile(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  BfDecisionHeadConfig c;
  BufferView b;
  BufferGuard *guard;
  CHECK(napi_get_cb_info(env, info, &argc, argv, NULL, (void **)&guard));
  if (argc != 2 || !config(env, argv[0], &c) ||
      !typed(env, argv[1], napi_float32_array, &b, guard) ||
      b.length != bf_decision_head_layout(&c, NULL, NULL))
    return error(env, "decision head: weight shape mismatch");
  Model *m = calloc(1, sizeof(*m));
  if (!m)
    return error(env, "decision head: allocation failed");
  m->config = c;
  int status = bf_decision_head_model_create(&c, b.data, b.length, &m->head);
  if (status) {
    free(m);
    return error(env, status == -3 ? "decision head: nonfinite weight"
                                   : "decision head: snapshot creation failed");
  }
  napi_value handle;
  if (napi_create_external(env, m, dispose, NULL, &handle) != napi_ok) {
    dispose(env, m, NULL);
    return error(env, "decision head: handle creation failed");
  }
  CHECK(napi_type_tag_object(env, handle, &MODEL_TAG));
  return handle;
}
static Model *model(napi_env env, napi_value value) {
  bool tagged = false;
  void *p = NULL;
  if (napi_check_object_type_tag(env, value, &MODEL_TAG, &tagged) != napi_ok ||
      !tagged || napi_get_value_external(env, value, &p) != napi_ok)
    return NULL;
  return p;
}
static napi_value workspace(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2], result;
  uint32_t s, q, o;
  CHECK(napi_get_cb_info(env, info, &argc, argv, NULL, NULL));
  Model *m = argc == 2 ? model(env, argv[0]) : NULL;
  if (!m || !integer(env, argv[1], "tokens", &s) ||
      !integer(env, argv[1], "fields", &q) ||
      !integer(env, argv[1], "options", &o))
    return error(env, "decision head: invalid dimensions");
  size_t n = bf_decision_head_workspace(&m->config, s, q, o);
  if (!n || n > 9007199254740991ULL)
    return error(env, "decision head: invalid workspace dimensions");
  CHECK(napi_create_double(env, (double)n, &result));
  return result;
}
static int overlaps(BufferView a, BufferView b) {
  uintptr_t x = (uintptr_t)a.data, y = (uintptr_t)b.data;
  // All buffers here have 4-byte elements. Compare distances, no end overflow.
  return x <= y ? y - x < a.length * 4 : x - y < b.length * 4;
}
static napi_value run(napi_env env, napi_callback_info info) {
  size_t argc = 6;
  napi_value argv[6];
  BufferView b[5];
  BufferGuard *guard;
  CHECK(napi_get_cb_info(env, info, &argc, argv, NULL, (void **)&guard));
  Model *m = argc == 6 ? model(env, argv[0]) : NULL;
  if (!m)
    return error(env, "decision head: invalid model handle");
  for (size_t i = 0; i < 5; i++)
    if (!typed(env, argv[i + 1],
               i == 1 || i == 2 ? napi_uint32_array : napi_float32_array,
               &b[i], guard) ||
        b[i].length > SIZE_MAX / 4)
      return error(env, "decision head: invalid typed input");
  for (size_t i = 0; i < 5; i++)
    for (size_t j = i + 1; j < 5; j++)
      if (overlaps(b[i], b[j]))
        return error(env, "decision head: input buffers must not overlap");
  if (b[0].length % m->config.hidden_size || b[1].length % 5 || b[2].length % 2)
    return error(env, "decision head: input shape mismatch");
  size_t s = b[0].length / m->config.hidden_size, q = b[1].length / 5,
         o = b[2].length / 2;
  if (q > 255 || o > 8192 || o > SIZE_MAX / m->config.hidden_size ||
      b[3].length != o * m->config.hidden_size)
    return error(env, "decision head: input shape or field budget exceeded");
  BfDecisionField *fields = calloc(q, sizeof(*fields));
  BfDecisionSpan *spans = calloc(o, sizeof(*spans));
  if (!fields || !spans) {
    free(fields);
    free(spans);
    return error(env, "decision head: allocation failed");
  }
  const uint32_t *fv = b[1].data, *ov = b[2].data;
  for (size_t i = 0; i < q; i++)
    fields[i] = (BfDecisionField){fv[i * 5],
                                  {fv[i * 5 + 1], fv[i * 5 + 2]},
                                  fv[i * 5 + 3],
                                  fv[i * 5 + 4]};
  for (size_t i = 0; i < o; i++)
    spans[i] = (BfDecisionSpan){ov[i * 2], ov[i * 2 + 1]};
  napi_value buffer, result;
  void *output;
  if (napi_create_arraybuffer(env, o * sizeof(float), &output, &buffer) !=
      napi_ok) {
    free(fields);
    free(spans);
    return error(env, "decision head: output allocation failed");
  }
  int status = bf_decision_head_model_run(m->head, b[0].data, s, fields, q,
                                          spans, o, b[3].data, b[4].data,
                                          b[4].length, output);
  free(fields);
  free(spans);
  if (status)
    return error(env, status == -2 ? "decision head: short workspace"
                      : status == -3
                          ? "decision head: nonfinite computation"
                          : "decision head: invalid spans or dimensions");
  CHECK(napi_create_typedarray(env, napi_float32_array, o, buffer, 0, &result));
  return result;
}
static napi_value init(napi_env env, napi_value exports) {
  // Get ArrayBuffer.prototype from an engine-created buffer, not the mutable
  // global ArrayBuffer constructor. Capture its builtin getter once.
  napi_value sample, prototype, global, object, descriptor_fn, key, descriptor, getter;
  CHECK(napi_create_arraybuffer(env, 1, NULL, &sample));
  CHECK(napi_get_prototype(env, sample, &prototype));
  CHECK(napi_get_global(env, &global));
  CHECK(napi_get_named_property(env, global, "Object", &object));
  CHECK(napi_get_named_property(env, object, "getOwnPropertyDescriptor", &descriptor_fn));
  CHECK(napi_create_string_utf8(env, "byteLength", NAPI_AUTO_LENGTH, &key));
  napi_value args[] = {prototype, key};
  CHECK(napi_call_function(env, object, descriptor_fn, 2, args, &descriptor));
  CHECK(napi_get_named_property(env, descriptor, "get", &getter));
  BufferGuard *guard = calloc(1, sizeof(*guard));
  if (!guard)
    return error(env, "decision head: buffer guard allocation failed");
  guard->env = env;
  if (napi_create_reference(env, getter, 1, &guard->arraybuffer_byte_length) != napi_ok) {
    free(guard);
    return error(env, "decision head: buffer guard initialization failed");
  }
  if (napi_add_env_cleanup_hook(env, release_guard, guard) != napi_ok) {
    release_guard(guard);
    return error(env, "decision head: buffer guard cleanup failed");
  }
  napi_property_descriptor properties[] = {
      {"layout", NULL, layout, NULL, NULL, NULL, napi_default, NULL},
      {"compile", NULL, compile, NULL, NULL, NULL, napi_default, guard},
      {"workspace", NULL, workspace, NULL, NULL, NULL, napi_default, NULL},
      {"run", NULL, run, NULL, NULL, NULL, napi_default, guard}};
  CHECK(napi_define_properties(
      env, exports, sizeof(properties) / sizeof(properties[0]), properties));
  return exports;
}
NAPI_MODULE(NODE_GYP_MODULE_NAME, init)
