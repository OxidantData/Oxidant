/*
 * Mock GPU shim for the oxidant-gpu spike (KAN-70).
 *
 * Default (non-`gpu`) build of the crate links this instead of the real libcudf
 * shim so the full plan-rule → JSON-spec → plain-C FFI → Arrow C Data Interface
 * plumbing is testable on CPU in CI. It ignores the spec entirely and always
 * returns one batch with a single int64 column "mock_sum" holding the value 42.
 *
 * The ArrowSchema/ArrowArray struct definitions below are the ABI-stable Arrow C
 * Data Interface structs (https://arrow.apache.org/docs/format/CDataInterface.html),
 * reproduced here so the shim has zero Arrow dependencies.
 */

#include <stdint.h>
#include <stdlib.h>

struct ArrowSchema {
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;
  void (*release)(struct ArrowSchema*);
  void* private_data;
};

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;
  void (*release)(struct ArrowArray*);
  void* private_data;
};

/* Child structs are owned (and freed) by the root's release callback; a child's
 * own release is a no-op so a consumer that releases the tree twice hits a
 * NULL release pointer the second time, per the C Data Interface contract. */
static void mock_release_noop_schema(struct ArrowSchema* schema) {
  if (schema != NULL) {
    schema->release = NULL;
  }
}

static void mock_release_noop_array(struct ArrowArray* array) {
  if (array != NULL) {
    array->release = NULL;
  }
}

static void mock_release_root_schema(struct ArrowSchema* schema) {
  int64_t i;
  if (schema == NULL) {
    return;
  }
  if (schema->children != NULL) {
    for (i = 0; i < schema->n_children; i++) {
      free(schema->children[i]);
    }
    free(schema->children);
    schema->children = NULL;
    schema->n_children = 0;
  }
  schema->release = NULL;
}

static void mock_release_root_array(struct ArrowArray* array) {
  int64_t i;
  if (array == NULL) {
    return;
  }
  if (array->children != NULL) {
    for (i = 0; i < array->n_children; i++) {
      struct ArrowArray* child = array->children[i];
      if (child != NULL) {
        /* child buffers: [validity (NULL), values (malloc'd)] */
        free((void*)child->buffers[1]);
        free(child->buffers);
        free(child);
      }
    }
    free(array->children);
    array->children = NULL;
    array->n_children = 0;
  }
  free((void*)array->buffers);
  array->buffers = NULL;
  array->release = NULL;
}

/* Ignores `spec_json`; fills `out_schema`/`out_array` with one struct batch of a
 * single int64 column "mock_sum" = 42. Returns 0 on success, non-zero on bad
 * arguments or allocation failure. */
int oxidant_gpu_exec(const char* spec_json, struct ArrowSchema* out_schema,
                     struct ArrowArray* out_array) {
  struct ArrowSchema* child_schema = NULL;
  struct ArrowArray* child_array = NULL;
  const void** child_buffers = NULL;
  int64_t* values = NULL;

  (void)spec_json;

  if (out_schema == NULL || out_array == NULL) {
    return 1;
  }

  child_schema = (struct ArrowSchema*)malloc(sizeof(struct ArrowSchema));
  child_array = (struct ArrowArray*)malloc(sizeof(struct ArrowArray));
  child_buffers = (const void**)malloc(2 * sizeof(void*));
  values = (int64_t*)malloc(sizeof(int64_t));
  out_schema->children =
      (struct ArrowSchema**)malloc(sizeof(struct ArrowSchema*));
  out_array->children = (struct ArrowArray**)malloc(sizeof(struct ArrowArray*));
  out_array->buffers = (const void**)malloc(sizeof(void*));
  if (child_schema == NULL || child_array == NULL || child_buffers == NULL ||
      values == NULL || out_schema->children == NULL ||
      out_array->children == NULL || out_array->buffers == NULL) {
    goto fail;
  }

  *child_schema = (struct ArrowSchema){
      .format = "l", /* int64 */
      .name = "mock_sum",
      .metadata = NULL,
      .flags = 0, /* non-nullable */
      .n_children = 0,
      .children = NULL,
      .dictionary = NULL,
      .release = mock_release_noop_schema,
      .private_data = NULL,
  };

  *out_schema = (struct ArrowSchema){
      .format = "+s", /* struct */
      .name = "",
      .metadata = NULL,
      .flags = 0,
      .n_children = 1,
      .children = out_schema->children,
      .dictionary = NULL,
      .release = mock_release_root_schema,
      .private_data = NULL,
  };
  out_schema->children[0] = child_schema;

  values[0] = 42;
  child_buffers[0] = NULL; /* no validity bitmap: null_count == 0 */
  child_buffers[1] = values;
  *child_array = (struct ArrowArray){
      .length = 1,
      .null_count = 0,
      .offset = 0,
      .n_buffers = 2,
      .n_children = 0,
      .buffers = child_buffers,
      .children = NULL,
      .dictionary = NULL,
      .release = mock_release_noop_array,
      .private_data = NULL,
  };

  out_array->buffers[0] = NULL; /* struct validity: none */
  *out_array = (struct ArrowArray){
      .length = 1,
      .null_count = 0,
      .offset = 0,
      .n_buffers = 1,
      .n_children = 1,
      .buffers = out_array->buffers,
      .children = out_array->children,
      .dictionary = NULL,
      .release = mock_release_root_array,
      .private_data = NULL,
  };
  out_array->children[0] = child_array;

  return 0;

fail:
  free(child_schema);
  free(child_array);
  free((void*)child_buffers);
  free(values);
  free(out_schema->children);
  free(out_array->children);
  free((void*)out_array->buffers);
  return 2;
}
