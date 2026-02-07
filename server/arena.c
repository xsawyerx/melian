#include <string.h>
#include <stdlib.h>
#include "util.h"
#include "log.h"
#include "arena.h"

Arena* arena_build(unsigned capacity) {
  Arena* arena = 0;
  unsigned bad = 0;
  do {
    arena = calloc(1, sizeof(Arena));
    if (!arena) {
      LOG_WARN("Could not allocate Arena object");
      break;
    }
    arena->capacity = capacity;

    arena->buffer = malloc(capacity);
    if (!arena->buffer) {
      LOG_WARN("Could not allocate Arena buffer");
      ++bad;
      break;
    }
  } while (0);
  if (bad) {
    arena_destroy(arena);
    arena = 0;
  }
  return arena;
}

void arena_destroy(Arena* arena) {
  if (!arena) return;
  if (arena->buffer) free(arena->buffer);
  free(arena);
}

void arena_reset(Arena* arena) {
  arena->used = 0;
}

static void arena_check_and_grow(Arena* arena, unsigned extra) {
  unsigned total = arena->used + extra;
  if (total <= arena->capacity) return;

  unsigned capacity = next_power_of_two(total, arena->capacity);
  uint8_t *buffer = realloc(arena->buffer, capacity);
  if (!buffer) {
    LOG_FATAL("Arena realloc failed: need %u bytes (cap %u)", total, capacity);
  }
  LOG_DEBUG("Arena need %u grow %p %u => %p %u", total, (void*)arena->buffer, arena->capacity, (void*)buffer, capacity);
  arena->buffer = buffer;
  arena->capacity = capacity;
}

unsigned arena_store(Arena* arena, const uint8_t *src, unsigned len) {
  arena_check_and_grow(arena, len);
  unsigned index = arena->used;
  uint8_t *ptr = arena->buffer + index;
  memcpy(ptr, src, len);
  arena->used += len;
  return index;
}

unsigned arena_store_framed(Arena* arena, const uint8_t *src, unsigned len) {
  // Store preframed value in arena
  uint8_t hdr[sizeof(unsigned)] = {
    (uint8_t)((len >> 24) & 0xFF),
    (uint8_t)((len >> 16) & 0xFF),
    (uint8_t)((len >> 8)  & 0xFF),
    (uint8_t)( len        & 0xFF)
  };
  unsigned vstore = arena_store(arena, hdr, sizeof(unsigned));
  if (vstore == (unsigned)-1) return -1;
  unsigned  fstore = arena_store(arena, src, len);
  if (fstore == (unsigned)-1) return -1;
  return vstore;
}
