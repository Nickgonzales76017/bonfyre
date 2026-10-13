#define _POSIX_C_SOURCE 200809L
// Bounded exact-source PCM mixing. The caller owns media access/rights.
// The pure C/WASM inner kernel never allocates, serializes or checks policy.
#include <errno.h>
#include <float.h>
#include <inttypes.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include "mix_kernel.h"

#define MAX_STEMS 64u
#define BLOCK_SAMPLES 1024u
#define MAX_SAMPLES 100000000u
#define MAX_CHANNELS 8u

typedef struct {
  const char *path;
  uint64_t at, from, frames;
  float gain;
  FILE *stream;
  uint64_t next_float_offset;
  int cursor_valid;
} Stem;

typedef struct {
  uint64_t source_reads;
  uint64_t explicit_seeks;
  uint64_t source_bytes_read;
  uint64_t overlap_probes;
} RenderStats;

/* One future event per validated stem. The event tables are stack-bounded by
 * MAX_STEMS and do not alter input or output ownership. */
typedef struct {
  uint64_t frame;
  unsigned stem_index;
} StemEvent;

static int event_compare(const void *left, const void *right) {
  const StemEvent *a = (const StemEvent *)left, *b = (const StemEvent *)right;
  if (a->frame < b->frame) return -1;
  if (a->frame > b->frame) return 1;
  return (a->stem_index > b->stem_index) - (a->stem_index < b->stem_index);
}

static int uint_arg(const char *value, uint64_t ceiling, uint64_t *out) {
  char *tail = NULL;
  if (!value || !*value || *value == '-' || *value == '+') return 0;
  errno = 0;
  uintmax_t n = strtoumax(value, &tail, 10);
  if (errno || !tail || *tail || n > ceiling) return 0;
  *out = (uint64_t)n;
  return 1;
}

static int gain_arg(const char *value, float *out) {
  char *tail = NULL;
  if (!value || !*value) return 0;
  errno = 0;
  double n = strtod(value, &tail);
  if (errno || !tail || *tail || !isfinite(n) || n < -4.0 || n > 4.0) return 0;
  *out = (float)n;
  return 1;
}

static void usage(void) {
  fputs("usage: creator-mix --samples N --channels C --output FILE "
        "--stem FILE AT FROM FRAMES GAIN [--stem ...] [--sparse-silence]\n"
        "Coordinates are integer sample frames; all bodies are interleaved f32le.\n", stderr);
}

static int read_stem(Stem *stem, uint64_t from, float *buffer, size_t count,
                     RenderStats *stats) {
  // A stem's source coordinates advance linearly through consecutive output
  // blocks. Do not seek again when the next body bytes are already at the
  // stream cursor. Each independent stem preserves its own file position.
  if (!stem->cursor_valid || from != stem->next_float_offset) {
    const off_t offset = (off_t)(from * sizeof(float));
    if (fseeko(stem->stream, offset, SEEK_SET) != 0) return 0;
    stats->explicit_seeks++;
  }
  if (fread(buffer, sizeof(float), count, stem->stream) != count) return 0;
  stem->next_float_offset = from + count;
  stem->cursor_valid = 1;
  stats->source_reads++;
  stats->source_bytes_read += count * sizeof(float);
  return 1;
}

int main(int argc, char **argv) {
  if (argc < 12 || sizeof(float) != 4 || FLT_RADIX != 2 || FLT_MANT_DIG != 24) {
    usage(); return 2;
  }
  const uint16_t one = 1;
  if (*(const unsigned char *)&one != 1) {
    fputs("f32le requires a little-endian host\n", stderr); return 2;
  }
  uint64_t samples = 0, channels = 0;
  const char *output = NULL;
  int sparse_silence = 0; // opt-in: use sparse file holes for silent blocks
  Stem stems[MAX_STEMS] = {{0}};
  size_t count = 0;
  RenderStats stats = {0};
  int valid = 1, result = 1;
  for (int i = 1; i < argc && valid;) {
    if (!strcmp(argv[i], "--samples") && i + 1 < argc) {
      valid = uint_arg(argv[i+1], MAX_SAMPLES, &samples); i += 2;
    } else if (!strcmp(argv[i], "--channels") && i + 1 < argc) {
      valid = uint_arg(argv[i+1], MAX_CHANNELS, &channels); i += 2;
    } else if (!strcmp(argv[i], "--output") && i + 1 < argc) {
      output = argv[i+1]; i += 2;
    } else if (!strcmp(argv[i], "--stem") && i + 5 < argc && count < MAX_STEMS) {
      Stem *s = &stems[count++];
      s->path = argv[i+1];
      valid = uint_arg(argv[i+2], MAX_SAMPLES, &s->at)
        && uint_arg(argv[i+3], MAX_SAMPLES, &s->from)
        && uint_arg(argv[i+4], MAX_SAMPLES, &s->frames)
        && gain_arg(argv[i+5], &s->gain);
      i += 6;
    } else if (!strcmp(argv[i], "--sparse-silence")) {
      sparse_silence = 1; i++;
    } else valid = 0;
  }
  if (!valid || !samples || !channels || !count || !output || !*output) {
    usage(); return 2;
  }

  // Validate every input and offset before creating a physical output.
  const uint64_t stride = channels * sizeof(float);
  for (size_t i = 0; i < count; ++i) {
    Stem *s = &stems[i];
    struct stat st;
    if (!s->frames || s->at + s->frames > samples ||
        stat(s->path, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
        (uint64_t)st.st_size % stride != 0 ||
        s->from + s->frames > (uint64_t)st.st_size / stride) {
      fprintf(stderr, "invalid stem boundary: index=%zu\n", i);
      goto done;
    }
    s->stream = fopen(s->path, "rb");
    if (!s->stream) { perror("open stem"); goto done; }
  }
  /* Pre-sort timeline events once, not on every 1024-frame output block.
   * Set bits for any stem entering the current block and clear stems whose
   * ending frame is at or before its start. Iterate set bits in ascending
   * original stem index to retain exact float32 accumulation order.
   * A stem that starts and finishes inside one block is still admitted. */
  StemEvent starts[MAX_STEMS], ends[MAX_STEMS];
  for (size_t i = 0; i < count; ++i) {
    starts[i] = (StemEvent){stems[i].at, (unsigned)i};
    ends[i] = (StemEvent){stems[i].at + stems[i].frames, (unsigned)i};
  }
  qsort(starts, count, sizeof(starts[0]), event_compare);
  qsort(ends, count, sizeof(ends[0]), event_compare);
  struct stat prior;
  if (lstat(output, &prior) == 0 || errno != ENOENT) {
    fputs("refusing to replace an existing output\n", stderr); goto done;
  }
  const size_t pathlen = strlen(output);
  if (pathlen > 4000) { fputs("output path too long\n", stderr); goto done; }
  char *temporary = malloc(pathlen + 16);
  if (!temporary) { fputs("out of memory\n", stderr); goto done; }
  sprintf(temporary, "%s.partial-XXXXXX", output);
  int fd = mkstemp(temporary);
  if (fd < 0) { perror("create output"); free(temporary); goto done; }
  FILE *destination = fdopen(fd, "wb");
  if (!destination) { close(fd); unlink(temporary); free(temporary); goto done; }
  const size_t work = BLOCK_SAMPLES * (size_t)channels;
  float *dst = calloc(work, sizeof(float)), *source = malloc(work * sizeof(float));
  if (!dst || !source) {
    fputs("out of memory\n", stderr); goto close_output;
  }
  uint64_t active_stems = 0, block_count = 0;
  uint64_t pending_zero_frames = 0, omitted_zero_frames = 0, output_data_bytes = 0;
  size_t start_event = 0, end_event = 0;
  for (uint64_t at = 0; at < samples; at += BLOCK_SAMPLES) {
    const size_t nframes = (size_t)((samples-at)<BLOCK_SAMPLES ? samples-at : BLOCK_SAMPLES);
    const uint64_t block_end = at + nframes;
    block_count++;
    while (end_event < count && ends[end_event].frame <= at) {
      active_stems &= ~(UINT64_C(1) << ends[end_event++].stem_index);
    }
    while (start_event < count && starts[start_event].frame < block_end) {
      active_stems |= UINT64_C(1) << starts[start_event++].stem_index;
    }
    if (sparse_silence && !active_stems) {
      // An unoccupied window is exactly all +0.0 f32le bytes. A file hole
      // reconstructs those bytes without materializing or writing them.
      // Keep a single pending run so many zero blocks need only one seek.
      pending_zero_frames += nframes;
      omitted_zero_frames += nframes;
      continue;
    }
    if (pending_zero_frames) {
      if (fseeko(destination, (off_t)(pending_zero_frames * stride), SEEK_CUR) != 0) {
        perror("seek silent region"); goto close_output;
      }
      pending_zero_frames = 0;
    }
    memset(dst, 0, nframes * (size_t)channels * sizeof(float));
    /* GCC/Clang ctz is defined here because candidates is never zero.
     * The ordering is the same ascending caller-provided index as before. */
    for (uint64_t candidates = active_stems; candidates; candidates &= (candidates - 1)) {
      const unsigned i = (unsigned)__builtin_ctzll(candidates);
      stats.overlap_probes++;
      Stem *s = &stems[i];
      const uint64_t lo = at > s->at ? at : s->at;
      const uint64_t hi = at + nframes < s->at + s->frames ? at + nframes : s->at + s->frames;
      if (lo >= hi) continue;
      const size_t bodyFloats = (size_t)(hi-lo) * (size_t)channels;
      if (!read_stem(s, (s->from + lo - s->at) * channels, source, bodyFloats, &stats)) {
        fputs("short or changed input body\n", stderr); goto close_output;
      }
      aurekai_mix_f32(dst + (lo-at)*channels, source, s->gain, bodyFloats);
    }
    for (size_t j = 0; j < nframes * (size_t)channels; ++j) {
      if (!isfinite(dst[j])) { fputs("nonfinite mixed sample\n", stderr); goto close_output; }
    }
    if (fwrite(dst, sizeof(float), nframes * (size_t)channels, destination) != nframes * (size_t)channels) {
      perror("write output"); goto close_output;
    }
    output_data_bytes += nframes * stride;
  }
  // A sparse file's trailing seek alone does not extend its length.
  // Write one explicit zero byte at the final logical offset instead of
  // calling ftruncate on the full body: macOS can otherwise account a full
  // allocation for the extension even when interior runs were skipped.
  // Existing output blocks always contain exact f32le bytes; this extra byte
  // belongs to a trailing +0.0 sample and is therefore byte-equivalent.
  if (sparse_silence && pending_zero_frames) {
    if (fseeko(destination, (off_t)(samples * stride - 1), SEEK_SET) != 0 ||
        fputc(0, destination) == EOF) {
      perror("extend trailing sparse silence"); goto close_output;
    }
  }
  if (fflush(destination) != 0 || fsync(fileno(destination)) != 0) {
    perror("flush output"); goto close_output;
  }
  if (fclose(destination) != 0) {
    destination = NULL; perror("close output"); goto close_output;
  }
  destination = NULL;
  // Atomically publish without replacing a previous output. This observation
  // still does not constitute an authority grant or an exact source digest.
  if (link(temporary, output) != 0) { perror("publish output"); goto close_output; }
  printf("{\"schema\":\"aurekai.creator.pcm-render.v1\",\"state\":\"materialized_unverified\","
         "\"sample_frames\":%" PRIu64 ",\"channels\":%" PRIu64 ",\"bytes\":%" PRIu64 ","
         "\"stems\":%zu,\"source_read_calls\":%" PRIu64 ","
         "\"source_explicit_seek_calls\":%" PRIu64 ","
         "\"sequential_seek_elisions\":%" PRIu64 ","
         "\"source_bytes_read\":%" PRIu64 ","
         "\"stem_overlap_probes\":%" PRIu64 ","
         "\"stem_overlap_probes_avoided\":%" PRIu64 ","
         "\"legacy_stem_overlap_probes\":%" PRIu64,
         samples, channels, samples * stride, count,
         stats.source_reads, stats.explicit_seeks,
         stats.source_reads - stats.explicit_seeks, stats.source_bytes_read,
         stats.overlap_probes, block_count * count - stats.overlap_probes,
         block_count * count);
  // Default JSON bytes remain identical. Sparse file semantics and counters
  // are visible only to callers that explicitly requested this representation.
  if (sparse_silence) {
    printf(",\"sparse_silence\":true,\"silent_zero_frames_elided\":%" PRIu64 ","
           "\"logical_silence_bytes_elided\":%" PRIu64 ","
           "\"output_fwrite_bytes\":%" PRIu64,
           omitted_zero_frames, omitted_zero_frames * stride, output_data_bytes);
  }
  puts(",\"execution_authority_inferred\":false}");
  result = 0;
close_output:
  free(dst); free(source);
  if (destination) fclose(destination);
  unlink(temporary); free(temporary);
done:
  for (size_t i = 0; i < count; ++i) if (stems[i].stream) fclose(stems[i].stream);
  return result;
}
