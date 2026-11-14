/* A bounded acoustic amplitude summary of the final *rendered* recording.
 * ffmpeg is already required by BonfyreBroadcast for the final mixdown.
 * Peaks are measured from decoded PCM, never generated from text or duration. */
#ifndef BONFYRE_BROADCAST_WAVEFORM_H
#define BONFYRE_BROADCAST_WAVEFORM_H
#define BF_AUDIO_WAVEFORM_BINS 64
#define BF_AUDIO_WAVEFORM_RATE 2000
typedef struct {
    double sum_squares[BF_AUDIO_WAVEFORM_BINS];
    uint64_t count[BF_AUDIO_WAVEFORM_BINS];
    uint64_t frames, expected;
} BfAudioWaveform;
static void bf_waveform_sample(BfAudioWaveform *w, int16_t sample) {
    if (!w || !w->expected) return;
    uint64_t bin = w->frames * BF_AUDIO_WAVEFORM_BINS / w->expected;
    if (bin >= BF_AUDIO_WAVEFORM_BINS) bin = BF_AUDIO_WAVEFORM_BINS - 1;
    double value = (double)sample / 32768.0;
    w->sum_squares[bin] += value * value;
    w->count[bin]++;
    w->frames++;
}
static int bf_waveform_string(const BfAudioWaveform *w, char *out, size_t cap) {
    if (!w || !w->frames || !out || cap < BF_AUDIO_WAVEFORM_BINS * 4) return 0;
    double rms[BF_AUDIO_WAVEFORM_BINS], highest = 0;
    for (int i = 0; i < BF_AUDIO_WAVEFORM_BINS; i++) {
        rms[i] = w->count[i] ? sqrt(w->sum_squares[i] / (double)w->count[i]) : 0;
        if (rms[i] > highest) highest = rms[i];
    }
    size_t pos = 0;
    for (int i = 0; i < BF_AUDIO_WAVEFORM_BINS; i++) {
        int peak = highest > 1e-7 ? (int)lround(rms[i] / highest * 100) : 0;
        if (peak > 100) peak = 100;
        int n = snprintf(out + pos, cap - pos, "%s%d", i ? "," : "", peak);
        if (n <= 0 || (size_t)n >= cap - pos) { out[0] = 0; return 0; }
        pos += (size_t)n;
    }
    return 1;
}
static int bf_waveform_valid(const char *data) {
    if (!data || !*data || strlen(data) > BF_AUDIO_WAVEFORM_BINS * 4) return 0;
    for (int i = 0; i < BF_AUDIO_WAVEFORM_BINS; i++) {
        if (*data < '0' || *data > '9') return 0;
        int peak = 0, digits = 0;
        while (*data >= '0' && *data <= '9' && digits < 4) {
            peak = peak * 10 + (*data++ - '0'); digits++;
        }
        if (digits > 3 || peak > 100) return 0;
        if (i + 1 == BF_AUDIO_WAVEFORM_BINS) return *data == 0;
        if (*data++ != ',') return 0;
    }
    return 0;
}
static int bf_waveform_from_audio(const char *file, double seconds, char *out, size_t cap) {
    if (!file || !*file || !isfinite(seconds) || seconds < 0.1 || seconds > 8 * 3600 ||
        !out || cap < BF_AUDIO_WAVEFORM_BINS * 4) return 0;
    out[0] = 0;
    int pipes[2];
    if (pipe(pipes) != 0) return 0;
    pid_t child = fork();
    if (child < 0) { close(pipes[0]); close(pipes[1]); return 0; }
    if (child == 0) {
        close(pipes[0]);
        if (dup2(pipes[1], STDOUT_FILENO) < 0) _exit(127);
        close(pipes[1]);
        int null = open("/dev/null", O_RDWR);
        if (null >= 0) { dup2(null, STDERR_FILENO); dup2(null, STDIN_FILENO); close(null); }
        execlp("ffmpeg", "ffmpeg", "-nostdin", "-v", "error", "-i", file, "-vn",
               "-ac", "1", "-ar", "2000", "-f", "s16le", "pipe:1", (char *)NULL);
        _exit(127);
    }
    close(pipes[1]);
    BfAudioWaveform waveform = {0};
    waveform.expected = (uint64_t)ceil(seconds * BF_AUDIO_WAVEFORM_RATE);
    unsigned char buffer[8192];
    int previous = -1, bad = 0;
    ssize_t bytes;
    while ((bytes = read(pipes[0], buffer, sizeof(buffer))) > 0) {
        size_t j = 0;
        if (previous >= 0) {
            bf_waveform_sample(&waveform, (int16_t)((unsigned)previous | ((unsigned)buffer[j++] << 8)));
            previous = -1;
        }
        while (j + 1 < (size_t)bytes) {
            uint16_t value = (uint16_t)((unsigned)buffer[j] | ((unsigned)buffer[j + 1] << 8));
            bf_waveform_sample(&waveform, (int16_t)value);
            j += 2;
        }
        if (j < (size_t)bytes) previous = buffer[j];
    }
    if (bytes < 0) bad = 1;
    close(pipes[0]);
    int status = 0;
    if (waitpid(child, &status, 0) != child || !WIFEXITED(status) ||
        WEXITSTATUS(status) != 0 || bad || previous >= 0) return 0;
    return bf_waveform_string(&waveform, out, cap);
}
#endif
