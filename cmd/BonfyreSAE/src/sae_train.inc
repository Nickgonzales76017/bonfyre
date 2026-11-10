/* ── train ────────────────────────────────────────────────────────────────── */
/* Included by main.c. Kept in its own file so the fitting code can be read
 * apart from the runtime commands. */

static const char *train_opt(int argc, char **argv, const char *name) {
    for (int i = 0; i + 1 < argc; i++) if (strcmp(argv[i], name) == 0) return argv[i + 1];
    return NULL;
}

static uint16_t train_f32_to_f16(float v) {
    uint32_t bits; memcpy(&bits, &v, 4);
    uint32_t sign = (bits >> 31) & 1;
    int32_t  exp  = (int32_t)((bits >> 23) & 0xff) - 127 + 15;
    uint32_t mant = (bits >> 13) & 0x3ff;
    if (exp <= 0)  return (uint16_t)(sign << 15);
    if (exp >= 31) return (uint16_t)((sign << 15) | 0x7c00);
    return (uint16_t)((sign << 15) | ((uint32_t)exp << 10) | mant);
}

/* Trains a sparse autoencoder on recorded activations and writes it as a
 * .bfsae. The activations are float32 vectors of the model's hidden size, as
 * a model run wrote them. One vector in ten is held out and never trained on;
 * the figures printed at the end are measured on those.
 *
 *   f = ReLU(E x + b)        x' = D f        loss = |x - x'|^2 + l1 * sum(f)
 *
 * Features start from training vectors, the decoder's columns are kept at
 * unit length, and the encoder and decoder are fitted by Adam. No feature is
 * given a meaning here: every tag is left empty, because what a feature
 * responds to has to be found by looking, and nothing has looked. */
static int cmd_train(int argc, char **argv) {
    const char *acts_path = train_opt(argc, argv, "--activations");
    const char *out_path = train_opt(argc, argv, "--out");
    const char *family = train_opt(argc, argv, "--family");
    const char *v;
    int dim = (v = train_opt(argc, argv, "--dim")) ? atoi(v) : 0;
    int features = (v = train_opt(argc, argv, "--features")) ? atoi(v) : 0;
    int layer = (v = train_opt(argc, argv, "--layer")) ? atoi(v) : -1;
    int epochs = (v = train_opt(argc, argv, "--epochs")) ? atoi(v) : 200;
    float l1 = (v = train_opt(argc, argv, "--l1")) ? (float)atof(v) : 0.02f;
    if (!acts_path || !out_path || !family || dim <= 0 || features <= 0 || layer < 0 || epochs <= 0) {
        fprintf(stderr, "usage: bonfyre-sae train --activations acts.f32 --dim D --features N --layer L\n"
                        "                         --family MODEL --out dict.bfsae [--epochs E] [--l1 X]\n");
        return 1;
    }

    FILE *in = fopen(acts_path, "rb");
    if (!in) { fprintf(stderr, "train: cannot open %s\n", acts_path); return 1; }
    fseek(in, 0, SEEK_END);
    long bytes = ftell(in);
    fseek(in, 0, SEEK_SET);
    size_t D = (size_t)dim, N = (size_t)features;
    if (bytes <= 0 || (size_t)bytes % (D * sizeof(float)) != 0) {
        fprintf(stderr, "train: %s is not a whole number of %d-float vectors\n", acts_path, dim);
        fclose(in); return 1;
    }
    size_t M = (size_t)bytes / (D * sizeof(float));
    float *X = (float *)malloc((size_t)bytes);
    if (!X || fread(X, sizeof(float), M * D, in) != M * D) {
        fprintf(stderr, "train: cannot read %s\n", acts_path); fclose(in); free(X); return 1;
    }
    fclose(in);
    size_t held = M / 10, train_n = M - held;
    if (held < 4 || train_n < N * 2) {
        fprintf(stderr, "train: %zu vectors is too few for %d features; at least %zu are needed\n",
                M, features, (N * 2 * 10 + 8) / 9);
        free(X); return 1;
    }

    /* Work in units of the mean vector length, and fold that back at the end. */
    double norm_sum = 0.0;
    for (size_t m = 0; m < M; m++) {
        double s = 0.0;
        for (size_t j = 0; j < D; j++) s += (double)X[m * D + j] * X[m * D + j];
        norm_sum += sqrt(s);
    }
    float unit = (float)(norm_sum / (double)M);
    if (!(unit > 0.0f)) { fprintf(stderr, "train: the activations are all zero\n"); free(X); return 1; }
    for (size_t i = 0; i < M * D; i++) X[i] /= unit;

    /* Every tenth vector is held out. */
    size_t *train_idx = (size_t *)malloc(train_n * sizeof(size_t));
    size_t *held_idx = (size_t *)malloc(held * sizeof(size_t));
    size_t tn = 0, hn = 0;
    for (size_t m = 0; train_idx && held_idx && m < M; m++) {
        if (m % 10 == 9 && hn < held) held_idx[hn++] = m; else if (tn < train_n) train_idx[tn++] = m;
    }
    train_n = tn; held = hn;

    float *E = (float *)calloc(N * D, sizeof(float)), *Dm = (float *)calloc(N * D, sizeof(float));
    float *b = (float *)calloc(N, sizeof(float));
    float *mE = (float *)calloc(N * D, sizeof(float)), *vE = (float *)calloc(N * D, sizeof(float));
    float *mD = (float *)calloc(N * D, sizeof(float)), *vD = (float *)calloc(N * D, sizeof(float));
    float *mb = (float *)calloc(N, sizeof(float)), *vb = (float *)calloc(N, sizeof(float));
    float *f = (float *)calloc(N, sizeof(float)), *err = (float *)calloc(D, sizeof(float));
    float *gf = (float *)calloc(N, sizeof(float));
    if (!E || !Dm || !b || !mE || !vE || !mD || !vD || !mb || !vb || !f || !err || !gf || !train_idx || !held_idx) {
        fprintf(stderr, "train: out of memory\n"); return 1;
    }
    /* Dm is held feature-major here ([N][D]) and written out as [D][N]. */
    for (size_t i = 0; i < N; i++) {
        const float *x = X + train_idx[(i * train_n) / N] * D;
        double s = 0.0;
        for (size_t j = 0; j < D; j++) s += (double)x[j] * x[j];
        float inv = s > 0.0 ? (float)(1.0 / sqrt(s)) : 0.0f;
        for (size_t j = 0; j < D; j++) { Dm[i * D + j] = x[j] * inv; E[i * D + j] = x[j] * inv; }
    }

    const float lr = 0.002f, b1 = 0.9f, b2 = 0.999f, eps = 1e-8f;
    long step = 0;
    for (int epoch = 0; epoch < epochs; epoch++) {
        double epoch_loss = 0.0;
        for (size_t t = 0; t < train_n; t++) {
            /* A fixed stride through the training vectors. */
            const float *x = X + train_idx[(t * 7919 + (size_t)epoch * 104729) % train_n] * D;
            for (size_t i = 0; i < N; i++) {
                float a = b[i];
                const float *row = E + i * D;
                for (size_t j = 0; j < D; j++) a += row[j] * x[j];
                f[i] = a > 0.0f ? a : 0.0f;
            }
            for (size_t j = 0; j < D; j++) err[j] = -x[j];
            for (size_t i = 0; i < N; i++) if (f[i] > 0.0f) {
                const float *col = Dm + i * D;
                for (size_t j = 0; j < D; j++) err[j] += f[i] * col[j];
            }
            for (size_t j = 0; j < D; j++) epoch_loss += (double)err[j] * err[j];
            step++;
            float c1 = 1.0f - powf(b1, (float)step), c2 = 1.0f - powf(b2, (float)step);
            for (size_t i = 0; i < N; i++) {
                float *col = Dm + i * D, *row = E + i * D;
                float g = 0.0f;
                for (size_t j = 0; j < D; j++) g += 2.0f * err[j] * col[j];
                gf[i] = f[i] > 0.0f ? g + l1 : 0.0f;
                if (f[i] > 0.0f) {
                    float *m1 = mD + i * D, *m2 = vD + i * D;
                    for (size_t j = 0; j < D; j++) {
                        float gd = 2.0f * err[j] * f[i];
                        m1[j] = b1 * m1[j] + (1 - b1) * gd; m2[j] = b2 * m2[j] + (1 - b2) * gd * gd;
                        col[j] -= lr * (m1[j] / c1) / (sqrtf(m2[j] / c2) + eps);
                    }
                    double s = 0.0;
                    for (size_t j = 0; j < D; j++) s += (double)col[j] * col[j];
                    float inv = s > 0.0 ? (float)(1.0 / sqrt(s)) : 1.0f;
                    for (size_t j = 0; j < D; j++) col[j] *= inv;
                }
                if (gf[i] != 0.0f) {
                    float *m1 = mE + i * D, *m2 = vE + i * D;
                    for (size_t j = 0; j < D; j++) {
                        float ge = gf[i] * x[j];
                        m1[j] = b1 * m1[j] + (1 - b1) * ge; m2[j] = b2 * m2[j] + (1 - b2) * ge * ge;
                        row[j] -= lr * (m1[j] / c1) / (sqrtf(m2[j] / c2) + eps);
                    }
                    mb[i] = b1 * mb[i] + (1 - b1) * gf[i]; vb[i] = b2 * vb[i] + (1 - b2) * gf[i] * gf[i];
                    b[i] -= lr * (mb[i] / c1) / (sqrtf(vb[i] / c2) + eps);
                }
            }
        }
        if (epoch == 0 || epoch + 1 == epochs || (epoch + 1) % 50 == 0)
            fprintf(stderr, "train: epoch %d  mean squared error %.6f\n", epoch + 1, epoch_loss / (double)(train_n * D));
    }

    /* Measured on the vectors that were held out. */
    double *mean = (double *)calloc(D, sizeof(double));
    for (size_t t = 0; t < train_n; t++) for (size_t j = 0; j < D; j++) mean[j] += X[train_idx[t] * D + j];
    for (size_t j = 0; j < D; j++) mean[j] /= (double)train_n;
    double sse = 0.0, sst = 0.0, cos_sum = 0.0, l0_sum = 0.0;
    for (size_t h = 0; h < held; h++) {
        const float *x = X + held_idx[h] * D;
        double dot = 0.0, xx = 0.0, rr = 0.0;
        for (size_t j = 0; j < D; j++) err[j] = 0.0f;
        for (size_t i = 0; i < N; i++) {
            float a = b[i];
            for (size_t j = 0; j < D; j++) a += E[i * D + j] * x[j];
            if (a > 0.0f) { l0_sum += 1.0; for (size_t j = 0; j < D; j++) err[j] += a * Dm[i * D + j]; }
        }
        for (size_t j = 0; j < D; j++) {
            double r = err[j], xv = x[j];
            sse += (xv - r) * (xv - r); sst += (xv - mean[j]) * (xv - mean[j]);
            dot += xv * r; xx += xv * xv; rr += r * r;
        }
        cos_sum += dot / (sqrt(xx) * sqrt(rr) + 1e-20);
    }
    double explained = sst > 0.0 ? 1.0 - sse / sst : 0.0;

    /* Per-feature statistics over the training vectors, in the model's units. */
    BfsaeFeatureMeta *meta = (BfsaeFeatureMeta *)calloc(N, sizeof(BfsaeFeatureMeta));
    size_t alive = 0;
    for (size_t i = 0; i < N; i++) {
        double s = 0.0, s2 = 0.0;
        for (size_t t = 0; t < train_n; t++) {
            const float *x = X + train_idx[t] * D;
            float a = b[i];
            for (size_t j = 0; j < D; j++) a += E[i * D + j] * x[j];
            if (a < 0.0f) a = 0.0f;
            s += a; s2 += (double)a * a;
        }
        double mu = s / (double)train_n, var = s2 / (double)train_n - mu * mu;
        meta[i].feature_id = (uint32_t)i;
        meta[i].bias = b[i];
        meta[i].mean_activation = (float)mu;
        meta[i].std_activation = (float)sqrt(var > 0.0 ? var : 0.0);
        meta[i].tags = BFSAE_TAG_NONE;
        if (mu > 0.0) alive++;
    }

    BfsaeHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    memcpy(hdr.magic, BFSAE_MAGIC, BFSAE_MAGIC_LEN);
    hdr.version = BFSAE_VERSION;
    hdr.site = BFSAE_SITE_RESIDUAL;
    hdr.dtype = BFSAE_DTYPE_FP16;
    hdr.layer = (uint32_t)layer;
    hdr.hidden_dim = (uint32_t)D;
    hdr.feature_count = (uint32_t)N;
    hdr.meta_offset = sizeof(BfsaeHeader);
    hdr.encoder_offset = hdr.meta_offset + N * sizeof(BfsaeFeatureMeta);
    hdr.decoder_offset = hdr.encoder_offset + N * D * sizeof(uint16_t);
    hdr.file_size = hdr.decoder_offset + N * D * sizeof(uint16_t);
    snprintf(hdr.model_family, sizeof(hdr.model_family), "%s", family);
    snprintf(hdr.training_notes, sizeof(hdr.training_notes), "trained n=%zu l1=%g", train_n, (double)l1);

    FILE *out = fopen(out_path, "wb");
    int ok = out != NULL;
    if (ok) ok = fwrite(&hdr, sizeof(hdr), 1, out) == 1 && fwrite(meta, sizeof(BfsaeFeatureMeta), N, out) == N;
    /* The encoder takes the model's own units: E' = E / unit. */
    for (size_t i = 0; ok && i < N; i++) for (size_t j = 0; ok && j < D; j++) {
        uint16_t h16 = train_f32_to_f16(E[i * D + j] / unit);
        ok = fwrite(&h16, sizeof(h16), 1, out) == 1;
    }
    /* The decoder returns them: D' = D * unit, laid out [D][N]. */
    for (size_t j = 0; ok && j < D; j++) for (size_t i = 0; ok && i < N; i++) {
        uint16_t h16 = train_f32_to_f16(Dm[i * D + j] * unit);
        ok = fwrite(&h16, sizeof(h16), 1, out) == 1;
    }
    if (out && fclose(out) != 0) ok = 0;
    if (!ok) { fprintf(stderr, "train: cannot write %s\n", out_path); remove(out_path); return 1; }

    printf("{\"kind\":\"sae-train\",\"dictionary\":\"%s\",\"model_family\":\"%s\",\"layer\":%d,\"hidden_dim\":%d,"
           "\"features\":%d,\"features_that_fire\":%zu,\"vectors\":%zu,\"trained_on\":%zu,\"held_out\":%zu,"
           "\"held_out_variance_explained\":%.4f,\"held_out_mean_cosine\":%.4f,\"held_out_mean_active_features\":%.2f,"
           "\"labelled_features\":0}\n",
           out_path, family, layer, dim, features, alive, M, train_n, held,
           explained, cos_sum / (double)held, l0_sum / (double)held);
    free(X); free(E); free(Dm); free(b); free(mE); free(vE); free(mD); free(vD); free(mb); free(vb);
    free(f); free(err); free(gf); free(train_idx); free(held_idx); free(mean); free(meta);
    return 0;
}
