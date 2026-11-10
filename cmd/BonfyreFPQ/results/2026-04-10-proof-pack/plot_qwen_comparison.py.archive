#!/usr/bin/env python3
import csv
from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parent
CSV_PATH = ROOT / "qwen_comparison.csv"
PNG_PATH = ROOT / "qwen_comparison.png"


def load_rows():
    with CSV_PATH.open() as f:
        return list(csv.DictReader(f))


def main():
    rows = load_rows()
    methods = [r["method"] for r in rows]
    degrad = [float(r["ppl_degradation_pct"]) for r in rows]
    quant_ppl = [float(r["quantized_ppl"]) for r in rows]

    colors = []
    for m in methods:
        if m.startswith("v8"):
            colors.append("#0a7f5a")
        elif m.startswith("FP32"):
            colors.append("#5f6b7a")
        elif m.startswith("HQQ"):
            colors.append("#c46b00")
        else:
            colors.append("#b03a2e")

    plt.rcParams.update({
        "figure.figsize": (11, 6.5),
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.facecolor": "#fbfbf7",
        "figure.facecolor": "#f4f1ea",
        "font.size": 11,
    })

    fig, (ax1, ax2) = plt.subplots(1, 2, gridspec_kw={"width_ratios": [1.1, 1.3]})

    ax1.bar(methods, quant_ppl, color=colors)
    ax1.set_title("Qwen 0.5B PPL")
    ax1.set_ylabel("Perplexity")
    ax1.tick_params(axis="x", rotation=35)
    for i, val in enumerate(quant_ppl):
        ax1.text(i, val + 0.6, f"{val:.2f}", ha="center", va="bottom", fontsize=10)

    ax2.bar(methods, degrad, color=colors)
    ax2.axhline(0, color="#333333", linewidth=1)
    ax2.set_title("PPL Degradation vs FP32")
    ax2.set_ylabel("Percent")
    ax2.tick_params(axis="x", rotation=35)
    for i, val in enumerate(degrad):
        ax2.text(i, val + (2 if val >= 0 else -2), f"{val:.2f}%", ha="center", va="bottom", fontsize=10)

    fig.suptitle("Qwen 2.5 0.5B, WikiText-2 Slice, 994 Tokens", fontsize=14, fontweight="bold")
    fig.text(
        0.5,
        0.01,
        "v8 RLF remains near-lossless on this slice while v4 COORD and HQQ 3-bit runs degrade sharply.",
        ha="center",
        fontsize=10,
    )
    fig.tight_layout(rect=[0, 0.04, 1, 0.95])
    fig.savefig(PNG_PATH, dpi=180)
    print(PNG_PATH)


if __name__ == "__main__":
    main()
