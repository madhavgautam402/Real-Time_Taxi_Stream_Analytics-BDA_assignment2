import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
from common.config import ROOT

fig, ax = plt.subplots(figsize=(16, 6.5))
ax.set_xlim(0, 16)
ax.set_ylim(0, 6.5)
ax.axis("off")

def box(x, y, w, h, text, color):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.05,rounding_size=0.12",
                                fc=color, ec="#333333", lw=1.4))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=9)

def arrow(x1, y1, x2, y2):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="->", lw=1.6, color="#333333"))

GREEN, BLUE, YELLOW, PINK = "#D9F2D0", "#BFE0FF", "#FFE9A8", "#F5D0E6"

box(0.1, 2.5, 1.8, 1.3, "Producer\nreplay taxi data\n+2% duplicates\n+2% delayed", GREEN)
box(2.4, 2.75, 1.5, 0.8, "taxi.raw\n4 partitions\nkey=pickup_zone", YELLOW)
box(4.4, 2.25, 2.0, 1.8, "Stage 1\nclean + enrich\nBloom filter\n(mark, not drop)", BLUE)
box(6.9, 2.75, 1.5, 0.8, "taxi.clean\n4 partitions", YELLOW)
box(8.9, 4.0, 2.1, 1.2, "Stage 2: analytics\nreservoir, HLL,\nDGIM, sampling", BLUE)
box(8.9, 1.1, 2.1, 1.2, "Stage 3: similarity\nMinHash-LSH\n(20x5 bands)", BLUE)
box(11.5, 4.2, 1.5, 0.8, "taxi.metrics\n1 partition", YELLOW)
box(11.5, 1.3, 1.5, 0.8, "taxi.lsh\n1 partition", YELLOW)
box(13.5, 2.45, 2.3, 1.4, "Final consumer\nmetrics + lsh\nresults.jsonl\n+ console", PINK)

arrow(1.9, 3.15, 2.4, 3.15)
arrow(3.9, 3.15, 4.4, 3.15)
arrow(6.4, 3.15, 6.9, 3.15)
arrow(8.4, 3.15, 8.9, 4.6)
arrow(8.4, 3.15, 8.9, 1.7)
arrow(11.0, 4.6, 11.5, 4.6)
arrow(11.0, 1.7, 11.5, 1.7)
arrow(13.0, 4.6, 13.5, 3.6)
arrow(13.0, 1.7, 13.5, 2.7)

ax.text(8, 6.1, "Real-Time Taxi Stream Analytics: every stage reads from and writes to Kafka",
        ha="center", fontsize=13, fontweight="bold")
ax.text(8, 0.35, "Stages: Spark Structured Streaming (local[N] slots)   |   Kafka: Docker, KRaft, single broker",
        ha="center", fontsize=9, color="#444444")

out = ROOT / "architecture.png"
fig.savefig(out, dpi=150, bbox_inches="tight")
print("saved", out)