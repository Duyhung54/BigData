"""Sinh biểu đồ cho báo cáo từ các file CSV kết quả.

Dùng: python3 report/make_figures.py
"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")           # không có màn hình trong container
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd              # noqa: E402

RESULTS_DIR = Path(__file__).parent / "results"
FIGURES_DIR = Path(__file__).parent / "figures"

# Bảng màu categorical đã qua kiểm tra (dataviz skill validator):
# lightness band PASS, chroma floor PASS, CVD separation PASS (ΔE >= 8),
# normal-vision floor PASS. Gán theo thứ tự cố định, không xoay vòng.
# Cảnh báo contrast (WARN < 3:1 với nền cho 3/5 màu) đã được xử lý bằng
# cách LUÔN đi kèm chú giải (legend) + nhãn trực tiếp trên chuỗi dữ liệu
# quan trọng, không bao giờ để nhận diện phụ thuộc riêng vào màu sắc.
PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]  # xanh dương, cam, ngọc, vàng, hồng

SURFACE = "#fcfcfb"   # nền figure và axes
INK = "#0b0b0b"       # tiêu đề
INK_2 = "#52514e"     # nhãn trục
MUTED = "#898781"     # nhãn tick, đường tham chiếu trung tính
GRID = "#e1e0d9"      # đường lưới — mờ, mảnh
BASELINE = "#c3c2b7"  # trục (spine)

LINEWIDTH = 2
MARKERSIZE = 8


def _style_axes(ax) -> None:
    """Áp token màu ink/surface dùng chung, xoá spine trên/phải, làm mờ lưới."""
    ax.set_facecolor(SURFACE)
    ax.figure.set_facecolor(SURFACE)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(BASELINE)
    ax.spines["bottom"].set_color(BASELINE)
    ax.tick_params(colors=MUTED, labelsize=9)
    ax.xaxis.label.set_color(INK_2)
    ax.yaxis.label.set_color(INK_2)
    ax.title.set_color(INK)
    ax.title.set_fontsize(12)
    ax.set_axisbelow(True)
    ax.grid(True, color=GRID, linewidth=0.8)


def _fmt_small(value: float) -> str:
    """Định dạng số nhỏ (metric xếp hạng) sao cho vẫn đọc được, không làm tròn về 0."""
    if pd.isna(value):
        return ""
    if value == 0:
        return "0.0000"
    if abs(value) < 0.0001:
        return f"{value:.0e}"
    return f"{value:.4f}"


def plot_tuning(tuning_csv: Path, out_png: Path) -> None:
    """RMSE theo rank, mỗi regParam một đường.

    Thông điệp: regParam quyết định RMSE (đường cong chữ U, đáy tại 0.1),
    còn rank gần như không ảnh hưởng (ba đường của regParam=0.1 gần như
    phẳng quanh 0.815–0.818).
    """
    df = pd.read_csv(tuning_csv)
    reg_values = sorted(df["regParam"].unique())

    fig, ax = plt.subplots(figsize=(7.5, 5))

    winner = df.loc[df["rmse"].idxmin()]

    for color, reg in zip(PALETTE, reg_values):
        group = df[df["regParam"] == reg].sort_values("rank")
        ax.plot(
            group["rank"],
            group["rmse"],
            marker="o",
            linewidth=LINEWIDTH,
            markersize=MARKERSIZE,
            color=color,
            label=f"regParam = {reg}",
        )

    # Nhãn trực tiếp trên điểm tốt nhất (chuỗi quan trọng nhất của biểu đồ).
    ax.annotate(
        f"Tốt nhất: rank={int(winner['rank'])}, regParam={winner['regParam']}\n"
        f"RMSE = {winner['rmse']:.5f}",
        xy=(winner["rank"], winner["rmse"]),
        xytext=(winner["rank"] + 28, winner["rmse"] + 0.115),
        fontsize=9,
        color=INK,
        ha="left",
        arrowprops=dict(arrowstyle="->", color=INK_2, linewidth=1),
    )

    ax.set_xticks(sorted(df["rank"].unique()))
    ax.set_xlabel("rank (số chiều ẩn)")
    ax.set_ylabel("RMSE trên tập validation")
    ax.set_title("regParam quyết định RMSE; rank gần như không ảnh hưởng")
    # Chú giải đặt HẲN bên ngoài vùng dữ liệu (dưới trục x) — đặt trong plot
    # (dù ở góc) từng che mất điểm rank=10 của đường regParam=0.5. Không thu
    # nhỏ font, không nới ylim để né dữ liệu — cả hai đều làm sai lệch cách
    # đọc biểu đồ.
    ax.legend(
        frameon=False,
        labelcolor=INK_2,
        fontsize=9,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.14),
        ncol=3,
    )
    _style_axes(ax)

    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_scaling(scaling_csv: Path, out_png: Path) -> None:
    """Speedup theo tổng số core, kèm đường tuyến tính lý tưởng để đối chiếu.

    Thông điệp: thêm worker KHÔNG giúp nhanh hơn — 8 core còn chậm hơn 2 core.
    Vẽ cả từng lần chạy riêng lẻ (để lộ độ lệch 62% ở mức 2 core) lẫn trung
    bình, để không che giấu độ nhiễu của phép đo.
    """
    df = pd.read_csv(scaling_csv)
    baseline_cores = df["total_cores"].min()
    baseline_seconds = df.loc[df["total_cores"] == baseline_cores, "seconds"].mean()

    df = df.copy()
    df["speedup"] = baseline_seconds / df["seconds"]

    mean_seconds = df.groupby("total_cores")["seconds"].mean().sort_index()
    mean_speedup = baseline_seconds / mean_seconds
    cores_sorted = mean_seconds.index
    ideal = cores_sorted / baseline_cores

    fig, ax = plt.subplots(figsize=(7.5, 5))

    # Đường tuyến tính lý tưởng — màu trung tính, chỉ để đối chiếu.
    ax.plot(
        cores_sorted,
        ideal,
        linestyle="--",
        linewidth=LINEWIDTH,
        color=MUTED,
        zorder=1,
        label="Tuyến tính lý tưởng",
    )

    # Từng lần chạy riêng lẻ — làm mờ, để lộ độ nhiễu của phép đo.
    ax.scatter(
        df["total_cores"],
        df["speedup"],
        s=MARKERSIZE ** 2,
        color=PALETTE[0],
        alpha=0.35,
        zorder=2,
        label="Từng lần chạy",
    )

    # Speedup thực đo (trung bình) — chuỗi quan trọng nhất của biểu đồ.
    ax.plot(
        cores_sorted,
        mean_speedup,
        marker="o",
        linewidth=LINEWIDTH,
        markersize=MARKERSIZE,
        color=PALETTE[0],
        zorder=3,
        label="Speedup thực đo (trung bình)",
    )

    last_core = cores_sorted[-1]
    last_speedup = mean_speedup.iloc[-1]
    ax.annotate(
        f"{last_speedup:.2f}× ở {int(last_core)} core\n(chậm hơn cả {int(baseline_cores)} core)",
        xy=(last_core, last_speedup),
        xytext=(last_core - 2.6, last_speedup + 0.65),
        fontsize=9,
        color=INK,
        ha="left",
        arrowprops=dict(arrowstyle="->", color=INK_2, linewidth=1),
    )

    ax.set_xticks(sorted(df["total_cores"].unique()))
    ax.set_xlabel("Tổng số core")
    ax.set_ylabel(f"Speedup (so với {int(baseline_cores)} core)")
    ax.set_title("Số core tăng nhưng speedup không tăng tương ứng")
    ax.legend(frameon=False, labelcolor=INK_2, fontsize=9, loc="upper left")
    _style_axes(ax)

    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def plot_baseline_comparison(metrics_csv: Path, out_png: Path) -> None:
    """So sánh ALS với baseline trên các metric xếp hạng.

    Thông điệp: als_unfiltered (chưa lọc phim ít lượt đánh giá) gần như bằng 0;
    als (đã lọc theo MIN_RATINGS_FOR_RECOMMENDATION, một ngưỡng hỗ trợ tối
    thiểu trên PHIM, không phải "cold-start" — thuật ngữ đó ở project này
    dành riêng cho user ít rating) khá hơn hẳn nhưng vẫn thua popularity.
    Chỉ vẽ các model có metric xếp hạng — global_mean/item_mean không có
    nên bị loại bỏ hoàn toàn (không suy diễn thành 0).
    """
    df = pd.read_csv(metrics_csv).set_index("model")
    columns = ["precision_at_k", "recall_at_k", "ndcg_at_k"]
    labels = ["Precision@10", "Recall@10", "NDCG@10"]
    subset = df[columns].dropna(how="all")

    # Thứ tự cố định kể câu chuyện: chưa lọc -> đã lọc -> baseline không cá nhân hoá.
    preferred_order = ["als_unfiltered", "als", "popularity"]
    order = [m for m in preferred_order if m in subset.index]
    order += [m for m in subset.index if m not in order]
    subset = subset.loc[order]

    fig, ax = plt.subplots(figsize=(7.5, 5))
    n_models = len(subset)
    width = 0.8 / n_models
    positions = list(range(len(columns)))

    for i, (model, row) in enumerate(subset.iterrows()):
        color = PALETTE[i % len(PALETTE)]
        xs = [p + i * width for p in positions]
        values = row[columns].astype(float).values
        ax.bar(xs, values, width=width * 0.9, color=color, label=model, zorder=2)
        for x, v in zip(xs, values):
            ax.annotate(
                _fmt_small(v),
                xy=(x, v),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=7.5,
                color=INK,
            )

    ax.set_xticks([p + width * (n_models - 1) / 2 for p in positions])
    ax.set_xticklabels(labels)
    ax.set_ylabel("Giá trị")
    # Xuống dòng: bản một dòng bị cắt ở lề phải figure (title dài hơn 7.5in ở
    # fontsize tiêu đề). Không thu nhỏ font tiêu đề để né — xuống dòng thay.
    ax.set_title(
        "Lọc phim ít lượt đánh giá cải thiện ALS hàng trăm lần,\n"
        "nhưng popularity vẫn dẫn đầu"
    )
    ax.legend(frameon=False, labelcolor=INK_2, fontsize=9, loc="upper left")
    _style_axes(ax)
    ax.set_ylim(0, ax.get_ylim()[1] * 1.15)  # chừa chỗ cho nhãn trên đỉnh cột

    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    jobs = [
        (plot_tuning, "tuning.csv", "tuning.png"),
        (plot_scaling, "scaling.csv", "scaling.png"),
        (plot_baseline_comparison, "metrics.csv", "baselines.png"),
    ]
    for plot_fn, source, target in jobs:
        csv_path = RESULTS_DIR / source
        if not csv_path.exists():
            print(f"Bỏ qua {target}: chưa có {csv_path}")
            continue
        plot_fn(csv_path, FIGURES_DIR / target)
        print(f"Đã tạo {FIGURES_DIR / target}")


if __name__ == "__main__":
    main()
