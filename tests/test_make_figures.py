import pandas as pd

from report.make_figures import plot_baseline_comparison, plot_scaling, plot_tuning


def test_plot_tuning_writes_png(tmp_path):
    csv = tmp_path / "tuning.csv"
    pd.DataFrame({
        "rank": [10, 50, 10, 50],
        "regParam": [0.1, 0.1, 0.2, 0.2],
        "maxIter": [10] * 4,
        "rmse": [0.85, 0.82, 0.88, 0.84],
        "fit_seconds": [10, 20, 10, 20],
    }).to_csv(csv, index=False)
    out = tmp_path / "tuning.png"

    plot_tuning(csv, out)

    assert out.exists() and out.stat().st_size > 0


def test_plot_scaling_writes_png(tmp_path):
    csv = tmp_path / "scaling.csv"
    pd.DataFrame({
        "n_workers": [1, 1, 2, 2, 4, 4],
        "total_cores": [2, 2, 4, 4, 8, 8],
        "run": [1, 2, 1, 2, 1, 2],
        "seconds": [400, 410, 250, 245, 180, 185],
    }).to_csv(csv, index=False)
    out = tmp_path / "scaling.png"

    plot_scaling(csv, out)

    assert out.exists() and out.stat().st_size > 0


def test_plot_baseline_comparison_writes_png(tmp_path):
    csv = tmp_path / "metrics.csv"
    pd.DataFrame({
        "model": ["als", "global_mean", "item_mean", "popularity"],
        "rmse": [0.81, 1.05, 0.95, None],
        "mae": [0.62, 0.84, 0.74, None],
        "precision_at_k": [0.09, None, None, 0.06],
        "recall_at_k": [0.11, None, None, 0.07],
        "ndcg_at_k": [0.13, None, None, 0.08],
        "coverage": [0.42, None, None, 0.001],
    }).to_csv(csv, index=False)
    out = tmp_path / "baselines.png"

    plot_baseline_comparison(csv, out)

    assert out.exists() and out.stat().st_size > 0
