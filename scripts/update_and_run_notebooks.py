"""
Update N02 and N03 to work with the corrected features.csv schema, then
execute both in-place. Changes are minimal and surgical:

N02:
  - After loading, drop is_orphan rows (so all delay rates exclude them)
  - Replace the "37% / class balance" copy in the takeaways with
    a fresh-numbers note

N03:
  - Replace 'num_trains_at_station' with 'num_predictions_in_feed' in
    NUMERIC_FEATURES
  - After loading, drop is_orphan rows (so we don't train on mislabeled data)
  - Update the "Next Steps" cell to remove obsolete bullets
"""

import os
import nbformat
from nbformat.v4 import new_code_cell
from nbconvert.preprocessors import ExecutePreprocessor

PROJ = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
N02 = os.path.join(PROJ, "notebooks", "02_eda.ipynb")
N03 = os.path.join(PROJ, "notebooks", "03_baseline_model.ipynb")


def patch_notebook(path, replacements, insertions=None):
    nb = nbformat.read(path, as_version=4)
    for cell in nb.cells:
        if cell.cell_type != "code" and cell.cell_type != "markdown":
            continue
        for old, new in replacements:
            if old in cell.source:
                cell.source = cell.source.replace(old, new)
                print(f"  Patched in {path.split('/')[-1]}: {old[:60]!r} → {new[:60]!r}")
    if insertions:
        for after_marker, code in insertions:
            for i, cell in enumerate(nb.cells):
                if cell.cell_type == "code" and after_marker in cell.source:
                    nb.cells.insert(i + 1, new_code_cell(code))
                    print(f"  Inserted new cell after {after_marker[:40]!r}")
                    break
    nbformat.write(nb, path)


def execute_notebook(path):
    print(f"\nExecuting {path.split('/')[-1]}...")
    nb = nbformat.read(path, as_version=4)
    ep = ExecutePreprocessor(
        timeout=900,
        kernel_name="python3",
        allow_errors=False,
    )
    ep.preprocess(nb, {"metadata": {"path": os.path.dirname(path)}})
    nbformat.write(nb, path)
    print(f"  Done.")


def main():
    print("=" * 60)
    print("Patching N02 (02_eda.ipynb)")
    print("=" * 60)
    patch_notebook(
        N02,
        replacements=[
            # update the load cell so EDA excludes orphans
            (
                'df = pd.read_csv(os.path.join(PROJ, "data", "features.csv"), parse_dates=["collected_at"])\n'
                'print(f"Shape: {df.shape}")\n'
                'df.head()',
                'df = pd.read_csv(os.path.join(PROJ, "data", "features.csv"), parse_dates=["collected_at"])\n'
                'print(f"Loaded shape: {df.shape}")\n'
                'if "is_orphan" in df.columns:\n'
                '    n_orphan = int(df["is_orphan"].sum())\n'
                '    df = df[df["is_orphan"] == 0].copy()\n'
                '    print(f"Dropped {n_orphan:,} orphan predictions (no arrival in 15 min)")\n'
                'print(f"Working shape: {df.shape}")\n'
                'df.head()',
            ),
            # rename in the realtime features cell
            (
                'subset = df[df["is_delayed"] == label]["num_trains_at_station"]',
                'subset = df[df["is_delayed"] == label]["num_predictions_in_feed"]',
            ),
            (
                'axes[0, 1].set_xlabel("Trains at Station")',
                'axes[0, 1].set_xlabel("Predictions in feed (per cycle)")',
            ),
            (
                'axes[0, 1].set_title("Trains at Station Distribution")',
                'axes[0, 1].set_title("Predictions in feed (near-constant ~6)")',
            ),
            # update takeaways to reflect honest numbers
            (
                "## 8. Key Takeaways\\n\\n- **Class balance** is reasonable (~37% delayed) — no need for aggressive oversampling.\\n- **Red Line** has the highest delay rate, consistent with it being the busiest/longest line.\\n- **Temporal patterns** show clear variation by hour — mid-morning and late-night have elevated delay rates.\\n- **Rolling features** (`delay_rate_30min`, `line_delay_rate_30min`) show strong separation between delayed/on-time, suggesting they will be powerful predictors.\\n- **Incidents** correlate with higher delay rates, as expected.\\n- **GTFS scheduled headway** has ~53% missing values (direction mapping mismatch) — will need to handle or drop for modeling.\\n- **Next step:** Baseline model with logistic regression using temporal + line + rolling features.",
                "## 8. Key Takeaways\\n\\n- **Dataset:** ~11.2M predictions across 5 weeks (Mar 11 – Apr 17), labeled headway-based after collapsing duplicate ARR/BRD polls into single arrival events.\\n- **Class balance** ≈ 28% delayed (excluding orphan predictions where no arrival was observed in the next 15 min).\\n- **Orphan predictions** are 17.5% of raw rows — almost certainly long delays the API never resolved. Filtered out for honest EDA.\\n- **Temporal patterns** show clear hour-of-day variation; rush hours have noticeably higher delay rates.\\n- **Rolling features** (`delay_rate_30min`, `line_delay_rate_30min`) carry strong autocorrelation with the target — useful for prediction but expect some look-ahead concern at inference time.\\n- **Incidents** correlate with higher delay rates as expected; effect is modest at the per-row level.\\n- **GTFS scheduled headway** now covers ~96% of rows (up from 48% before fixing service-id and direction mapping). Median ~9.5 min.\\n- **Next step:** baseline model with the corrected features.",
            ),
        ],
    )
    execute_notebook(N02)

    print("\n" + "=" * 60)
    print("Patching N03 (03_baseline_model.ipynb)")
    print("=" * 60)
    patch_notebook(
        N03,
        replacements=[
            # Drop orphans + handle renamed column when loading
            (
                'df = pd.read_csv(os.path.join(PROJ, "data", "features.csv"), parse_dates=["collected_at"])\n'
                'print(f"Loaded {len(df):,} rows")',
                'df = pd.read_csv(os.path.join(PROJ, "data", "features.csv"), parse_dates=["collected_at"])\n'
                'print(f"Loaded {len(df):,} rows")\n'
                'if "is_orphan" in df.columns:\n'
                '    df = df[df["is_orphan"] == 0].copy()\n'
                '    print(f"After dropping orphans: {len(df):,} rows")',
            ),
            # Rename in NUMERIC_FEATURES
            (
                '"minutes_num", "num_trains_at_station", "avg_cars_at_station",',
                '"minutes_num", "num_predictions_in_feed", "avg_cars_at_station",',
            ),
            # Refresh next-steps copy
            (
                "## 7. Next Steps\\n\\n- **Hyperparameter tuning** with `TimeSeriesSplit` cross-validation\\n- **XGBoost / LightGBM** for potential AUC improvement\\n- **Feature selection** — drop low-importance features, address GTFS headway missingness\\n- **More data** — pipeline continues collecting; re-run `build_features.py` before final submission\\n- **Threshold tuning** — optimize classification threshold for best F1 / business utility",
                "## 7. Next Steps\\n\\n- **Validate on a held-out future week** (use TimeSeriesSplit instead of a single 80/20 cut).\\n- **Address rolling-feature autocorrelation** — `delay_rate_30min` and `line_delay_rate_30min` carry strong temporal leakage; compare against a model that excludes them.\\n- **CatBoost** (already explored in `12_model.ipynb`) on the corrected features.\\n- **Threshold tuning** for operational F1 / cost-aware classification.",
            ),
        ],
    )
    execute_notebook(N03)

    print("\nAll done.")


if __name__ == "__main__":
    main()
