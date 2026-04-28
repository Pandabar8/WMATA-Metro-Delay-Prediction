# Workshop Talking Points — WMATA Delay Prediction

**ENAI 603 | Prof. Patrice Seyed | Spring 2026**

---

## 30-Second Pitch

We built an end-to-end pipeline that predicts WMATA Metro train delays from real-time API data. Over 5 weeks of continuous collection (Mar 11 – Apr 17), we gathered ~10M prediction rows and ~13K incident records, engineered headway-based delay labels, and trained a model targeting AUC-ROC > 0.80.

## The Problem

WMATA serves hundreds of thousands of daily commuters but has well-known reliability issues. The WMATA API publishes _time-to-arrival_, not delays — so the first real challenge was **defining what "delayed" even means** from the data we had.

## What We Built

1. **Collection pipeline** — Polls predictions every 2 min, incidents every 5 min. Ran 24/7 on an Oracle Cloud VM via cron.
2. **Delay labeling** — Headway-based: a train is delayed if the gap between consecutive arrivals at a station exceeds the _median headway for that line and hour_ by more than 2 minutes.
3. **Feature engineering** — Temporal (hour, rush hour, day of week), station/line, rolling delay rates, active-incident flags, previous/next stop context.
4. **Modeling** — Logistic Regression baseline → Random Forest → XGBoost, with time-series-aware cross-validation.

## Key Findings

- **Delay rate ≈ 20.7%** after correcting for two label-construction bugs (see below). This is the class balance we modeled against.
- **Two labeling fixes that materially changed results:**
  - Median headway was originally computed _globally_ — this inflated off-peak "delays" because trains genuinely run less often at night. Fixed by segmenting median headway _by line and hour_.
  - Phantom gaps from missed polls (e.g., gaps > 2× median) were being labeled as delays. Now filtered out.
- Result: delay rate dropped from a misleading 37% to a realistic 20.68%.

## What Went Wrong (Be Honest)

- **Initial delay definition was naive.** Caught and fixed mid-project. Good lesson in why label construction deserves as much scrutiny as modeling.
- **Frozen dataset.** We stopped collection on Apr 17 to lock in the analysis. Trade-off: no live demo of the pipeline, but reproducible results.

## What's Next (If Asked)

- Incorporate weather data as an exogenous feature.
- Model delay _propagation_ (how a delay at one station cascades downstream).
- Compare against GTFS scheduled times as an independent ground-truth source.

## If Time For Visuals

Have these ready in a browser tab / notebook:

1. **Network map** of the WMATA rail system — `notebooks/21_data_visualization.ipynb`
2. **Delay rate by hour-of-day** — shows the rush-hour pattern, justifies the segmented-headway fix
3. **Model performance** — AUC-ROC curve, feature importances from the final model

## Quick FAQ

- **"How big is your data?"** ~10M prediction rows, ~13.5K incident rows, 5+ weeks.
- **"Why SQLite?"** Simple, single-file, perfectly adequate for this scale, easy to share.
- **"How did you handle the running pipeline?"** Cron on an Oracle Cloud free-tier VM. Daily syncs to the local DB.
- **"What's the most interesting thing you learned?"** That defining the target variable was harder and more consequential than picking the model.
