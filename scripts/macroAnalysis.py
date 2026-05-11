import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import OLSInfluence


class Phase2_OLS_Engine:
    def __init__(self, macro_df, micro_df):

        self.macro_df = macro_df.copy()

        # Ensure dates are datetime objects and sort them
        if not pd.api.types.is_datetime64_any_dtype(self.macro_df.index):
            self.macro_df.index = pd.to_datetime(self.macro_df.index)
        self.macro_df.sort_index(inplace=True)

        self.micro_df = micro_df.copy()
        if "ReportDate" in self.micro_df.columns:
            self.micro_df["ReportDate"] = pd.to_datetime(self.micro_df["ReportDate"])
            self.micro_df.set_index("ReportDate", inplace=True)
        self.micro_df.sort_index(inplace=True)

    def prepare_macro_features(self):

        # 1. Compress daily data into Quarterly Averages
        # 'QE' maps to Quarter End.
        macro_q = self.macro_df.resample("QE").mean()

        # 2. Apply Architectural Lenses (Lags)
        # 1 row = 1 Quarter (3 months)
        features = pd.DataFrame(index=macro_q.index)

        # 0-Month Lags (Coincident)
        features["Brent_Crude"] = macro_q["Brent_Crude"]
        features["USD_INR"] = macro_q["USD_INR"]
        features["Broad_Commodity"] = macro_q["Broad_Commodity"]

        # 3-Month Lags (Shift 1 Quarter)
        features["US_Dollar_Index_3M"] = macro_q["US_Dollar_Index"].shift(1)
        features["India_CPI_3M"] = macro_q["India_CPI"].shift(1)

        # 6-Month Lags (Shift 2 Quarters)
        features["India_10Y_Yield_6M"] = macro_q["India_10Y_Yield"].shift(2)
        features["US_10Y_Yield_6M"] = macro_q["US_10Y_Yield"].shift(2)
        features["Yield_Spread_6M"] = macro_q["Yield_Spread"].shift(2)

        # Drop rows where lag creation caused NaNs
        return features.dropna()

    def run_static_baseline(self, target_column, excluded_dates=None):

        # 1. Get processed Macro Features (X)
        X_processed = self.prepare_macro_features()

        # 2. Isolate the target Micro Metric (Y) and sync the timeline
        if target_column not in self.micro_df.columns:
            raise ValueError(
                f"Target column '{target_column}' not found in micro data."
            )

        Y_raw = self.micro_df[target_column].dropna()

        # Align X and Y on exact overlapping dates
        aligned_dates = X_processed.index.intersection(Y_raw.index)
        if len(aligned_dates) == 0:
            return {
                "error": "No overlapping dates found between Macro and Micro data after applying lags."
            }

        X_aligned = X_processed.loc[aligned_dates]
        Y_aligned = Y_raw.loc[aligned_dates]

        # 3. Apply UI Triage (The Outlier Exclusions)
        if excluded_dates:
            excluded_dt = pd.to_datetime(excluded_dates)
            X_clean = X_aligned.drop(index=excluded_dt, errors="ignore")
            Y_clean = Y_aligned.drop(index=excluded_dt, errors="ignore")
        else:
            X_clean = X_aligned
            Y_clean = Y_aligned

        # 4. Build the OLS Bridge
        X_with_const = sm.add_constant(X_clean)  # Generates the Alpha (Moat)
        model = sm.OLS(Y_clean, X_with_const).fit()

        # 5. The Diagnostic Sweep (Cook's D, Residuals, Confidence Bands)
        predictions = model.get_prediction(X_with_const)
        summary_frame = predictions.summary_frame(alpha=0.05)  # 95% Confidence Level

        influence = OLSInfluence(model)
        cooks_d = influence.cooks_distance[0]

        residuals = Y_clean - model.fittedvalues

        # 6. Package the JSON Payload for the Dashboard Control Room
        payload = {
            "target_metric": target_column,
            "r_squared": round(model.rsquared, 4),
            "alpha_moat": (
                round(model.params["const"], 4) if "const" in model.params else 0
            ),
            "betas": model.params.drop("const", errors="ignore").round(4).to_dict(),
            "p_values": model.pvalues.round(4).to_dict(),
            # Time-series plotting arrays for the UI charts
            "timeline_data": {
                "dates": Y_clean.index.strftime("%Y-%m-%d").tolist(),
                "actual_y": Y_clean.tolist(),
                "predicted_y": model.fittedvalues.tolist(),
                "residuals": residuals.tolist(),
                "conf_lower": summary_frame["obs_ci_lower"].tolist(),
                "conf_upper": summary_frame["obs_ci_upper"].tolist(),
                "cooks_distance": list(cooks_d),
            },
        }

        return payload
