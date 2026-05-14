import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import OLSInfluence

# Maps database Sectors and Industries to their 3 most critical Macro Beams.
INDUSTRY_MACRO_MAP = {
    # --- LEVEL 1: BROAD SECTORS ---
    "Technology": ["USD_INR", "US_Dollar_Index_3M", "US_10Y_Yield_6M"],
    "Healthcare": ["USD_INR", "Broad_Commodity", "US_Dollar_Index_3M"],
    "Basic Materials": ["Broad_Commodity", "Brent_Crude", "India_10Y_Yield_6M"],
    "Energy": ["Brent_Crude", "USD_INR", "Yield_Spread_6M"],
    "Consumer Cyclical": ["India_CPI_3M", "India_10Y_Yield_6M", "Brent_Crude"],
    "Consumer Defensive": ["India_CPI_3M", "Broad_Commodity", "India_10Y_Yield_6M"],
    "Industrials": ["Broad_Commodity", "Brent_Crude", "India_10Y_Yield_6M"],
    "Utilities": ["India_10Y_Yield_6M", "US_10Y_Yield_6M", "Brent_Crude"],
    "Financial Services": ["India_10Y_Yield_6M", "Yield_Spread_6M", "US_10Y_Yield_6M"],
    # --- LEVEL 2: SPECIFIC INDUSTRY OVERRIDES ---
    # Tech & IT
    "Software - Application": ["USD_INR", "US_Dollar_Index_3M", "US_10Y_Yield_6M"],
    "Software - Infrastructure": ["USD_INR", "US_Dollar_Index_3M", "US_10Y_Yield_6M"],
    "Information Technology Services": [
        "USD_INR",
        "US_Dollar_Index_3M",
        "US_10Y_Yield_6M",
    ],
    "Computer Hardware": ["USD_INR", "Broad_Commodity", "US_Dollar_Index_3M"],
    "Consumer Electronics": ["USD_INR", "Broad_Commodity", "India_CPI_3M"],
    # Semiconductors (High CapEx, heavy global trade)
    "Semiconductors": ["US_10Y_Yield_6M", "US_Dollar_Index_3M", "Broad_Commodity"],
    "Semiconductor Equipment & Materials": [
        "US_10Y_Yield_6M",
        "US_Dollar_Index_3M",
        "Broad_Commodity",
    ],
    "Electronic Components": ["US_Dollar_Index_3M", "Broad_Commodity", "USD_INR"],
    # Financials
    "Banks - Regional": ["India_10Y_Yield_6M", "Yield_Spread_6M", "India_CPI_3M"],
    "Credit Services": ["India_10Y_Yield_6M", "Yield_Spread_6M", "India_CPI_3M"],
    "Financial Conglomerates": [
        "India_10Y_Yield_6M",
        "Yield_Spread_6M",
        "US_10Y_Yield_6M",
    ],
    # FMCG & Defensive
    "Packaged Foods": ["India_CPI_3M", "Broad_Commodity", "India_10Y_Yield_6M"],
    "Tobacco": ["India_CPI_3M", "Broad_Commodity", "India_10Y_Yield_6M"],
    "Household & Personal Products": [
        "India_CPI_3M",
        "Broad_Commodity",
        "India_10Y_Yield_6M",
    ],
    # Healthcare
    "Drug Manufacturers - Specialty & Generic": [
        "USD_INR",
        "Broad_Commodity",
        "US_Dollar_Index_3M",
    ],
    "Biotechnology": ["US_10Y_Yield_6M", "USD_INR", "US_Dollar_Index_3M"],
    # Basic Materials & Chemicals
    "Specialty Chemicals": ["Brent_Crude", "Broad_Commodity", "USD_INR"],
    "Chemicals": ["Brent_Crude", "Broad_Commodity", "USD_INR"],
    "Agricultural Inputs": ["Broad_Commodity", "Brent_Crude", "India_CPI_3M"],
    "Steel": ["Broad_Commodity", "India_10Y_Yield_6M", "US_Dollar_Index_3M"],
    "Aluminum": ["Broad_Commodity", "India_10Y_Yield_6M", "US_Dollar_Index_3M"],
    "Other Industrial Metals & Mining": [
        "Broad_Commodity",
        "US_Dollar_Index_3M",
        "India_10Y_Yield_6M",
    ],
    # Heavy Industrials & Construction
    "Engineering & Construction": [
        "India_10Y_Yield_6M",
        "Broad_Commodity",
        "India_CPI_3M",
    ],
    "Building Materials": ["India_10Y_Yield_6M", "Broad_Commodity", "Brent_Crude"],
    "Building Products & Equipment": [
        "India_10Y_Yield_6M",
        "Broad_Commodity",
        "India_CPI_3M",
    ],
    "Specialty Industrial Machinery": [
        "Broad_Commodity",
        "India_10Y_Yield_6M",
        "US_Dollar_Index_3M",
    ],
    "Metal Fabrication": ["Broad_Commodity", "Brent_Crude", "India_10Y_Yield_6M"],
    "Electrical Equipment & Parts": [
        "Broad_Commodity",
        "India_10Y_Yield_6M",
        "USD_INR",
    ],
    "Tools & Accessories": ["Broad_Commodity", "India_10Y_Yield_6M", "India_CPI_3M"],
    "Pollution & Treatment Controls": [
        "India_10Y_Yield_6M",
        "Broad_Commodity",
        "US_Dollar_Index_3M",
    ],
    "Aerospace & Defense": [
        "US_Dollar_Index_3M",
        "Broad_Commodity",
        "India_10Y_Yield_6M",
    ],
    # Energy
    "Oil & Gas Integrated": ["Brent_Crude", "USD_INR", "Yield_Spread_6M"],
    "Oil & Gas Refining & Marketing": ["Brent_Crude", "USD_INR", "Yield_Spread_6M"],
    "Thermal Coal": ["Broad_Commodity", "Brent_Crude", "India_10Y_Yield_6M"],
    "Solar": ["Broad_Commodity", "US_10Y_Yield_6M", "US_Dollar_Index_3M"],
    # Transportation & Logistics
    "Marine Shipping": ["Brent_Crude", "US_Dollar_Index_3M", "Yield_Spread_6M"],
    "Railroads": ["Brent_Crude", "India_CPI_3M", "India_10Y_Yield_6M"],
    # Consumer Cyclical & Discretionary
    "Auto Parts": ["Broad_Commodity", "Brent_Crude", "India_10Y_Yield_6M"],
    "Apparel Manufacturing": ["India_CPI_3M", "Broad_Commodity", "USD_INR"],
    "Furnishings, Fixtures & Appliances": [
        "India_CPI_3M",
        "Broad_Commodity",
        "India_10Y_Yield_6M",
    ],
    "Travel Services": ["Brent_Crude", "India_CPI_3M", "USD_INR"],
    # Utilities
    "Utilities - Renewable": [
        "India_10Y_Yield_6M",
        "US_10Y_Yield_6M",
        "Broad_Commodity",
    ],
    "Utilities - Independent Power Producers": [
        "India_10Y_Yield_6M",
        "Brent_Crude",
        "Broad_Commodity",
    ],
    "Utilities - Regulated Electric": [
        "India_10Y_Yield_6M",
        "Brent_Crude",
        "US_10Y_Yield_6M",
    ],
    "Utilities - Regulated Gas": [
        "India_10Y_Yield_6M",
        "Brent_Crude",
        "US_10Y_Yield_6M",
    ],
    # Special Classifications
    "Conglomerates": ["India_10Y_Yield_6M", "USD_INR", "Brent_Crude"],
    "Unknown": ["US_10Y_Yield_6M", "USD_INR", "Brent_Crude"],
    "DEFAULT": ["US_10Y_Yield_6M", "USD_INR", "Brent_Crude"],
}


class Phase2_OLS_Engine:
    def __init__(self, macro_df, micro_df):
        # --- BULLETPROOF DATETIME PARSING & TIMEZONE STRIPPING ---
        self.macro_df = macro_df.copy()
        self.macro_df.index = pd.to_datetime(self.macro_df.index, errors="coerce")
        self.macro_df = self.macro_df[self.macro_df.index.notnull()]
        if self.macro_df.index.tz is not None:
            self.macro_df.index = self.macro_df.index.tz_convert(None)
        self.macro_df.sort_index(inplace=True)

        self.micro_df = micro_df.copy()
        if "ReportDate" in self.micro_df.columns:
            self.micro_df.set_index("ReportDate", inplace=True)

        self.micro_df.index = pd.to_datetime(self.micro_df.index, errors="coerce")
        self.micro_df = self.micro_df[self.micro_df.index.notnull()]
        if self.micro_df.index.tz is not None:
            self.micro_df.index = self.micro_df.index.tz_convert(None)
        self.micro_df.sort_index(inplace=True)

    def prepare_macro_features(self):
        # 1. Compress daily data into Quarterly Averages
        # FIX: Pandas 2.2+ requires "QE" instead of "Q"
        macro_q = self.macro_df.resample("QE").mean()

        # 2. Apply Architectural Lenses (Lags)
        features = pd.DataFrame(index=macro_q.index)

        # Helper to safely pull columns even if missing from DB
        def safe_pull(col_name):
            if col_name in macro_q.columns:
                return macro_q[col_name]
            return pd.Series(np.nan, index=macro_q.index)

        # 0-Month Lags
        features["Brent_Crude"] = safe_pull("Brent_Crude")
        features["USD_INR"] = safe_pull("USD_INR")
        features["Broad_Commodity"] = safe_pull("Broad_Commodity")

        # 3-Month Lags
        features["US_Dollar_Index_3M"] = safe_pull("US_Dollar_Index").shift(1)
        features["India_CPI_3M"] = safe_pull("India_CPI").shift(1)

        # 6-Month Lags
        features["India_10Y_Yield_6M"] = safe_pull("India_10Y_Yield").shift(2)
        features["US_10Y_Yield_6M"] = safe_pull("US_10Y_Yield").shift(2)
        features["Yield_Spread_6M"] = safe_pull("Yield_Spread").shift(2)

        return features

    def predict_phantom_dot(self, model, valid_cols, last_reported_date):
        X_processed = self.prepare_macro_features()

        # Force strict Quarter-Snap alignment to match trained model
        X_processed.index = (
            X_processed.index.to_period("Q").to_timestamp(how="end").normalize()
        )
        X_processed["time_index"] = range(1, len(X_processed) + 1)

        last_reported_date = pd.to_datetime(last_reported_date)
        future_macro = X_processed[X_processed.index > last_reported_date]

        if future_macro.empty:
            return None

        next_quarter = future_macro.iloc[[0]]
        next_quarter_clean = next_quarter[valid_cols].dropna()
        if next_quarter_clean.empty:
            return None

        exog_cols = model.model.exog_names
        next_exog = pd.DataFrame(index=next_quarter_clean.index)

        for col in exog_cols:
            if col == "const":
                next_exog["const"] = 1.0
            else:
                next_exog[col] = next_quarter_clean[col]

        phantom_value = model.predict(next_exog).iloc[0]

        return {
            "target_date": next_exog.index[0].strftime("%Y-%m-%d"),
            "predicted_value": round(phantom_value, 4),
        }

    def run_static_baseline(
        self,
        target_column,
        sector="DEFAULT",
        industry="DEFAULT",
        excluded_dates=None,
        custom_beams=None,
    ):
        X_processed = self.prepare_macro_features()

        # --- THE ULTIMATE ALIGNMENT FIX (QUARTER-SNAP) ---
        # Snaps all dates to exact quarter ends (e.g., 2024-03-29 becomes 2024-03-31)
        X_processed.index = (
            X_processed.index.to_period("Q").to_timestamp(how="end").normalize()
        )

        if target_column not in self.micro_df.columns:
            return {
                "error": f"Target column '{target_column}' not found in micro data."
            }

        Y_raw = self.micro_df[target_column].dropna()
        Y_raw.index = Y_raw.index.to_period("Q").to_timestamp(how="end").normalize()
        # --------------------------------------------------

        # Align X and Y on exact overlapping dates
        aligned_dates = X_processed.index.intersection(Y_raw.index)

        N = len(aligned_dates)
        if N < 3:
            return {"error": f"Insufficient data (N={N}). Minimum 3 quarters required."}

        X_processed["time_index"] = range(1, len(X_processed) + 1)

        if N < 12:
            valid_cols = ["time_index"]
        elif N < 24:
            if custom_beams and len(custom_beams) == 3:
                valid_cols = custom_beams
            else:
                mapped_beams = INDUSTRY_MACRO_MAP.get(
                    industry,
                    INDUSTRY_MACRO_MAP.get(sector, INDUSTRY_MACRO_MAP["DEFAULT"]),
                )
                valid_cols = mapped_beams
        else:
            valid_cols = [c for c in X_processed.columns if c != "time_index"]

        valid_cols = [c for c in valid_cols if c in X_processed.columns]
        X_aligned = X_processed.loc[aligned_dates, valid_cols].dropna()

        if len(X_aligned) < len(valid_cols) + 3:
            valid_cols = ["time_index"]
            X_aligned = X_processed.loc[aligned_dates, valid_cols].dropna()

        Y_aligned = Y_raw.loc[X_aligned.index]

        if excluded_dates:
            excluded_dt = (
                pd.to_datetime(excluded_dates)
                .to_period("Q")
                .to_timestamp(how="end")
                .normalize()
            )
            X_clean = X_aligned.drop(index=excluded_dt, errors="ignore")
            Y_clean = Y_aligned.drop(index=excluded_dt, errors="ignore")
        else:
            X_clean = X_aligned
            Y_clean = Y_aligned

        X_with_const = sm.add_constant(X_clean)
        model = sm.OLS(Y_clean, X_with_const).fit()

        predictions = model.get_prediction(X_with_const)
        summary_frame = predictions.summary_frame(alpha=0.05)

        influence = OLSInfluence(model)
        cooks_d = influence.cooks_distance[0]

        N_clean = len(Y_clean)
        cooks_threshold = 4 / N_clean if N_clean > 0 else 0
        is_outlier = [bool(cd > cooks_threshold) for cd in cooks_d]

        residuals = Y_clean - model.fittedvalues

        payload = {
            "target_metric": target_column,
            "r_squared": round(model.rsquared, 4),
            "alpha_moat": (
                round(model.params["const"], 4) if "const" in model.params else 0
            ),
            "betas": model.params.drop("const", errors="ignore").round(4).to_dict(),
            "p_values": model.pvalues.round(4).to_dict(),
            "timeline_data": {
                "dates": Y_clean.index.strftime("%Y-%m-%d").tolist(),
                "actual_y": Y_clean.tolist(),
                "predicted_y": model.fittedvalues.tolist(),
                "residuals": residuals.tolist(),
                "conf_lower": summary_frame["obs_ci_lower"].tolist(),
                "conf_upper": summary_frame["obs_ci_upper"].tolist(),
                "cooks_distance": list(cooks_d),
                "is_outlier": is_outlier,
            },
        }

        last_date = Y_clean.index.max()
        payload["phantom_dot"] = self.predict_phantom_dot(model, valid_cols, last_date)

        return payload
