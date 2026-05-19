import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import text
from scripts.macroAnalysis import Phase2_OLS_Engine, MacroMomentumTracker


def render_ols_engine(
    company_sector,
    company_industry,
    df_op_margin,
    df_gr_margin,
    df_int_cov,
    df_turnover,
    df_fcf_margin,
    df_rev_growth,
    engine,
):
    st.header("OLS Macro Bridge & Forensic Triage")
    try:
        macro_query = text(
            'SELECT "ReportDate", "IndicatorName", "Close_Value" FROM macro_indicators'
        )
        macro_raw = pd.read_sql(macro_query, engine)
        macro_df = macro_raw.pivot(
            index="ReportDate",
            columns="IndicatorName",
            values="Close_Value",
        )
        macro_df.index = pd.to_datetime(macro_df.index)
    except Exception as e:
        st.error(f"Failed to load Macro Database: {e}")
        macro_df = pd.DataFrame()

    if not macro_df.empty:
        target_options = {
            "Operating Margin (Pricing Power)": ("operating_margin", df_op_margin),
            "Gross Margin (Anchor Cost)": ("gross_margin", df_gr_margin),
            "Interest Coverage (Solvency)": ("interest_coverage", df_int_cov),
            "Asset Turnover (Capital Efficiency)": ("asset_turnover", df_turnover),
            "FCF Margin (Cash Conversion)": ("fcf_margin", df_fcf_margin),
            "Revenue Growth YoY (Macro Demand)": ("revenue_growth", df_rev_growth),
        }
        selected_pillar_label = st.selectbox(
            "Select Micro Pillar to Validate:", list(target_options.keys())
        )
        target_col, target_df = target_options[selected_pillar_label]

        if not target_df.empty:
            # --- ENGINE 2: GET Z-SCORES FOR DUMMY TRACES LATER ---
            tracker = MacroMomentumTracker(macro_df)
            signals = tracker.get_latest_regime_signals()

            ols_engine = Phase2_OLS_Engine(macro_df, target_df)
            initial_payload = ols_engine.run_static_baseline(
                target_col, sector=company_sector, industry=company_industry
            )

            if "error" in initial_payload:
                st.warning(
                    f"Could not process {target_col}: {initial_payload['error']}"
                )
            else:
                init_tl = initial_payload["timeline_data"]
                n_quarters = len(init_tl["dates"])
                custom_macro_selection = None

                if 12 <= n_quarters < 24:
                    active_beams = list(initial_payload["betas"].keys())
                    st.info(
                        f"💡 Limited Data Detected (N={n_quarters}). Engine auto-loaded the **{company_sector}** macro template: `{active_beams}`"
                    )
                    all_beams = [
                        "Brent_Crude",
                        "USD_INR",
                        "Broad_Commodity",
                        "US_Dollar_Index_3M",
                        "India_CPI_3M",
                        "India_10Y_Yield_6M",
                        "US_10Y_Yield_6M",
                        "Yield_Spread_6M",
                    ]
                    default_beams = [b for b in active_beams if b in all_beams]
                    custom_macro_selection = st.multiselect(
                        "Override Macro Template (Select exactly 3):",
                        options=all_beams,
                        default=default_beams,
                        max_selections=3,
                        key=f"macro_override_{target_col}",
                    )

                if (
                    custom_macro_selection is not None
                    and len(custom_macro_selection) == 3
                    and set(custom_macro_selection) != set(default_beams)
                ):
                    clean_payload = ols_engine.run_static_baseline(
                        target_col,
                        sector=company_sector,
                        industry=company_industry,
                        custom_beams=custom_macro_selection,
                    )
                    if "error" not in clean_payload:
                        initial_payload = clean_payload
                    init_tl = initial_payload["timeline_data"]

                system_outliers = [
                    date
                    for date, is_out in zip(init_tl["dates"], init_tl["is_outlier"])
                    if is_out
                ]

                st.markdown("### Outlier Control Center")
                if system_outliers:
                    st.warning(
                        f"System detected {len(system_outliers)} mathematical anomalies: {', '.join(system_outliers)}"
                    )
                else:
                    st.success("No mathematical anomalies detected.")

                handling_mode = st.radio(
                    "Select Outlier Handling Strategy:",
                    [
                        "Include All Data",
                        "Exclude System-Detected Outliers",
                        "Custom Manual Selection",
                    ],
                    horizontal=True,
                    key=f"radio_outlier_{target_col}",
                )
                final_exclusions = []
                if handling_mode == "Exclude System-Detected Outliers":
                    final_exclusions = system_outliers
                elif handling_mode == "Custom Manual Selection":
                    final_exclusions = st.multiselect(
                        "Select specific quarters to drop from the model:",
                        options=init_tl["dates"],
                        default=system_outliers,
                        key=f"multi_{target_col}",
                    )

                if final_exclusions:
                    clean_payload = ols_engine.run_static_baseline(
                        target_col,
                        sector=company_sector,
                        industry=company_industry,
                        excluded_dates=final_exclusions,
                        custom_beams=custom_macro_selection,
                    )
                    if "error" in clean_payload:
                        clean_payload = initial_payload
                else:
                    clean_payload = initial_payload

                clean_tl = clean_payload["timeline_data"]
                final_payload = clean_payload

                st.metric(
                    "Bridge Strength (R-Squared)",
                    f"{final_payload['r_squared']*100:.1f}%",
                )
                st.divider()
                st.markdown(f"### The Bridge: {target_col} Actual vs. Predicted")

                fig_line = go.Figure()

                # 1. ACTUAL CHART TRACES
                fig_line.add_trace(
                    go.Scatter(
                        x=clean_tl["dates"] + clean_tl["dates"][::-1],
                        y=clean_tl["conf_upper"] + clean_tl["conf_lower"][::-1],
                        fill="toself",
                        fillcolor="rgba(128,128,128,0.2)",
                        line=dict(color="rgba(255,255,255,0)"),
                        name="95% Tolerance",
                        hoverinfo="skip",
                    )
                )

                normal_x, normal_y, outlier_x, outlier_y = [], [], [], []
                for i in range(len(init_tl["dates"])):
                    if (
                        init_tl["is_outlier"][i]
                        or init_tl["dates"][i] in final_exclusions
                    ):
                        outlier_x.append(init_tl["dates"][i])
                        outlier_y.append(init_tl["actual_y"][i])
                    else:
                        normal_x.append(init_tl["dates"][i])
                        normal_y.append(init_tl["actual_y"][i])

                fig_line.add_trace(
                    go.Scatter(
                        x=init_tl["dates"],
                        y=init_tl["actual_y"],
                        mode="lines",
                        name="Actual Trendline",
                        line=dict(color="#FF4B4B", width=2),
                        hoverinfo="skip",
                    )
                )
                fig_line.add_trace(
                    go.Scatter(
                        x=normal_x,
                        y=normal_y,
                        mode="markers",
                        name="Normal Quarter",
                        marker=dict(color="#FF4B4B", size=8),
                    )
                )
                fig_line.add_trace(
                    go.Scatter(
                        x=outlier_x,
                        y=outlier_y,
                        mode="markers",
                        name="Flagged Outlier",
                        marker=dict(
                            color="#FFC107",
                            size=12,
                            line=dict(color="red", width=2),
                        ),
                        hovertemplate="<b>%{x}</b><br>Outlier Value: %{y}<extra></extra>",
                    )
                )
                fig_line.add_trace(
                    go.Scatter(
                        x=clean_tl["dates"],
                        y=clean_tl["predicted_y"],
                        mode="lines",
                        name="OLS Predicted",
                        line=dict(color="#0068C9", dash="dash", width=2),
                        connectgaps=True,
                    )
                )

                if "phantom_dot" in final_payload and final_payload["phantom_dot"]:
                    phantom = final_payload["phantom_dot"]
                    fig_line.add_trace(
                        go.Scatter(
                            x=[
                                clean_tl["dates"][-1],
                                phantom["target_date"],
                            ],
                            y=[
                                clean_tl["predicted_y"][-1],
                                phantom["predicted_value"],
                            ],
                            mode="lines",
                            name="Forward Trajectory",
                            line=dict(color="#00FFAA", dash="dot", width=2),
                            showlegend=False,
                            hoverinfo="skip",
                        )
                    )
                    fig_line.add_trace(
                        go.Scatter(
                            x=[phantom["target_date"]],
                            y=[phantom["predicted_value"]],
                            mode="markers",
                            name="Phantom Predictor (Next Qtr)",
                            marker=dict(
                                color="rgba(0,0,0,0)",
                                size=14,
                                line=dict(color="#00FFAA", width=3),
                            ),
                            hovertemplate="<b>Projected: %{x}</b><br>Value: %{y}<extra></extra>",
                        )
                    )

                # 2. ENGINE 2 DUMMY TRACES FOR NATIVE LEGEND
                if signals:
                    # Add an invisible spacer title in the legend
                    fig_line.add_trace(
                        go.Scatter(
                            x=[None],
                            y=[None],
                            mode="markers",
                            marker=dict(color="rgba(0,0,0,0)"),
                            name="<b>-- MACRO RADAR --</b>",
                            hoverinfo="skip",
                        )
                    )

                    for indicator, data in signals.items():
                        z_val = data["Z_Score"]
                        if z_val is None:
                            trace_name = f"⚪ {indicator}: N/A"
                            dot_color = "gray"
                        elif z_val >= 2.0 or z_val <= -2.0:
                            trace_name = f"🚨 {indicator}: {z_val}"
                            dot_color = "#FF4B4B"  # Red
                        else:
                            trace_name = f"🟢 {indicator}: {z_val}"
                            dot_color = "#00FFAA"  # Green

                        fig_line.add_trace(
                            go.Scatter(
                                x=[None],
                                y=[None],  # Invisible on the chart
                                mode="markers",
                                marker=dict(color=dot_color, size=10),
                                name=trace_name,
                                hoverinfo="skip",
                            )
                        )

                fig_line.update_layout(
                    margin=dict(l=0, r=0, t=30, b=0),
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                )

                st.plotly_chart(
                    fig_line,
                    use_container_width=True,
                    key=f"chart_{target_col}",
                )
                if "phantom_dot" in final_payload and final_payload["phantom_dot"]:
                    st.success(
                        f"Based on live trailing 90-day macro data, the engine projects the target metric to hit **{phantom['predicted_value']:.4f}** for the upcoming unreleased quarter ending **{phantom['target_date']}**."
                    )

                with st.expander("View Underlying Numerical Data"):
                    chart_df = pd.DataFrame(
                        {
                            "ReportDate": clean_tl["dates"],
                            "Actual Reported": [
                                round(val, 4) for val in clean_tl["actual_y"]
                            ],
                            "OLS Predicted": [
                                round(val, 4) for val in clean_tl["predicted_y"]
                            ],
                            "Upper Tolerance Band": [
                                round(val, 4) for val in clean_tl["conf_upper"]
                            ],
                            "Lower Tolerance Band": [
                                round(val, 4) for val in clean_tl["conf_lower"]
                            ],
                        }
                    ).set_index("ReportDate")
                    st.dataframe(chart_df, use_container_width=True)

                st.markdown("#### Residual Tracker")
                bar_colors = [
                    "#00C851" if val > 0 else "#FF4444" for val in clean_tl["residuals"]
                ]
                fig_res = go.Figure(
                    data=[
                        go.Bar(
                            x=clean_tl["dates"],
                            y=clean_tl["residuals"],
                            marker_color=bar_colors,
                            width=0.4,
                        )
                    ]
                )
                fig_res.update_layout(
                    margin=dict(l=0, r=0, t=20, b=0),
                    yaxis_title="Margin Beat/Miss",
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    shapes=[
                        dict(
                            type="line",
                            xref="paper",
                            x0=0,
                            x1=1,
                            y0=0,
                            y1=0,
                            line=dict(color="rgba(255, 255, 255, 0.5)", width=2),
                        )
                    ],
                )
                st.plotly_chart(fig_res, use_container_width=True)

                with st.expander("View Mathematical DNA (Alpha Moat & Betas)"):
                    st.markdown(
                        f"Alpha (Structural Moat): `{final_payload['alpha_moat']}`"
                    )
                    st.write("Beta Sensitivities:")
                    st.json(final_payload["betas"])
                    st.write("P-Values (Statistical Significance):")
                    st.json(final_payload["p_values"])
    else:
        st.info(
            "Macro Data is missing. Please run the ETL Control Center Macro Spigot first."
        )
