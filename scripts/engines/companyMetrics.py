import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scripts.ratioAnalysis import (
    fetch_ccc,
    fetch_cfo_to_pat,
    fetch_operating_margin,
    fetch_debt_to_equity,
    fetch_roic,
    fetch_fcf_yield,
    fetch_dol,
    fetch_gross_margin,
    fetch_interest_coverage,
    fetch_asset_turnover,
    fetch_revenue_growth_yoy,
    fetch_fcf_margin,
    fetch_piotroski_f_score,
    fetch_beneish_m_score,
)


def render_company_metrics(
    selected_db_ticker, selected_source, view_mode, edgar_break_date, engine
):

    # 1. ENCAPSULATED DATA FETCHING
    with st.spinner(
        f"Running Forensic Scan for {selected_db_ticker} using {selected_source.upper()} data..."
    ):
        # Fetch Phase 1 & 2 Math
        df_roic = fetch_roic(selected_db_ticker, selected_source)
        df_fcf = fetch_fcf_yield(selected_db_ticker, selected_source)
        df_de = fetch_debt_to_equity(selected_db_ticker, selected_source)
        df_ccc = fetch_ccc(selected_db_ticker, selected_source)
        df_dol = fetch_dol(selected_db_ticker, selected_source)
        df_turnover = fetch_asset_turnover(selected_db_ticker, selected_source)
        df_op_margin = fetch_operating_margin(selected_db_ticker, selected_source)
        df_gr_margin = fetch_gross_margin(selected_db_ticker, selected_source)
        df_int_cov = fetch_interest_coverage(selected_db_ticker, selected_source)
        df_cfo_pat = fetch_cfo_to_pat(selected_db_ticker, selected_source)
        df_rev_growth = fetch_revenue_growth_yoy(selected_db_ticker, selected_source)
        df_fcf_margin = fetch_fcf_margin(selected_db_ticker, selected_source)

        # Fetch Phase 1 Gatekeepers
        df_piotroski = fetch_piotroski_f_score(selected_db_ticker, engine)
        df_beneish = fetch_beneish_m_score(selected_db_ticker, engine)

        # Apply Perspective Filtering
        if "Predictive" in view_mode and edgar_break_date:
            cutoff = pd.to_datetime(edgar_break_date)
            df_roic = df_roic[pd.to_datetime(df_roic["ReportDate"]) > cutoff]
            df_fcf = df_fcf[pd.to_datetime(df_fcf["ReportDate"]) > cutoff]
            df_de = df_de[pd.to_datetime(df_de["ReportDate"]) > cutoff]
            df_ccc = df_ccc[pd.to_datetime(df_ccc["ReportDate"]) > cutoff]
            df_dol = df_dol[pd.to_datetime(df_dol["ReportDate"]) > cutoff]
            df_op_margin = df_op_margin[
                pd.to_datetime(df_op_margin["ReportDate"]) > cutoff
            ]
            df_gr_margin = df_gr_margin[
                pd.to_datetime(df_gr_margin["ReportDate"]) > cutoff
            ]
            df_int_cov = df_int_cov[pd.to_datetime(df_int_cov["ReportDate"]) > cutoff]
            df_cfo_pat = df_cfo_pat[pd.to_datetime(df_cfo_pat["ReportDate"]) > cutoff]
            df_turnover = df_turnover[
                pd.to_datetime(df_turnover["ReportDate"]) > cutoff
            ]

    # 2. ENCAPSULATED UI RENDER
    with st.expander("Base Company Metrics & Data Room", expanded=True):
        st.markdown("### Dashboard Display Controls")

        toggle_col1, toggle_col2 = st.columns(2)
        with toggle_col1:
            show_convergence = st.toggle("Master Convergence Chart", value=True)
        with toggle_col2:
            show_fundamentals = st.toggle("Phase 1: Fundamental Deep Dive", value=False)

        st.divider()

        # VIEW 1: THE CONVERGENCE CHART
        if show_convergence:
            st.markdown("### Capital Efficiency & Structural Health Overlay")
            st.caption(
                "Overlaying the company's Return on Invested Capital (ROIC) against its Piotroski Health Score to spot business decay."
            )

            if not df_piotroski.empty and not df_roic.empty:
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                df_p_plot = df_piotroski.sort_values("ReportDate")
                df_r_plot = df_roic.sort_values("ReportDate")

                fig.add_trace(
                    go.Scatter(
                        x=df_r_plot["ReportDate"],
                        y=df_r_plot["roic"] * 100,
                        name="ROIC (%)",
                        line=dict(color="#2E86C1", width=3),
                    ),
                    secondary_y=False,
                )

                colors = [
                    (
                        "#27AE60"
                        if score >= 7
                        else "#F1C40F" if score >= 4 else "#C0392B"
                    )
                    for score in df_p_plot["Piotroski_F_Score"]
                ]
                fig.add_trace(
                    go.Bar(
                        x=df_p_plot["ReportDate"],
                        y=df_p_plot["Piotroski_F_Score"],
                        name="Piotroski Score (0-9)",
                        marker_color=colors,
                        opacity=0.4,
                    ),
                    secondary_y=True,
                )

                fig.update_yaxes(title_text="ROIC (%)", secondary_y=False)
                fig.update_yaxes(
                    range=[0, 36],
                    showgrid=False,
                    showticklabels=False,
                    secondary_y=True,
                )
                fig.update_layout(
                    height=500,
                    hovermode="x unified",
                    legend=dict(
                        orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1
                    ),
                    margin=dict(l=20, r=20, t=60, b=20),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Insufficient data to build convergence chart.")

        # VIEW 2: FUNDAMENTALS
        if show_fundamentals:
            if not df_beneish.empty:
                latest_m_score = float(df_beneish.iloc[0]["Beneish_M_Score"])
                if latest_m_score > -1.78:
                    st.error(
                        f"Beneish M-Score Warning: {latest_m_score} - Statistical probability of earnings manipulation detected."
                    )
                else:
                    st.success(
                        f"Beneish M-Score: {latest_m_score} - Accounting appears clean."
                    )

            st.subheader("Structural Anchors")
            st.caption("Long-term capital efficiency and survival metrics.")

            yc1, yc2, yc3, yc4, yc5, yc6 = st.columns(6)
            with yc1:
                latest_roic = (
                    df_roic["roic"].iloc[0] * 100
                    if not df_roic.empty and pd.notnull(df_roic["roic"].iloc[0])
                    else 0
                )
                st.metric("ROIC", f"{latest_roic:.2f}%")
            with yc2:
                latest_fcf = (
                    df_fcf["FCF_Yield"].iloc[0] * 100
                    if not df_fcf.empty
                    and pd.notnull(df_fcf.get("FCF_Yield", pd.Series([None])).iloc[0])
                    else 0
                )
                st.metric("FCF Yield", f"{latest_fcf:.2f}%")
            with yc3:
                latest_de = (
                    df_de["debt_to_equity"].iloc[0]
                    if not df_de.empty and pd.notnull(df_de["debt_to_equity"].iloc[0])
                    else 0
                )
                st.metric("Debt / Equity", f"{latest_de:.2f}")
            with yc4:
                latest_ccc = (
                    df_ccc["cash_conversion_cycle"].iloc[0]
                    if not df_ccc.empty
                    and pd.notnull(df_ccc["cash_conversion_cycle"].iloc[0])
                    else 0
                )
                st.metric("Cash Conv. Cycle", f"{latest_ccc:.0f} Days")
            with yc5:
                latest_dol = (
                    df_dol["degree_of_operating_leverage"].iloc[0]
                    if not df_dol.empty
                    and pd.notnull(df_dol["degree_of_operating_leverage"].iloc[0])
                    else 0
                )
                st.metric("Op. Leverage (DOL)", f"{latest_dol:.2f}x")
            with yc6:
                latest_turnover = (
                    df_turnover["asset_turnover"].iloc[0]
                    if not df_turnover.empty
                    and pd.notnull(df_turnover["asset_turnover"].iloc[0])
                    else 0
                )
                st.metric("Asset Turnover", f"{latest_turnover:.2f}x")

            st.divider()
            st.subheader("Tactical Responders")
            qc1, qc2, qc3, qc4 = st.columns(4)
            with qc1:
                latest_op_margin = (
                    df_op_margin["operating_margin"].iloc[0] * 100
                    if not df_op_margin.empty
                    and pd.notnull(df_op_margin["operating_margin"].iloc[0])
                    else 0
                )
                st.metric("Operating Margin", f"{latest_op_margin:.2f}%")
            with qc2:
                latest_gr_margin = (
                    df_gr_margin["gross_margin"].iloc[0] * 100
                    if not df_gr_margin.empty
                    and pd.notnull(df_gr_margin["gross_margin"].iloc[0])
                    else 0
                )
                st.metric("Gross Margin", f"{latest_gr_margin:.2f}%")
            with qc3:
                latest_int_cov = (
                    df_int_cov["interest_coverage"].iloc[0]
                    if not df_int_cov.empty
                    and pd.notnull(df_int_cov["interest_coverage"].iloc[0])
                    else 0
                )
                st.metric("Interest Coverage", f"{latest_int_cov:.2f}x")
            with qc4:
                latest_cfo_pat = (
                    df_cfo_pat["cfo_to_pat"].iloc[0]
                    if not df_cfo_pat.empty
                    and pd.notnull(df_cfo_pat["cfo_to_pat"].iloc[0])
                    else 0
                )
                st.metric("CFO / PAT", f"{latest_cfo_pat:.2f}")

            st.divider()
            st.subheader("Data Room")
            with st.expander("View Raw Structural Matrices"):
                if not df_roic.empty:
                    p1_merged = df_roic[["ReportDate", "roic"]]
                    if not df_fcf.empty:
                        p1_merged = pd.merge(
                            p1_merged,
                            df_fcf[["ReportDate", "FCF_Yield"]],
                            on="ReportDate",
                            how="outer",
                        )
                    if not df_de.empty:
                        p1_merged = pd.merge(
                            p1_merged,
                            df_de[["ReportDate", "debt_to_equity"]],
                            on="ReportDate",
                            how="outer",
                        )
                    if not df_ccc.empty:
                        p1_merged = pd.merge(
                            p1_merged,
                            df_ccc[["ReportDate", "cash_conversion_cycle"]],
                            on="ReportDate",
                            how="outer",
                        )
                    if not df_dol.empty:
                        p1_merged = pd.merge(
                            p1_merged,
                            df_dol[["ReportDate", "degree_of_operating_leverage"]],
                            on="ReportDate",
                            how="outer",
                        )
                    if not df_turnover.empty:
                        p1_merged = pd.merge(
                            p1_merged,
                            df_turnover[["ReportDate", "asset_turnover"]],
                            on="ReportDate",
                            how="outer",
                        )
                    st.dataframe(
                        p1_merged.sort_values(by="ReportDate", ascending=False),
                        use_container_width=True,
                    )

            with st.expander("View Raw Tactical Matrices"):
                if not df_op_margin.empty:
                    p2_merged = df_op_margin[["ReportDate", "operating_margin"]]
                    if not df_gr_margin.empty:
                        p2_merged = pd.merge(
                            p2_merged,
                            df_gr_margin[["ReportDate", "gross_margin"]],
                            on="ReportDate",
                            how="outer",
                        )
                    if not df_int_cov.empty:
                        p2_merged = pd.merge(
                            p2_merged,
                            df_int_cov[["ReportDate", "interest_coverage"]],
                            on="ReportDate",
                            how="outer",
                        )
                    if not df_cfo_pat.empty:
                        p2_merged = pd.merge(
                            p2_merged,
                            df_cfo_pat[["ReportDate", "cfo_to_pat"]],
                            on="ReportDate",
                            how="outer",
                        )
                    st.dataframe(
                        p2_merged.sort_values(by="ReportDate", ascending=False),
                        use_container_width=True,
                    )

    # 3. RETURN REQUIRED ENGINE DATA
    # (So the Dashboard can pass it down to OLS without re-fetching)
    return {
        "df_op_margin": df_op_margin,
        "df_gr_margin": df_gr_margin,
        "df_int_cov": df_int_cov,
        "df_turnover": df_turnover,
        "df_fcf_margin": df_fcf_margin,
        "df_rev_growth": df_rev_growth,
    }
