## Cash Equivalent Flaw between data source.

Because of this structural difference, your database will hold two very different versions of reality depending on which source you fetched.

If you run the ETL for ONGC using YFinance, and then run it again using Screener, the resulting financial metrics will not match:

    Understated Liquidity: The Screener dataset will severely understate the company's usable cash. Metrics like the Cash Ratio, Quick Ratio, and Net Debt will look significantly worse (higher risk) in the Screener dataset than in the YFinance dataset.

    Asset Distortion: The Screener dataset traps highly liquid capital inside the long-term Investments bucket, skewing Return on Invested Capital (ROIC) calculations, as operating cash is misclassified as a strategic investment.

Your ETL code is functioning perfectly by refusing to mix the aggregated Investments into the cash bucket (which would artificially inflate liquidity with long-term assets). The flaw is not in your code; the flaw is inherent to Screener's consolidated data feed. When your Swarm evaluates Indian companies using Screener data, it must be programmed to expect artificially depressed liquidity ratios.
