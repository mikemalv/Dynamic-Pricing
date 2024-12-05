# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.functions as F
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Define model input features at the top
feature_cols = [
    "new_price",
    "base_price",
    "price_hist_dow",
    "price_year_dow",
    "price_month_dow",
    "price_change_hist_dow",
    "price_change_year_dow",
    "price_change_month_dow",
    "price_hist_roll",
    "price_year_roll",
    "price_month_roll",
    "price_change_hist_roll",
    "price_change_year_roll",
    "price_change_month_roll",
]

# Page configuration
st.set_page_config(
    page_title="Casino F&B Pricing App",
    page_icon="🎰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main {
        background-color: #1E1E1E;
        color: white;
    }
    .stApp {
        background-color: black;
    }
    .st-emotion-cache-1y4p8pa {
        padding: 2rem;
        border-radius: 10px;
        background-color: #2E2E2E;
    }
    .st-emotion-cache-6qob1r {
        background-color: #3E3E3E;
    }
    .st-emotion-cache-1v0mbdj {
        color: white;
    }
    .metric-card {
        background-color: #3E3E3E;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    </style>
""", unsafe_allow_html=True)

# Main title with emoji
st.title("🎰 Casino F&B Pricing App 🎰")

# Get the current credentials and data
try:
    session = get_active_session()
    df = session.table("pricing").with_column("comment", F.lit(""))

    # Create tabs for different sections
    tab1, tab2 = st.tabs(["🎯 Price Management", "📈 Demand Forecast"])

    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            brand = st.selectbox(
                "Select Brand:",
                df.select("brand").distinct(),
                help="Choose the F&B brand to manage prices"
            )
            
        with col2:
            item = st.selectbox(
                "Select Item:",
                df.filter(F.col("brand") == brand).select("item").distinct(),
                help="Choose the menu item to update prices"
            )

        # Enhanced data editor
        st.markdown("### Current Pricing Structure")
        set_prices = session.create_dataframe(
            st.data_editor(
                df.filter((F.col("brand") == brand) & (F.col("item") == item)),
                key="price_editor",
                use_container_width=True,
                hide_index=True,
                column_config={
                    "new_price": st.column_config.NumberColumn(
                        "New Price",
                        help="Enter the new price",
                        min_value=0,
                        max_value=1000,
                        step=0.5,
                        format="$%.2f"
                    )
                }
            )
        )

    with tab2:
        st.subheader("📈 Demand Forecast Analysis")
        
        # Calculate demand and profit metrics
        df_demand = set_prices.join(
            session.table("pricing_detail"), ["brand", "item", "day_of_week"]
        ).select(
            "day_of_week",
            "current_price_demand",
            "new_price",
            "item_cost",
            "average_basket_profit",
            "current_price_profit",
            (F.call_udf("udf_demand_price", [F.col(c) for c in feature_cols])).alias(
                "new_price_demand"
            ),
        )

        # Calculate demand lift
        demand_lift = df_demand.select(
            F.round(
                (
                    (F.sum("new_price_demand") - F.sum("current_price_demand"))
                    / F.sum("current_price_demand")
                )
                * 100,
                1,
            )
        ).collect()[0][0]

        # Calculate profit lift
        profit_lift = (
            df_demand.with_column(
                "new_price_profit",
                F.col("new_price_demand")
                * (F.col("new_price") - F.col("item_cost") + F.col("average_basket_profit")),
            )
            .select(
                F.round(
                    (
                        (F.sum("new_price_profit") - F.sum("current_price_profit"))
                        / F.sum("current_price_profit")
                    )
                    * 100,
                    1,
                )
            )
            .collect()[0][0]
        )

        # Enhanced metrics display
        col1, col2 = st.columns(2)
        with col1:
            st.metric(
                "Demand Lift",
                f"{demand_lift}%",
                help="Percentage change in expected demand"
            )
        with col2:
            st.metric(
                "Profit Lift",
                f"{profit_lift}%",
                help="Percentage change in expected profit"
            )

        # Enhanced demand visualization using Plotly
        df_demand_pd = df_demand.to_pandas()
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_demand_pd["DAY_OF_WEEK"],
            y=df_demand_pd["NEW_PRICE_DEMAND"],
            name="New Price Demand",
            line=dict(color="#00ff00", width=2)
        ))
        fig.add_trace(go.Scatter(
            x=df_demand_pd["DAY_OF_WEEK"],
            y=df_demand_pd["CURRENT_PRICE_DEMAND"],
            name="Current Price Demand",
            line=dict(color="#ff0000", width=2)
        ))
        
        fig.update_layout(
            title="Weekly Demand Comparison",
            xaxis_title="Day of Week",
            yaxis_title="Demand",
            plot_bgcolor="#2E2E2E",
            paper_bgcolor="#2E2E2E",
            font=dict(color="white")
        )
        
        st.plotly_chart(fig, use_container_width=True)

    # Action buttons
    if st.button("Update Prices", type="primary"):
        with st.spinner("Updating prices..."):
            set_prices.with_column("timestamp", F.current_timestamp()).write.mode(
                "append"
            ).save_as_table("pricing_final")
        st.success("Prices updated successfully!")

    # Historical view with enhanced styling
    with st.expander("📜 View Price History"):
        history_df = session.table("pricing_final").order_by(F.col("timestamp").desc())
        st.dataframe(
            history_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "timestamp": st.column_config.DatetimeColumn(
                    "Update Time",
                    format="DD-MM-YYYY HH:mm:ss"
                )
            }
        )

except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    st.info("Please check your Snowflake connection and try again.")

# Footer
st.markdown("---")
st.markdown("🎰 Casino F&B Pricing App © 2024 | Powered by Snowflake")
