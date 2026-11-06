import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parent
GOLD_ROOT = PROJECT_ROOT / "data" / "gold"

GOLD_DATASETS = [
    "order_facts",
    "order_line_revenue",
    "daily_revenue",
    "daily_orders",
    "products",
    "product_daily",
    "revenue_by_category",
    "session_facts",
    "customer_behaviour",
    "funnel_daily",
    "revenue_allocation_check",
]

def read_parquet_folder(folder: Path) -> pd.DataFrame:
    """Read Spark Parquet part-files from a dataset folder."""
    if not folder.exists():
        raise FileNotFoundError(f"Dataset folder not found: {folder}")
    files = [
        f for f in folder.rglob("*.parquet")
        if not f.name.endswith(".crc") and not f.name.startswith("._")
    ]
    if not files:
        raise FileNotFoundError(f"No Parquet files found in: {folder}")
    frames = [pd.read_parquet(file) for file in files]
    return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

@st.cache_data(show_spinner="Loading Gold Parquet datasets...")
def load_all_datasets():
    loaded = {}
    for name in GOLD_DATASETS:
        loaded[name] = read_parquet_folder(GOLD_ROOT / name)
    return loaded

def validate_datasets(data):
    rows = []
    for name, frame in data.items():
        rows.append({
            "dataset": name,
            "rows": len(frame),
            "columns": len(frame.columns),
            "missing_values": int(frame.isna().sum().sum()),
            "duplicate_rows": int(frame.duplicated().sum()) if not frame.empty else 0,
        })
    return pd.DataFrame(rows)

@st.cache_data(ttl=15, show_spinner=False)
def load_streaming_snapshot():
    """Read Member 2's validated Parquet output when it is included in the repo."""
    candidates = [
        PROJECT_ROOT / "member2_spark_streaming" / "member2_spark_streaming" / "data" / "lake" / "validated_events",
        PROJECT_ROOT / "member2_spark_streaming" / "data" / "lake" / "validated_events",
        PROJECT_ROOT / "data" / "validated_events",
    ]
    event_files = []
    for folder in candidates:
        if folder.exists():
            event_files.extend(
                f for f in folder.rglob("*.parquet")
                if not f.name.endswith(".crc") and not f.name.startswith("._")
            )
    if not event_files:
        raise FileNotFoundError(
            "Member 2 validated-events Parquet files were not found in the repository."
        )
    frames = [pd.read_parquet(file) for file in event_files]
    recent = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
    total = len(recent)
    if "event_type" in recent.columns:
        event_counts = (
            recent["event_type"].fillna("Unknown").value_counts()
            .rename_axis("event_type").reset_index(name="event_count")
        )
    else:
        event_counts = pd.DataFrame(columns=["event_type", "event_count"])
    timestamp_column = (
        "event_timestamp" if "event_timestamp" in recent.columns
        else "event_date" if "event_date" in recent.columns
        else None
    )
    per_minute = pd.DataFrame()
    if timestamp_column:
        recent[timestamp_column] = pd.to_datetime(recent[timestamp_column], errors="coerce")
        if timestamp_column == "event_timestamp":
            per_minute = (
                recent.dropna(subset=[timestamp_column])
                .set_index(timestamp_column)
                .resample("1min").size()
                .rename("event_count").reset_index()
                .rename(columns={timestamp_column: "event_minute"})
            )
    if timestamp_column:
        recent = recent.sort_values(timestamp_column, ascending=False, na_position="last")
    return total, recent.head(500), event_counts, per_minute, list(recent.columns)


# =========================================================
# 1. PAGE CONFIGURATION
# =========================================================

st.set_page_config(
    page_title="E-Commerce Intelligence",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded"
)


# =========================================================
# 2. LIGHT CORPORATE DESIGN
# =========================================================

st.markdown("""
<style>

/* Main application */
.stApp {
    background-color: #F5F7FB;
    color: #172033;
}

/* Main content */
.block-container {
    padding-top: 2rem;
    padding-bottom: 3rem;
    max-width: 1500px;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background-color: #FFFFFF;
    border-right: 1px solid #E5EAF2;
}

[data-testid="stSidebar"] > div:first-child {
    padding-top: 1.5rem;
}

/* Headings */
h1, h2, h3 {
    color: #172033 !important;
    font-weight: 700 !important;
}

p, label {
    color: #526078;
}

/* KPI cards */
[data-testid="stMetric"] {
    background-color: #FFFFFF;
    border: 1px solid #E6EAF1;
    padding: 20px;
    border-radius: 12px;
    box-shadow: 0 2px 8px rgba(20, 35, 60, 0.04);
}

[data-testid="stMetricLabel"] {
    color: #64748B !important;
    font-size: 14px;
}

[data-testid="stMetricValue"] {
    color: #172033 !important;
    font-size: 26px;
    font-weight: 700;
}

/* Navigation radio */
[data-testid="stSidebar"] [data-testid="stRadio"] label {
    color: #34435B;
    font-weight: 500;
}

/* Buttons */
.stButton > button {
    border-radius: 8px;
    border: 1px solid #D8E0EC;
    font-weight: 600;
}

/* Dataframes */
[data-testid="stDataFrame"] {
    border: 1px solid #E6EAF1;
    border-radius: 10px;
}

/* Dividers */
hr {
    border-color: #E5EAF2;
}

/* Small status labels */
.status-text {
    color: #64748B;
    font-size: 12px;
}

</style>
""", unsafe_allow_html=True)


# =========================================================
# 3. LOAD GOLD DATA FROM LOCAL REPOSITORY PARQUET FILES
# =========================================================

@st.cache_data(show_spinner="Loading E-Commerce datasets...")
def get_data():
    return load_all_datasets()

def refresh_data():
    get_data.clear()
    load_streaming_snapshot.clear()
    st.rerun()

try:
    data = get_data()
except Exception as error:
    st.error("Unable to load the Gold datasets from data/gold.")
    st.exception(error)
    st.info(
        "Check that all 11 Gold dataset folders are committed under "
        "data/gold/ and that requirements.txt includes pyarrow."
    )
    st.stop()


# Dataset references
orders = data["order_facts"]
order_lines = data["order_line_revenue"]
daily_revenue = data["daily_revenue"]
daily_orders = data["daily_orders"]
products = data["products"]
product_daily = data["product_daily"]
categories = data["revenue_by_category"]
sessions = data["session_facts"]
customers = data["customer_behaviour"]
funnel = data["funnel_daily"]
allocation = data["revenue_allocation_check"]


# =========================================================
# 4. HELPER FUNCTIONS
# =========================================================

def numeric_sum(df, column):
    if column not in df.columns:
        return 0.0

    return pd.to_numeric(
        df[column], errors="coerce"
    ).fillna(0).sum()


def format_currency(value):
    return f"₹{value:,.2f}"


def format_number(value):
    return f"{value:,.0f}"


def prepare_chart(fig):
    fig.update_layout(
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        font=dict(
            family="Arial",
            color="#34435B"
        ),
        margin=dict(l=20, r=20, t=35, b=20),
        legend=dict(
            bgcolor="rgba(255,255,255,0)"
        ),
        hoverlabel=dict(
            bgcolor="#FFFFFF",
            font_color="#172033"
        )
    )

    fig.update_xaxes(
        showgrid=False,
        linecolor="#E5EAF2"
    )

    fig.update_yaxes(
        gridcolor="#EEF1F6",
        zerolinecolor="#E5EAF2"
    )

    return fig


def section_header(title, description=None):
    st.subheader(title)

    if description:
        st.caption(description)



# =========================================================
# 4A. STREAMING EVENTS SOURCE
# =========================================================
# Streaming Monitor reads the committed Member 2 validated Parquet snapshot.
# It does not require a PostgreSQL server or cloud database credentials.


# =========================================================
# 5. BUSINESS METRICS
# =========================================================

net_revenue = numeric_sum(orders, "net_revenue")
paid_revenue = numeric_sum(daily_revenue, "gross_paid_revenue")

total_orders = len(orders)
total_customers = len(customers)
total_products = len(products)
total_sessions = len(sessions)

paid_orders = int(numeric_sum(daily_revenue, "paid_orders"))


# =========================================================
# 6. SIDEBAR NAVIGATION
# =========================================================

with st.sidebar:

    st.markdown(
        """
        <div style="padding: 8px 0 15px 0;">
            <div style="font-size:27px;font-weight:800;color:#17345C;">
                🛍️ E-COMMERCE
            </div>
            <div style="font-size:11px;letter-spacing:2px;
                        color:#8190A7;margin-top:4px;">
                INTELLIGENCE PLATFORM
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.divider()

    st.markdown("#### WORKSPACE")

    selected_page = st.radio(
        "Navigation",
        [
            "Executive Overview",
            "Revenue Analytics",
            "Product Analytics",
            "Customer Analytics",
            "Order Analytics",
            "Conversion Funnel",
            "Streaming Monitor",
            "Data Quality"
        ],
        label_visibility="collapsed"
    )

    st.divider()

    st.markdown("#### DATA CONNECTION")

    st.success("Gold Parquet Connected")
    st.caption("Source: data/gold — 11 Gold datasets")
    st.button(
        "↻ Refresh Data",
        use_container_width=True,
        on_click=refresh_data
    )

    st.divider()

    st.markdown(
        """
        <div style="font-size:11px;color:#8190A7;">
            PROJECT<br>
            <b style="color:#526078;">
            REAL-TIME E-COMMERCE DATA ENGINEERING
            </b>
            <br><br>
            DASHBOARD<br>
            <b style="color:#526078;">MEMBER 5</b>
        </div>
        """,
        unsafe_allow_html=True
    )


# =========================================================
# 7. COMMON PAGE HEADER
# =========================================================

st.markdown(
    """
    <div style="display:flex;justify-content:space-between;
                align-items:center;flex-wrap:wrap;">
        <div>
            <div style="font-size:12px;color:#8190A7;
                        letter-spacing:1.5px;">
                E-COMMERCE / BUSINESS INTELLIGENCE
            </div>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)


# =========================================================
# 8. EXECUTIVE OVERVIEW
# =========================================================

if selected_page == "Executive Overview":

    st.title("Executive Overview")

    st.caption(
        "Monitor business performance and key e-commerce metrics."
    )

    st.divider()

    section_header("Business Performance")

    # KPI row 1
    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Net Revenue",
        format_currency(net_revenue)
    )

    c2.metric(
        "Gross Paid Revenue",
        format_currency(paid_revenue)
    )

    c3.metric(
        "Total Orders",
        format_number(total_orders)
    )

    c4.metric(
        "Total Customers",
        format_number(total_customers)
    )

    # KPI row 2
    c5, c6, c7 = st.columns(3)

    c5.metric("Total Products", format_number(total_products))
    c6.metric("Total Sessions", format_number(total_sessions))
    c7.metric("Paid Orders", format_number(paid_orders))

    st.divider()

    # Revenue charts
    section_header("Revenue Performance")

    left, right = st.columns(2)

    with left:

        st.markdown("**Revenue Summary**")

        if not daily_revenue.empty and "payment_date" in daily_revenue.columns:

            chart_data = daily_revenue.copy()

            chart_data["payment_date"] = pd.to_datetime(
                chart_data["payment_date"],
                errors="coerce"
            )

            revenue_columns = [
                col for col in [
                    "gross_paid_revenue",
                    "net_revenue"
                ]
                if col in chart_data.columns
            ]

            if revenue_columns:

                fig = px.bar(
                    chart_data,
                    x="payment_date",
                    y=revenue_columns,
                    barmode="group",
                    labels={
                        "payment_date": "Payment Date",
                        "value": "Revenue (₹)",
                        "variable": "Revenue Type"
                    }
                )

                st.plotly_chart(
                    prepare_chart(fig),
                    use_container_width=True
                )

            else:
                st.info("Revenue columns are unavailable.")

        else:
            st.info("No daily revenue records available.")

    with right:

        st.markdown("**Revenue by Category**")

        if not categories.empty:

            fig = px.pie(
                categories,
                names="category",
                values="allocated_revenue",
                hole=0.58
            )

            st.plotly_chart(
                prepare_chart(fig),
                use_container_width=True
            )

        else:
            st.info("No category data available.")

    st.divider()

    # Top products
    section_header("Top Performing Products")

    if not products.empty:

        top_products = products.sort_values(
            "allocated_revenue",
            ascending=False
        ).head(10)

        fig = px.bar(
            top_products,
            x="product_id",
            y="allocated_revenue",
            labels={
                "product_id": "Product ID",
                "allocated_revenue": "Allocated Revenue (₹)"
            }
        )

        st.plotly_chart(
            prepare_chart(fig),
            use_container_width=True
        )

        st.dataframe(
            top_products,
            use_container_width=True,
            hide_index=True
        )

    st.caption(
        "Data source: Gold Parquet snapshot. "
        "This overview is not yet connected to a live cloud stream."
    )


# =========================================================
# 9. REVENUE ANALYTICS
# =========================================================

elif selected_page == "Revenue Analytics":

    st.title("Revenue Analytics")

    st.caption("Understand revenue, refunds and revenue allocation.")

    st.divider()

    c1, c2, c3 = st.columns(3)

    c1.metric("Gross Paid Revenue", format_currency(paid_revenue))
    c2.metric("Net Revenue", format_currency(net_revenue))

    refunds = numeric_sum(daily_revenue, "refunds")
    c3.metric("Refunds", format_currency(refunds))

    st.divider()

    left, right = st.columns(2)

    with left:

        st.markdown("**Daily Revenue**")

        if not daily_revenue.empty:

            chart = daily_revenue.copy()

            chart["payment_date"] = pd.to_datetime(
                chart["payment_date"],
                errors="coerce"
            )

            fig = px.bar(
                chart,
                x="payment_date",
                y=[
                    c for c in [
                        "gross_paid_revenue",
                        "refunds",
                        "net_revenue"
                    ]
                    if c in chart.columns
                ],
                barmode="group"
            )

            st.plotly_chart(
                prepare_chart(fig),
                use_container_width=True
            )

    with right:

        st.markdown("**Category Revenue Contribution**")

        fig = px.pie(
            categories,
            names="category",
            values="allocated_revenue",
            hole=0.5
        )

        st.plotly_chart(
            prepare_chart(fig),
            use_container_width=True
        )

    st.markdown("**Revenue Allocation Validation**")

    st.dataframe(
        allocation,
        use_container_width=True,
        hide_index=True
    )

    st.markdown("**Daily Revenue Records**")

    st.dataframe(
        daily_revenue,
        use_container_width=True,
        hide_index=True
    )


# =========================================================
# 10. PRODUCT ANALYTICS
# =========================================================

elif selected_page == "Product Analytics":

    st.title("Product Analytics")

    st.caption("Explore product-level and category-level performance.")

    st.divider()

    c1, c2 = st.columns(2)

    c1.metric("Total Products", format_number(total_products))

    c2.metric(
        "Product Revenue",
        format_currency(numeric_sum(products, "allocated_revenue"))
    )

    st.divider()

    st.markdown("**Category Performance**")

    category_filter = st.selectbox(
        "Select Category",
        ["All Categories"] + sorted(
            categories["category"].dropna().astype(str).unique().tolist()
        )
    )

    filtered_categories = categories.copy()

    if category_filter != "All Categories":
        filtered_categories = filtered_categories[
            filtered_categories["category"].astype(str) == category_filter
        ]

    fig = px.bar(
        filtered_categories,
        x="category",
        y="allocated_revenue",
        labels={
            "category": "Category",
            "allocated_revenue": "Allocated Revenue (₹)"
        }
    )

    st.plotly_chart(
        prepare_chart(fig),
        use_container_width=True
    )

    st.markdown("**Product Performance Table**")

    st.dataframe(
        products.sort_values(
            "allocated_revenue",
            ascending=False
        ),
        use_container_width=True,
        hide_index=True
    )

    st.markdown("**Product Daily Records**")

    st.dataframe(
        product_daily,
        use_container_width=True,
        hide_index=True
    )


# =========================================================
# 11. CUSTOMER ANALYTICS
# =========================================================

elif selected_page == "Customer Analytics":

    st.title("Customer Analytics")

    st.caption("Understand customer activity and session behaviour.")

    st.divider()

    c1, c2, c3 = st.columns(3)

    c1.metric("Customers", format_number(total_customers))

    c2.metric(
        "Total Customer Events",
        format_number(numeric_sum(customers, "total_events"))
    )

    c3.metric(
        "Customer Sessions",
        format_number(numeric_sum(customers, "sessions"))
    )

    st.divider()

    st.markdown("**Most Active Customers**")

    top_customers = customers.sort_values(
        "total_events",
        ascending=False
    ).head(10)

    fig = px.bar(
        top_customers,
        x="customer_id",
        y="total_events",
        labels={
            "customer_id": "Customer ID",
            "total_events": "Total Events"
        }
    )

    st.plotly_chart(
        prepare_chart(fig),
        use_container_width=True
    )

    st.markdown("**Customer Behaviour Records**")

    st.dataframe(
        customers,
        use_container_width=True,
        hide_index=True
    )

    st.markdown("**Session Analytics**")

    st.dataframe(
        sessions,
        use_container_width=True,
        hide_index=True
    )


# =========================================================
# 12. ORDER ANALYTICS
# =========================================================

elif selected_page == "Order Analytics":

    st.title("Order Analytics")

    st.caption("Explore payment, refunds and order performance.")

    st.divider()

    c1, c2, c3 = st.columns(3)

    c1.metric("Total Orders", format_number(total_orders))

    c2.metric(
        "Paid Orders",
        format_number(paid_orders)
    )

    c3.metric(
        "Net Revenue",
        format_currency(net_revenue)
    )

    st.divider()

    if "final_order_status" in orders.columns:

        st.markdown("**Order Status Distribution**")

        status_data = (
            orders["final_order_status"]
            .fillna("Unknown")
            .value_counts()
            .rename_axis("Status")
            .reset_index(name="Orders")
        )

        fig = px.pie(
            status_data,
            names="Status",
            values="Orders",
            hole=0.5
        )

        st.plotly_chart(
            prepare_chart(fig),
            use_container_width=True
        )

    st.markdown("**Order Records**")

    st.dataframe(
        orders,
        use_container_width=True,
        hide_index=True
    )

    st.markdown("**Order Line Revenue**")

    st.dataframe(
        order_lines,
        use_container_width=True,
        hide_index=True
    )


# =========================================================
# 13. CONVERSION FUNNEL
# =========================================================

elif selected_page == "Conversion Funnel":

    st.title("Conversion Funnel")

    st.caption("Explore aggregate customer activity across funnel stages.")

    st.divider()

    funnel_columns = {
        "search_sessions": "Search",
        "view_sessions": "Product Views",
        "cart_sessions": "Add to Cart",
        "purchase_sessions": "Purchases"
    }

    if not funnel.empty:

        available = {
            label: numeric_sum(funnel, column)
            for column, label in funnel_columns.items()
            if column in funnel.columns
        }

        funnel_data = pd.DataFrame({
            "Stage": list(available.keys()),
            "Sessions": list(available.values())
        })

        cols = st.columns(len(funnel_data))

        for col, (_, row) in zip(cols, funnel_data.iterrows()):
            col.metric(
                row["Stage"],
                format_number(row["Sessions"])
            )

        fig = px.bar(
            funnel_data,
            x="Stage",
            y="Sessions",
            text="Sessions"
        )

        st.plotly_chart(
            prepare_chart(fig),
            use_container_width=True
        )

        st.dataframe(
            funnel,
            use_container_width=True,
            hide_index=True
        )

        st.info(
            "These values are aggregate stage counts from the Gold dataset. "
            "They are not sequential session-level conversion rates."
        )

    else:
        st.warning("No funnel records are currently available.")



# =========================================================
# 14. STREAMING MONITOR
# =========================================================

elif selected_page == "Streaming Monitor":

    st.title("Streaming Monitor")
    st.caption(
        "Inspect the validated event snapshot produced by Member 2."
    )
    st.divider()

    refresh_col, info_col = st.columns([1, 3])
    with refresh_col:
        if st.button("↻ Refresh streaming data", use_container_width=True):
            load_streaming_snapshot.clear()
            st.rerun()
    with info_col:
        st.caption(
            "Reads the validated Parquet snapshot included in this repository."
        )

    try:
        stream_total, stream_recent, stream_types, stream_per_minute, stream_columns = (
            load_streaming_snapshot()
        )

        # Overall event KPIs
        k1, k2, k3 = st.columns(3)
        k1.metric("Total Stored Events", format_number(stream_total))
        k2.metric(
            "Event Types",
            format_number(stream_types["event_type"].nunique())
            if "event_type" in stream_types.columns else "—"
        )
        k3.metric("Recent Records Loaded", format_number(len(stream_recent)))

        st.divider()

        left, right = st.columns(2)

        with left:
            st.markdown("**Events by Type**")
            if not stream_types.empty:
                fig = px.bar(
                    stream_types,
                    x="event_type",
                    y="event_count",
                    text="event_count",
                    labels={
                        "event_type": "Event Type",
                        "event_count": "Events",
                    },
                )
                st.plotly_chart(
                    prepare_chart(fig),
                    use_container_width=True,
                )
            else:
                st.info("No event-type records are available.")

        with right:
            st.markdown("**Events per Minute — Last 60 Minutes**")
            if not stream_per_minute.empty:
                fig = px.line(
                    stream_per_minute,
                    x="event_minute",
                    y="event_count",
                    markers=True,
                    labels={
                        "event_minute": "Time",
                        "event_count": "Events",
                    },
                )
                st.plotly_chart(
                    prepare_chart(fig),
                    use_container_width=True,
                )
            else:
                st.info(
                    "No timestamped events were found in the last 60 minutes. "
                    "The table may contain an older batch snapshot."
                )

        st.divider()
        st.markdown("**Latest Streaming Events**")

        if not stream_recent.empty:
            display = stream_recent.copy()
            if "event_timestamp" in display.columns:
                display["event_timestamp"] = pd.to_datetime(
                    display["event_timestamp"], errors="coerce"
                )
            st.dataframe(
                display,
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("The streaming_events table is currently empty.")

        st.caption(
            "Source: Member 2 validated-events Parquet snapshot. "
            "This is a stored snapshot, not a continuously updating cloud stream."
        )

    except Exception as error:
        st.error("Unable to load streaming events from PostgreSQL.")
        st.exception(error)
        st.info(
            "Include Member 2's validated-events Parquet folder in the repository. "
            "The other dashboard pages will continue to work without it."
        )


# =========================================================
# 15. DATA QUALITY
# =========================================================

elif selected_page == "Data Quality":

    st.title("Data Quality Monitor")

    st.caption("Inspect the completeness and consistency of Gold datasets.")

    st.divider()

    validation = validate_datasets(data)

    total_rows = int(validation["rows"].sum())

    total_missing = int(validation["missing_values"].sum())

    total_duplicates = int(validation["duplicate_rows"].sum())

    c1, c2, c3 = st.columns(3)

    c1.metric("Datasets", len(validation))

    c2.metric("Total Records", format_number(total_rows))

    c3.metric("Missing Values", format_number(total_missing))

    st.divider()

    st.markdown("**Dataset Validation Summary**")

    st.dataframe(
        validation,
        use_container_width=True
    )

    st.markdown("**Duplicate Records**")

    st.metric(
        "Duplicate Rows Across Datasets",
        format_number(total_duplicates)
    )

    if total_missing == 0 and total_duplicates == 0:
        st.success(
            "All loaded Gold datasets passed the basic missing-value "
            "and duplicate-row checks."
        )
    else:
        st.warning(
            "Some data-quality issues were detected. "
            "Review the validation table."
        )

    st.caption(
        "These are dashboard-level checks, not a replacement "
        "for the complete pipeline validation."
    )


# =========================================================
# 16. FOOTER
# =========================================================

st.divider()

st.markdown(
    """
    <div style="text-align:center;color:#8190A7;font-size:12px;">
        E-COMMERCE INTELLIGENCE PLATFORM
        <br>
        Data Engineering & Business Analytics
        <br><br>
        Dashboard Development – Member 5
    </div>
    """,
    unsafe_allow_html=True
)
