import os
from pathlib import Path
from datetime import date, datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="E-Commerce Intelligence",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

st.markdown("""
<style>
.stApp {background:#f5f7fb;}

/* Sidebar */
[data-testid="stSidebar"] {background:#101828;}
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h1,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h2,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h3,
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] .stCaption,
[data-testid="stSidebar"] [data-testid="stWidgetLabel"] {
    color:#eef2ff !important;
}
/* Date inputs: dark text on the light input surface */
[data-testid="stSidebar"] input {
    color:#172033 !important;
    -webkit-text-fill-color:#172033 !important;
    background:#ffffff !important;
    caret-color:#172033 !important;
}
[data-testid="stSidebar"] [data-testid="stDateInput"] button {
    color:#172033 !important;
}
[data-testid="stSidebar"] [data-testid="stDateInput"] svg {
    fill:#344054 !important;
}
/* High-contrast refresh button */
[data-testid="stSidebar"] .stButton > button {
    background:#2563eb !important;
    border:1px solid #2563eb !important;
    color:#ffffff !important;
    font-weight:700 !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background:#1d4ed8 !important;
    border-color:#1d4ed8 !important;
    color:#ffffff !important;
}
/* Connection badge must remain readable despite sidebar styles */
[data-testid="stSidebar"] .status {
    background:#ecfdf3 !important;
    color:#027a48 !important;
    border:1px solid #abefc6;
    border-radius:9px;
    padding:10px 12px;
    font-weight:700;
    text-align:center;
}

/* Main dashboard */
.hero {padding:22px 26px;border-radius:16px;background:linear-gradient(115deg,#14213d,#1d4ed8);color:white;margin-bottom:18px;}
.hero h1 {color:white;margin:0;font-size:2rem;}
.hero p {color:#dbeafe;margin:7px 0 0 0;}
.section-title {font-size:1.2rem;font-weight:700;color:#172554;margin:12px 0 8px;}
div[data-testid="stMetric"] {
    min-width:0;
    background:white;
    border:1px solid #e6eaf2;
    padding:16px 18px;
    border-radius:13px;
    box-shadow:0 2px 8px rgba(16,24,40,.04);
}
div[data-testid="stMetric"] label {color:#667085;}
div[data-testid="stMetricValue"] {
    color:#101828;
    font-weight:700;
    font-size:clamp(1.05rem, 1.65vw, 1.8rem);
    line-height:1.25;
    white-space:normal !important;
    overflow-wrap:anywhere;
    word-break:normal;
}
div[data-testid="stMetricValue"] > div {
    white-space:normal !important;
    overflow-wrap:anywhere;
}
footer {visibility:hidden;}
</style>
""", unsafe_allow_html=True)

BLUE, TEAL, PURPLE, ORANGE = "#2563eb", "#0f766e", "#7c3aed", "#ea580c"


@st.cache_resource
def get_engine():
    required = ["DB_USER", "DB_PASSWORD", "DB_NAME"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise ValueError("Missing .env settings: " + ", ".join(missing))
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5433")),
        database=os.getenv("DB_NAME"),
    )
    return create_engine(url, pool_pre_ping=True)


@st.cache_data(ttl=30, show_spinner=False)
def load_data(query):
    with get_engine().connect() as conn:
        return pd.read_sql(text(query), conn)


def money(value):
    return f"₹{float(value or 0):,.2f}"


def number(value):
    return f"{int(value or 0):,}"


def filter_dates(frame, column, start, end):
    if frame.empty or column not in frame.columns:
        return frame.copy()
    result = frame.copy()
    result[column] = pd.to_datetime(result[column], errors="coerce").dt.date
    return result[result[column].between(start, end)].copy()


def style_chart(fig, height=360):
    fig.update_layout(
        template="plotly_white", height=height,
        margin=dict(l=15, r=15, t=55, b=15),
        paper_bgcolor="white", plot_bgcolor="white",
        font=dict(family="Arial", color="#344054"),
        legend_title_text="",
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="#eef2f6", zeroline=False)
    return fig


try:
    with st.spinner("Connecting to PostgreSQL and loading analytics..."):
        order_facts = load_data("SELECT * FROM order_facts")
        daily_revenue = load_data("SELECT * FROM daily_revenue ORDER BY payment_date")
        daily_orders = load_data("SELECT * FROM daily_orders ORDER BY event_date")
        products = load_data("SELECT * FROM products")
        product_daily = load_data("SELECT * FROM product_daily ORDER BY payment_date")
        categories = load_data("SELECT * FROM revenue_by_category")
        sessions = load_data("SELECT * FROM session_facts")
        customers = load_data("SELECT * FROM customer_behaviour")
        funnel = load_data("SELECT * FROM funnel_daily ORDER BY event_date")
except Exception as error:
    st.error("Unable to load dashboard data from PostgreSQL.")
    st.code(str(error))
    st.info("Check Docker Desktop and DB_HOST, DB_PORT, DB_NAME, DB_USER and DB_PASSWORD in .env.")
    st.stop()


with st.sidebar:
    st.markdown("## 🛒 E-COMMERCE")
    st.caption("ANALYTICS & INTELLIGENCE")
    st.divider()
    page = st.radio("WORKSPACE", [
        "Overview", "Revenue Analytics", "Product Analytics",
        "Customer Analytics", "Conversion Funnel"
    ])
    st.divider()
    st.markdown("### 📅 Date range")

    dates = []
    for frame, col in [(daily_revenue, "payment_date"), (daily_orders, "event_date")]:
        if not frame.empty and col in frame.columns:
            dates.extend(pd.to_datetime(frame[col], errors="coerce").dropna().dt.date.tolist())

    if dates:
        min_date, max_date = min(dates), max(dates)
    else:
        max_date, min_date = date.today(), date.today() - timedelta(days=30)

    start_date = st.date_input("From", value=min_date, min_value=min_date, max_value=max_date)
    end_date = st.date_input("To", value=max_date, min_value=min_date, max_value=max_date)
    if start_date > end_date:
        st.error("'From' date cannot be after 'To' date.")
        st.stop()

    st.divider()
    if st.button("🔄 Refresh dashboard", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    auto_refresh = st.checkbox("Enable automatic refresh", value=False)
    refresh_seconds = st.selectbox(
        "Refresh interval",
        options=[15, 30, 60],
        index=1,
        format_func=lambda seconds: f"Every {seconds} seconds",
        disabled=not auto_refresh,
    )
    if auto_refresh:
        st_autorefresh(
            interval=refresh_seconds * 1000,
            key="ecommerce_dashboard_autorefresh",
        )

    st.markdown('<div class="status">● PostgreSQL Connected</div>', unsafe_allow_html=True)
    st.caption("Data cache: 30 seconds")
    st.caption(f"Last page refresh: {datetime.now().strftime('%d %b %Y, %I:%M:%S %p')}")


revenue_f = filter_dates(daily_revenue, "payment_date", start_date, end_date)
orders_f = filter_dates(daily_orders, "event_date", start_date, end_date)
product_daily_f = filter_dates(product_daily, "payment_date", start_date, end_date)
funnel_f = filter_dates(funnel, "event_date", start_date, end_date)

gross = revenue_f["gross_paid_revenue"].fillna(0).sum() if not revenue_f.empty else 0
net = revenue_f["net_revenue"].fillna(0).sum() if not revenue_f.empty else 0
refunds = revenue_f["refunds"].fillna(0).sum() if not revenue_f.empty else 0
cancelled = revenue_f["cancelled_after_payment"].fillna(0).sum() if not revenue_f.empty else 0
paid_orders = revenue_f["paid_orders"].fillna(0).sum() if not revenue_f.empty else 0

st.markdown(f"""
<div class="hero">
<h1>🛒 E-Commerce Intelligence Dashboard</h1>
<p>Real-time data platform · Business performance · Customer insights</p>
<p style="font-size:.85rem;">Reporting period: {start_date:%d %b %Y} — {end_date:%d %b %Y}</p>
</div>
""", unsafe_allow_html=True)


if page == "Overview":
    st.header("Business Overview")
    st.caption("A snapshot of revenue, orders, products and customer activity.")
    a, b, c, d = st.columns(4)
    a.metric("Gross paid revenue", money(gross))
    b.metric("Net revenue", money(net))
    c.metric("Paid orders", number(paid_orders))
    d.metric("Refunds", money(refunds))
    e, f, g, h = st.columns(4)
    e.metric("Products", number(len(products)))
    f.metric("Customers", number(len(customers)))
    g.metric("Sessions", number(len(sessions)))
    h.metric("Cancelled after payment", money(cancelled))

    st.markdown('<div class="section-title">Revenue performance</div>', unsafe_allow_html=True)
    left, right = st.columns([1.5, 1])
    with left:
        if not revenue_f.empty:
            plot = revenue_f.copy()
            plot["payment_date_label"] = pd.to_datetime(
                plot["payment_date"], errors="coerce"
            ).dt.strftime("%d %b %Y")
            fig = px.bar(
                plot, x="payment_date_label",
                y=["gross_paid_revenue", "net_revenue"],
                barmode="group", title="Gross revenue vs net revenue",
                labels={
                    "payment_date_label": "Payment date",
                    "value": "Revenue (₹)",
                    "variable": "Revenue type",
                },
                color_discrete_sequence=[BLUE, TEAL],
            )
            st.plotly_chart(style_chart(fig, 390), use_container_width=True)
            if len(plot) == 1:
                st.caption("Only one day of revenue data is available. A multi-day trend will appear when more daily records are loaded.")
        else:
            st.info("No revenue records in the selected date range.")
    with right:
        if not categories.empty:
            fig = px.pie(categories, names="category", values="allocated_revenue",
                         hole=.58, title="Revenue by category (all available data)",
                         color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_traces(textposition="inside", textinfo="percent")
            st.plotly_chart(style_chart(fig, 390), use_container_width=True)
        else:
            st.info("No category data available.")

    st.markdown('<div class="section-title">Top products</div>', unsafe_allow_html=True)
    if not products.empty:
        top = products.nlargest(8, "allocated_revenue").sort_values("allocated_revenue")
        fig = px.bar(top, x="allocated_revenue", y="product_id", orientation="h",
                     title="Top 8 products by allocated revenue",
                     labels={"allocated_revenue":"Allocated revenue (₹)","product_id":"Product"},
                     color="allocated_revenue", color_continuous_scale="Blues")
        st.plotly_chart(style_chart(fig, 390), use_container_width=True)
    else:
        st.info("No product data available.")


elif page == "Revenue Analytics":
    st.header("💰 Revenue Analytics")
    a, b, c, d = st.columns(4)
    a.metric("Gross paid", money(gross))
    b.metric("Net revenue", money(net))
    c.metric("Refunds", money(refunds))
    d.metric("Paid orders", number(paid_orders))
    if not revenue_f.empty:
        plot = revenue_f.copy()
        plot["payment_date"] = pd.to_datetime(plot["payment_date"], errors="coerce")
        plot = plot.dropna(subset=["payment_date"]).sort_values("payment_date")
        if len(plot) == 1:
            plot["payment_date_label"] = plot["payment_date"].dt.strftime("%d %b %Y")
            fig = px.bar(
                plot, x="payment_date_label", y="net_revenue", text_auto=".2s",
                title="Net revenue snapshot — one available day",
                labels={"net_revenue": "Net revenue (₹)", "payment_date_label": "Payment date"},
                color_discrete_sequence=[BLUE],
            )
            st.caption("Only one daily revenue record is available. This is a snapshot, not a time trend. A trend line will appear once more dates are loaded.")
        else:
            fig = px.line(
                plot, x="payment_date", y="net_revenue", markers=True,
                title="Daily net revenue trend",
                labels={"net_revenue": "Net revenue (₹)", "payment_date": "Payment date"},
                color_discrete_sequence=[BLUE],
            )
            fig.update_traces(line_width=3, marker_size=8)
        st.plotly_chart(style_chart(fig, 380), use_container_width=True)

        adjustment = plot.melt(id_vars=["payment_date"],
                               value_vars=["refunds","cancelled_after_payment"],
                               var_name="Adjustment", value_name="Amount")
        fig2 = px.bar(adjustment, x="payment_date", y="Amount", color="Adjustment",
                      barmode="group", title="Refunds and cancellations after payment",
                      color_discrete_sequence=[ORANGE, PURPLE])
        st.plotly_chart(style_chart(fig2, 350), use_container_width=True)
        st.subheader("Daily revenue records")
        st.dataframe(revenue_f, use_container_width=True, hide_index=True)
    else:
        st.info("No revenue records found for this date range.")


elif page == "Product Analytics":
    st.header("📦 Product Analytics")
    st.caption("Product performance and revenue contribution.")
    if not products.empty:
        total_product_revenue = products["allocated_revenue"].fillna(0).sum()
        top_product = products.sort_values("allocated_revenue", ascending=False).iloc[0]
        a, b, c = st.columns(3)
        a.metric("Products tracked", number(len(products)))
        b.metric("Allocated revenue", money(total_product_revenue))
        c.metric("Top product", str(top_product["product_id"]))

        left, right = st.columns([1.4, 1])
        top = products.nlargest(10, "allocated_revenue").sort_values("allocated_revenue")
        with left:
            fig = px.bar(top, x="allocated_revenue", y="product_id", orientation="h",
                         title="Top 10 products by allocated revenue",
                         labels={"allocated_revenue":"Revenue (₹)","product_id":"Product"},
                         color="allocated_revenue", color_continuous_scale="Blues")
            st.plotly_chart(style_chart(fig, 440), use_container_width=True)
        with right:
            if not categories.empty:
                fig2 = px.pie(categories, names="category", values="allocated_revenue",
                              hole=.5, title="Revenue share by category",
                              color_discrete_sequence=px.colors.qualitative.Pastel)
                fig2.update_traces(textinfo="percent+label")
                st.plotly_chart(style_chart(fig2, 440), use_container_width=True)

        st.subheader("Product performance table")
        st.dataframe(products.sort_values("allocated_revenue", ascending=False),
                     use_container_width=True, hide_index=True)
        if not product_daily_f.empty:
            st.subheader("Daily product revenue")
            fig3 = px.line(product_daily_f, x="payment_date", y="allocated_revenue",
                           color="product_id", markers=True,
                           title="Product revenue over selected dates")
            st.plotly_chart(style_chart(fig3, 400), use_container_width=True)
    else:
        st.info("No product data available.")


elif page == "Customer Analytics":
    st.header("👥 Customer Analytics")
    st.caption("Understand customer activity and session behaviour.")
    avg_events = customers["total_events"].mean() if not customers.empty else 0
    avg_sessions = customers["sessions"].mean() if not customers.empty and "sessions" in customers else 0
    a, b, c = st.columns(3)
    a.metric("Customers", number(len(customers)))
    b.metric("Sessions", number(len(sessions)))
    c.metric("Avg. events per customer", f"{avg_events:,.1f}")
    st.metric("Avg. sessions per customer", f"{avg_sessions:,.1f}")

    if not customers.empty:
        top = customers.nlargest(10, "total_events").sort_values("total_events")
        fig = px.bar(top, x="total_events", y="customer_id", orientation="h",
                     title="Top 10 customers by activity",
                     labels={"total_events":"Total events","customer_id":"Customer"},
                     color="total_events", color_continuous_scale="Teal")
        st.plotly_chart(style_chart(fig, 420), use_container_width=True)
        st.subheader("Customer behaviour")
        st.dataframe(top, use_container_width=True, hide_index=True)
    else:
        st.info("No customer behaviour records available.")
    if not sessions.empty:
        st.subheader("Recent session facts")
        st.dataframe(sessions.head(25), use_container_width=True, hide_index=True)


elif page == "Conversion Funnel":
    st.header("🔄 Shopping Journey Analysis")
    st.caption("Daily aggregate activity across the main shopping journey.")

    if not funnel_f.empty:
        required = [
            "search_sessions", "view_sessions",
            "cart_sessions", "purchase_sessions",
        ]
        missing_columns = [column for column in required if column not in funnel_f.columns]

        if missing_columns:
            st.error("The funnel_daily table is missing required columns: " + ", ".join(missing_columns))
        else:
            totals = {
                "Search": int(funnel_f["search_sessions"].fillna(0).sum()),
                "Product view": int(funnel_f["view_sessions"].fillna(0).sum()),
                "Add to cart": int(funnel_f["cart_sessions"].fillna(0).sum()),
                "Purchase": int(funnel_f["purchase_sessions"].fillna(0).sum()),
            }

            a, b, c, d = st.columns(4)
            a.metric("Search-stage sessions", number(totals["Search"]))
            b.metric("View-stage sessions", number(totals["Product view"]))
            c.metric("Cart-stage sessions", number(totals["Add to cart"]))
            d.metric("Purchase-stage sessions", number(totals["Purchase"]))

            stage_data = pd.DataFrame({
                "Stage": list(totals.keys()),
                "Recorded session activity": list(totals.values()),
            })
            fig = px.bar(
                stage_data,
                x="Stage",
                y="Recorded session activity",
                text="Recorded session activity",
                title="Recorded activity by shopping stage",
                color="Stage",
                color_discrete_sequence=[BLUE, "#60a5fa", TEAL, PURPLE],
            )
            fig.update_traces(textposition="outside", cliponaxis=False)
            st.plotly_chart(style_chart(fig, 410), use_container_width=True)

            st.subheader("Stage activity summary")
            st.dataframe(stage_data, use_container_width=True, hide_index=True)
            st.info(
                "These are aggregate stage counts, not a session-level cohort funnel. "
                "The current funnel_daily table does not show whether the same individual "
                "session completed each successive stage. Therefore, ratios such as purchases "
                "divided by searches should not be called true conversion rates. To calculate "
                "a valid sequential funnel, the pipeline must provide session-level stage "
                "membership or event timestamps for search, view, add-to-cart and purchase."
            )
            if len(funnel_f) == 1:
                st.caption("Only one date is available in the selected period. More daily records are needed for a stage-activity trend.")
    else:
        st.info("No shopping-stage activity records found for the selected date range.")


st.divider()
st.caption("E-Commerce Data Engineering Platform | Built with Streamlit, PostgreSQL, Pandas and Plotly")
