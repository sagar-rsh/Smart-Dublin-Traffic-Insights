import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from datetime import datetime
import sys
import logging
import gc

# Configuration
st.set_page_config(
    page_title="Dublin Traffic Analytics",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database Connection


@st.cache_resource
def get_db_engine():
    try:
        engine = create_engine(
            st.secrets["redshift"]["url"], pool_size=1, max_overflow=0
        )
        logger.info("Created new engine with connection pool")
        return engine
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        st.error("Failed to connect to database. Please check your credentials.")
        st.stop()

# Data Loading with Memory Management


@st.cache_data(show_spinner="Loading traffic data...", max_entries=1)
def load_data():
    engine = get_db_engine()
    chunks = []

    try:
        # Load fact_trips in chunks
        with st.spinner("Loading trip data..."):
            with engine.connect() as conn:
                for chunk in pd.read_sql(
                    """SELECT route, "date", "time", avg_travel_time, trip_count, trip_type, junction_start, junction_end FROM prod.fact_trips LIMIT 300000""",
                    con=conn.connection,
                    chunksize=100000
                ):
                    chunks.append(chunk)
                    if sys.getsizeof(chunks) > 700 * 1024 * 1024:  # ~700MB limit
                        st.warning("Reached memory safety limit during load")
                        break

        fact_trips = pd.concat(chunks) if chunks else pd.DataFrame()

        # Load dimension tables
        dim_tables = {}
        dim_queries = {
            'dim_routes': "SELECT route, link, direction_name FROM prod.dim_routes",
            'dim_junctions': "SELECT junction_id, junction_name FROM prod.dim_junctions"
        }
        with engine.connect() as conn:
            for table, query in dim_queries.items():
                with st.spinner(f"Loading {table}..."):
                    dim_tables[table] = pd.read_sql(query, con=conn.connection)

        return fact_trips, dim_tables["dim_routes"], dim_tables["dim_junctions"]

    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        st.error(f"Data loading error: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    # finally:
    #     engine.dispose()
    #     logger.info("Database connections cleaned up")

# Data Processing


def process_data(fact_trips, dim_routes):
    try:
        # Convert datatypes
        fact_trips['time'] = pd.to_datetime(
            fact_trips['time'], format='%H:%M:%S', errors='coerce')
        fact_trips['date'] = pd.to_datetime(
            fact_trips['date'], errors='coerce')

        # Extract temporal features
        fact_trips['hour'] = fact_trips['time'].dt.hour
        fact_trips['day_of_week'] = fact_trips['date'].dt.day_name()

        # Merge with routes
        if not dim_routes.empty:
            fact_trips = fact_trips.merge(
                dim_routes[['route', 'link', 'direction_name']
                           ].drop_duplicates(),
                on='route',
                how='left'
            )

        # Memory optimization
        for col in fact_trips.select_dtypes(include=['int64']):
            fact_trips[col] = pd.to_numeric(
                fact_trips[col], downcast='integer')
        for col in fact_trips.select_dtypes(include=['object']):
            fact_trips[col] = fact_trips[col].astype('category')

        return fact_trips.dropna(subset=['date', 'time'])

    except Exception as e:
        logger.error(f"Data processing failed: {e}")
        st.error("Failed to process data")
        return pd.DataFrame()
    finally:
        gc.collect()  # Force garbage collection
        # mem = psutil.Process().memory_info().rss / 1024 ** 2
        # logger.info(f"Pre-visualization memory: {mem:.1f} MB")

# Visualization


def safe_plotly_chart(fig):
    """Render Plotly charts with error handling"""
    try:
        if len(fig.data[0].x) > 500000:
            st.image(fig.to_image(format="png", width=1200),
                     use_column_width=True)
        else:
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Chart rendering failed: {str(e)}")

# Main Application


def main():
    # Load data
    fact_trips, dim_routes, dim_junctions = load_data()

    # Check data
    if fact_trips.empty:
        st.error("No trip data available. Please try again later.")
        return

    # Process data
    with st.spinner("Processing data..."):
        df = process_data(fact_trips, dim_routes)

    st.title("🚦 Dublin Traffic Travel Time Analytics")

    # Sidebar Filters
    st.sidebar.title("Filters")

    # Date range filter
    min_date, max_date = df['date'].min(), df['date'].max()
    date_range = st.sidebar.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    # Time filter
    time_range = st.sidebar.slider(
        "Select time range (hours)",
        min_value=0,
        max_value=23,
        value=(6, 20),  # Default range (6am to 8pm)
        step=1,
        format="%d:00"
    )

    # Other filters
    trip_type = st.sidebar.selectbox(
        "Trip Type",
        ['All'] + sorted(df['trip_type'].dropna().unique().tolist())
    )

    route = st.sidebar.selectbox(
        "Route",
        ['All'] + sorted(df['route'].dropna().unique().tolist())
    )

    # Apply filters
    filtered_df = df[
        (df['date'].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1]))) &
        (df['hour'].between(time_range[0], time_range[1]))
    ]

    if trip_type != 'All':
        filtered_df = filtered_df[filtered_df['trip_type'] == trip_type]
    if route != 'All':
        filtered_df = filtered_df[filtered_df['route'] == route]

    # Display Metrics
    st.subheader("Performance Overview")
    cols = st.columns(4)
    cols[0].metric("Total Trips", f"{len(filtered_df):,}")
    cols[1].metric("Avg Travel Time",
                   f"{filtered_df['avg_travel_time'].mean():.1f} sec")
    cols[2].metric(
        "Peak Hour", f"{filtered_df.groupby('hour')['trip_count'].sum().idxmax()}:00")
    cols[3].metric("Busiest Route",
                   f"Route {filtered_df['route'].value_counts().idxmax()}")

    # Main Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Time Patterns",
        "🚏 Junctions",
        "🛣️ Route Analysis",
        "📊 Data"
    ])

    with tab1:
        if not filtered_df.empty:
            # 1. Avg Travel Time by Hour
            st.subheader("Average Travel Time by Hour of Day")
            avg_time_by_hour = filtered_df.groupby(
                'hour')['avg_travel_time'].mean().reset_index()
            fig1 = px.line(
                avg_time_by_hour,
                x='hour',
                y='avg_travel_time',
                markers=True,
                title="Hourly Travel Time Patterns",
                labels={
                    'avg_travel_time': 'Avg Travel Time (s)', 'hour': 'Hour of Day'}
            )
            safe_plotly_chart(fig1)

            # 2. Peak Hour Analysis
            st.subheader("Peak Hour Analysis")
            hourly_counts = filtered_df.groupby(
                'hour')['trip_count'].sum().reset_index()
            fig2 = px.area(
                hourly_counts,
                x='hour',
                y='trip_count',
                title="Trip Volume by Hour",
                labels={'trip_count': 'Number of Trips', 'hour': 'Hour of Day'}
            )
            safe_plotly_chart(fig2)

            # 3. Trip Count by Day of Week
            st.subheader("Trip Count by Day of Week")
            trips_by_day = filtered_df.groupby('day_of_week')['trip_count'].sum().reindex([
                'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'
            ]).reset_index()
            fig3 = px.bar(
                trips_by_day,
                x='day_of_week',
                y='trip_count',
                title="Weekly Trip Patterns",
                labels={'trip_count': 'Number of Trips',
                        'day_of_week': 'Day of Week'}
            )
            safe_plotly_chart(fig3)

            # 4. Travel Time Distribution
            st.subheader("Travel Time Distribution")
            if len(filtered_df) > 100000:  # Restricting data points to reduce load
                sample_df = filtered_df.sample(100000)
                fig4 = px.histogram(sample_df, x='avg_travel_time', nbins=50,
                                    labels={'avg_travel_time': 'Travel Time (seconds)'})
            else:
                fig4 = px.histogram(filtered_df, x='avg_travel_time', nbins=50,
                                    labels={'avg_travel_time': 'Travel Time (seconds)'})
            safe_plotly_chart(fig4)

    with tab2:
        # 1. Junction Pair Analysis
        st.subheader("Common Junction Pairs")
        if 'junction_start' in filtered_df.columns and 'junction_end' in filtered_df.columns:
            junction_pairs = filtered_df.groupby(
                ['junction_start', 'junction_end']).size().reset_index(name='count')
            junction_pairs = junction_pairs.sort_values(
                'count', ascending=False).head(10)
            fig9 = px.bar(junction_pairs, x='count', y='junction_start', color='junction_end',
                          labels={'count': 'Number of Trips',
                                  'junction_start': 'Start Junction'},
                          orientation='h')
            safe_plotly_chart(fig9)

        # 2. Top Start Junctions
        st.subheader("Top 10 Start Junctions by Trip Count")
        if 'junction_start' in filtered_df.columns:
            top_starts = filtered_df['junction_start'].value_counts().head(
                10).reset_index()
            top_starts.columns = ['junction_start', 'trip_count']
            fig7 = px.bar(top_starts, x='junction_start', y='trip_count',
                          labels={'trip_count': 'Number of Trips', 'junction_start': 'Start Junction'})
            safe_plotly_chart(fig7)

        # 3. Top End Junctions
        st.subheader("Top 10 End Junctions by Trip Count")
        if 'junction_end' in filtered_df.columns:
            top_ends = filtered_df['junction_end'].value_counts().head(
                10).reset_index()
            top_ends.columns = ['junction_end', 'trip_count']
            fig8 = px.bar(top_ends, x='junction_end', y='trip_count',
                          labels={'trip_count': 'Number of Trips', 'junction_end': 'End Junction'})
            safe_plotly_chart(fig8)

    with tab3:
        # 1. Top Routes by Travel Time
        st.subheader("Top 10 Routes by Average Travel Time")
        route_stats = filtered_df.groupby('route').agg({
            'avg_travel_time': 'mean',
            'trip_count': 'sum'
        }).reset_index()

        if not route_stats.empty:
            top_routes = route_stats.sort_values(
                'avg_travel_time', ascending=False).head(10)
            fig4 = px.bar(
                top_routes,
                x='route',
                y='avg_travel_time',
                title="Slowest Routes (by average travel time)",
                labels={
                    'avg_travel_time': 'Average Travel Time (seconds)',
                    'route': 'Route ID',
                    'trip_count': 'Trip Count'
                },
                hover_data=['trip_count'],
                color='avg_travel_time',
                color_continuous_scale='Reds'
            )
            # Update layout for better readability
            fig4.update_layout(
                xaxis={'categoryorder': 'total descending'},
                yaxis_title="Average Travel Time (seconds)",
                xaxis_title="Route ID",
                coloraxis_showscale=False
            )
            safe_plotly_chart(fig4)
        else:
            st.warning("No route data available with current filters")

        # 2. Route Performance Over Time
        st.subheader("Route Performance Over Time")
        if not filtered_df.empty:
            if route == 'All':
                # Show top 3 routes if no specific route selected
                top_routes = filtered_df['route'].value_counts().head(
                    3).index.tolist()
                route_data = filtered_df[filtered_df['route'].isin(top_routes)]
                fig5 = px.line(
                    route_data.groupby(['date', 'route'])[
                        'avg_travel_time'].mean().reset_index(),
                    x='date',
                    y='avg_travel_time',
                    color='route',
                    title="Daily Performance for Top 3 Routes",
                    labels={
                        'avg_travel_time': 'Average Travel Time (seconds)',
                        'date': 'Date'
                    }
                )
            else:
                # Show selected route's performance
                route_data = filtered_df[filtered_df['route'] == route]
                fig5 = px.line(
                    route_data.groupby('date')[
                        'avg_travel_time'].mean().reset_index(),
                    x='date',
                    y='avg_travel_time',
                    title=f"Daily Performance for Route {route}",
                    labels={
                        'avg_travel_time': 'Average Travel Time (seconds)',
                        'date': 'Date'
                    }
                )
            safe_plotly_chart(fig5)
        else:
            st.warning(
                "No route performance data available with current filters")

    with tab4:
        st.subheader("Filtered Data Preview")
        st.dataframe(filtered_df.head(100))

        st.download_button(
            "Download Filtered Data",
            data=filtered_df.to_csv(index=False).encode('utf-8'),
            file_name=f"dublin_traffic_{datetime.now().date()}.csv",
            mime='text/csv'
        )

    # Footer
    st.markdown("---")
    st.markdown(f"""
    **Data Source**: Smart Dublin - Journey times across Dublin City, from Traffic Department's TRIPS system DCC • 
    **Last Refresh**: {datetime.now().strftime('%Y-%m-%d %H:%M')} • 

    **Note**: Travel time units assumed to be seconds
    """)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Application error")
        st.error("A critical error occurred")
        st.code(f"Error details: {str(e)}")
