import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from datetime import time

# Redshift connection string
REDSHIFT_CONN_STRING = st.secrets["redshift"]["url"]
engine = create_engine(REDSHIFT_CONN_STRING)

# Load data from Redshift
@st.cache_data
def load_data():
    with engine.connect() as conn:
        fact_trips = pd.read_sql("SELECT * FROM dbt_sp.fact_trips", con=conn.connection)
        dim_routes = pd.read_sql("SELECT * FROM dbt_sp.dim_routes", con=conn.connection)
        dim_junctions = pd.read_sql("SELECT * FROM dbt_sp.dim_junctions", con=conn.connection)
        dim_time = pd.read_sql("SELECT * FROM dbt_sp.dim_time", con=conn.connection)
    return fact_trips, dim_routes, dim_junctions, dim_time

fact_trips, dim_routes, dim_junctions, dim_time = load_data()

# Preprocessing
fact_trips['time'] = pd.to_datetime(fact_trips['time'], format='%H:%M:%S')
fact_trips['hour'] = fact_trips['time'].dt.hour
fact_trips['date'] = pd.to_datetime(fact_trips['date'])
fact_trips['day_of_week'] = fact_trips['date'].dt.day_name()

# Merge with dim_routes to get route information
fact_trips = fact_trips.merge(dim_routes[['route', 'link', 'direction_name']].drop_duplicates(), 
                             on='route', how='left')

# Dashboard Title
st.title("🚦 Dublin Traffic Travel Time Analytics")
st.markdown("""
This dashboard provides insights into travel times across Dublin's road network, 
analyzing patterns by time of day, route, and trip type.
""")

# Sidebar Filters
st.sidebar.title("Filters")

# Date range filter
min_date = fact_trips['date'].min()
max_date = fact_trips['date'].max()
date_range = st.sidebar.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if len(date_range) == 2:
    start_date, end_date = date_range
    fact_trips = fact_trips[(fact_trips['date'] >= pd.to_datetime(start_date)) & 
                           (fact_trips['date'] <= pd.to_datetime(end_date))]

# Time of day filter
time_range = st.sidebar.slider(
    "Hour range",
    min_value=0,
    max_value=23,
    value=(6, 20)  # Default to daytime hours
)
fact_trips = fact_trips[(fact_trips['hour'] >= time_range[0]) & 
                       (fact_trips['hour'] <= time_range[1])]

# Trip type filter
trip_type_filter = st.sidebar.selectbox(
    "Select Trip Type", 
    options=['All'] + fact_trips['trip_type'].dropna().unique().tolist()
)
if trip_type_filter != 'All':
    fact_trips = fact_trips[fact_trips['trip_type'] == trip_type_filter]

# Route filter
route_filter = st.sidebar.selectbox(
    "Select Route", 
    options=['All'] + sorted(fact_trips['route'].dropna().unique().tolist())
)
if route_filter != 'All':
    fact_trips = fact_trips[fact_trips['route'] == route_filter]

# Direction filter
direction_filter = st.sidebar.selectbox(
    "Select Direction", 
    options=['All'] + fact_trips['direction_name'].dropna().unique().tolist()
)
if direction_filter != 'All':
    fact_trips = fact_trips[fact_trips['direction_name'] == direction_filter]

# Key Metrics
st.subheader("Key Performance Indicators")
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Trips Recorded", f"{len(fact_trips):,}")
with col2:
    st.metric("Avg Travel Time", f"{fact_trips['avg_travel_time'].mean():.1f} sec")
with col3:
    st.metric("Peak Hour", f"{fact_trips.groupby('hour')['trip_count'].sum().idxmax()}:00")
with col4:
    st.metric("Busiest Route", fact_trips['route'].value_counts().idxmax())

# Main Tabs
tab1, tab2, tab3, tab4 = st.tabs(["Time Analysis", "Junction Analysis", "Route Analysis", "Raw Data"])

with tab1:
    # 1. Avg Travel Time by Hour
    st.subheader("Average Travel Time by Hour of Day")
    avg_time_by_hour = fact_trips.groupby('hour')['avg_travel_time'].mean().reset_index()
    fig1 = px.line(avg_time_by_hour, x='hour', y='avg_travel_time', markers=True, 
                  labels={'avg_travel_time': 'Average Travel Time (s)', 'hour': 'Hour of Day'})
    st.plotly_chart(fig1, use_container_width=True)
    
    # 2. Peak Hour Analysis
    st.subheader("Peak Hour Analysis")
    hourly_counts = fact_trips.groupby('hour')['trip_count'].sum().reset_index()
    fig2 = px.area(hourly_counts, x='hour', y='trip_count',
                labels={'trip_count': 'Trips', 'hour': 'Hour of Day'})
    st.plotly_chart(fig2, use_container_width=True)

    # 3. Trip Count by Day of Week
    st.subheader("Trip Count by Day of Week")
    trips_by_day = fact_trips.groupby('day_of_week')['trip_count'].sum().reindex([
        'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'
    ]).reset_index()
    fig3 = px.bar(trips_by_day, x='day_of_week', y='trip_count', 
                 labels={'trip_count': 'Number of Trips', 'day_of_week': 'Day of Week'})
    st.plotly_chart(fig3, use_container_width=True)
    
    # 4. Travel Time Distribution
    st.subheader("Travel Time Distribution")
    fig4 = px.histogram(fact_trips, x='avg_travel_time', nbins=50,
                       labels={'avg_travel_time': 'Travel Time (seconds)'})
    st.plotly_chart(fig4, use_container_width=True)

with tab2:
    # 1. Junction Pair Analysis 
    st.subheader("Common Junction Pairs")
    if 'junction_start' in fact_trips.columns and 'junction_end' in fact_trips.columns:
        junction_pairs = fact_trips.groupby(['junction_start', 'junction_end']).size().reset_index(name='count')
        junction_pairs = junction_pairs.sort_values('count', ascending=False).head(10)
        fig9 = px.bar(junction_pairs, x='count', y='junction_start', color='junction_end',
                     labels={'count': 'Number of Trips', 'junction_start': 'Start Junction'},
                     orientation='h')
        st.plotly_chart(fig9, use_container_width=True)
        
    # 2. Top Start Junctions
    st.subheader("Top 10 Start Junctions by Trip Count")
    if 'junction_start' in fact_trips.columns:
        top_starts = fact_trips['junction_start'].value_counts().head(10).reset_index()
        top_starts.columns = ['junction_start', 'trip_count']
        fig7 = px.bar(top_starts, x='junction_start', y='trip_count',
                     labels={'trip_count': 'Number of Trips', 'junction_start': 'Start Junction'})
        st.plotly_chart(fig7, use_container_width=True)
    
    # 3. Top End Junctions
    st.subheader("Top 10 End Junctions by Trip Count")
    if 'junction_end' in fact_trips.columns:
        top_ends = fact_trips['junction_end'].value_counts().head(10).reset_index()
        top_ends.columns = ['junction_end', 'trip_count']
        fig8 = px.bar(top_ends, x='junction_end', y='trip_count',
                     labels={'trip_count': 'Number of Trips', 'junction_end': 'End Junction'})
        st.plotly_chart(fig8, use_container_width=True)

with tab3:
    # 1. Top Routes by Travel Time
    st.subheader("Top 10 Routes by Average Travel Time")
    top_routes = fact_trips.groupby('route')['avg_travel_time'].mean().sort_values(ascending=False).head(10).reset_index()
    fig4 = px.bar(top_routes, x='route', y='avg_travel_time',
                 labels={'avg_travel_time': 'Average Travel Time (s)', 'route': 'Route ID'})
    st.plotly_chart(fig4, use_container_width=True)
    
    # 2. Route Performance Over Time
    st.subheader("Route Performance Over Time")
    if route_filter == 'All':
        sample_routes = fact_trips['route'].value_counts().head(3).index.tolist()
        route_perf = fact_trips[fact_trips['route'].isin(sample_routes)].groupby(['date', 'route'])['avg_travel_time'].mean().reset_index()
        fig5 = px.line(route_perf, x='date', y='avg_travel_time', color='route',
                      labels={'avg_travel_time': 'Average Travel Time (s)', 'date': 'Date'})
        st.plotly_chart(fig5, use_container_width=True)
    else:
        route_perf = fact_trips.groupby(['date', 'route'])['avg_travel_time'].mean().reset_index()
        fig5 = px.line(route_perf, x='date', y='avg_travel_time',
                      labels={'avg_travel_time': 'Average Travel Time (s)', 'date': 'Date'})
        st.plotly_chart(fig5, use_container_width=True)
    
    # 3. Direction Comparison
    st.subheader("Travel Time by Direction")
    if 'direction_name' in fact_trips.columns:
        dir_comparison = fact_trips.groupby('direction_name')['avg_travel_time'].mean().reset_index()
        fig6 = px.bar(dir_comparison, x='direction_name', y='avg_travel_time',
                      labels={'avg_travel_time': 'Average Travel Time (s)', 'direction_name': 'Direction'})
        st.plotly_chart(fig6, use_container_width=True)


with tab4:
    # Raw Data Preview
    st.subheader("Sample Data")
    st.dataframe(fact_trips.head(100))
    
    # Download Option
    st.download_button(
        label="Download Filtered Data as CSV",
        data=fact_trips.to_csv(index=False).encode('utf-8'),
        file_name='filtered_travel_times.csv',
        mime='text/csv'
    )

# Footer
st.markdown("---")
st.markdown("""
**Data Source**: Smart Dublin - Journey times across Dublin City, from Traffic Department's TRIPS system DCC  
**Last Updated**: {}  
**Note**: Travel time units assumed to be seconds
""".format(pd.to_datetime('today').strftime('%Y-%m-%d')))