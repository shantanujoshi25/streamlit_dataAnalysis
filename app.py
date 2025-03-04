import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from streamlit_folium import folium_static
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px
import plotly.graph_objects as go

# Page config
st.set_page_config(
    page_title="San Jose Crash Data Analysis",
    page_icon="🚗",
    layout="wide"
)

# Title and description
st.title("San Jose Crash Data Analysis")
st.markdown("Traffic accident analysis for the City of San Jose")

# Function to authenticate and create BigQuery client
def get_bigquery_client():
    try:
        # Create BigQuery client - use default authentication
        client = bigquery.Client(project='datamining-assign')
        return client
    except Exception as e:
        st.error(f"Authentication error: {e}")
        return None

# Function to load data from BigQuery
@st.cache_data(ttl=3600)  # Cache data for 1 hour
def load_bigquery_data(query):
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()
    
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame()

# Add interactive filters
st.sidebar.header("Filters")

# Load years for filtering
years_query = """
SELECT DISTINCT EXTRACT(YEAR FROM DATE) as year
FROM `datamining-assign.crash_data.processed_crash_data`
ORDER BY year
"""

years_data = load_bigquery_data(years_query)
if not years_data.empty:
    years = years_data['year'].astype(int).tolist()
    selected_years = st.sidebar.multiselect(
        "Select Years",
        options=years,
        default=years
    )
else:
    selected_years = []
    st.sidebar.warning("Could not load year data")

# Load severity categories for filtering
severity_query = """
SELECT DISTINCT SEVERITY_CATEGORY 
FROM `datamining-assign.crash_data.processed_crash_data`
ORDER BY SEVERITY_CATEGORY
"""

severity_data = load_bigquery_data(severity_query)
if not severity_data.empty:
    severities = severity_data['SEVERITY_CATEGORY'].tolist()
    selected_severities = st.sidebar.multiselect(
        "Select Severity",
        options=severities,
        default=severities
    )
else:
    selected_severities = []
    st.sidebar.warning("Could not load severity data")

# Build WHERE clause for filters
where_clauses = ["INTASTREETNAME IS NOT NULL AND INTBSTREETNAME IS NOT NULL"]

if selected_years:
    years_str = ", ".join([str(year) for year in selected_years])
    where_clauses.append(f"EXTRACT(YEAR FROM DATE) IN ({years_str})")

if selected_severities:
    severities_str = ", ".join([f"'{s}'" for s in selected_severities])
    where_clauses.append(f"SEVERITY_CATEGORY IN ({severities_str})")

where_clause = " AND ".join(where_clauses)

# 1. Top 10 Crash Sites Section
st.header("Top 10 Crash Sites")

# Load intersection crash data with filters
intersection_query = f"""
SELECT 
    INTASTREETNAME, 
    INTBSTREETNAME, 
    COUNT(*) as crash_count,
    AVG(LATITUDE) as latitude,
    AVG(LONGITUDE) as longitude
FROM `datamining-assign.crash_data.processed_crash_data`
WHERE {where_clause}
GROUP BY INTASTREETNAME, INTBSTREETNAME
ORDER BY crash_count DESC
LIMIT 10
"""

intersection_data = load_bigquery_data(intersection_query)

if not intersection_data.empty:
    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["Map", "Chart", "Table"])
    
    with tab1:
        # Create map
        st.subheader("Top 10 Crash Intersections Map")
        
        # Check if we have lat/long data
        if 'latitude' in intersection_data.columns and 'longitude' in intersection_data.columns:
            # Create a map centered on San Jose
            san_jose_coords = [37.3382, -121.8863]
            m = folium.Map(location=san_jose_coords, zoom_start=12)
            
            # Add markers for each intersection
            for idx, row in intersection_data.iterrows():
                if pd.notna(row['latitude']) and pd.notna(row['longitude']):
                    popup_text = f"<b>{row['INTASTREETNAME']} & {row['INTBSTREETNAME']}</b><br>Crashes: {row['crash_count']}"
                    
                    folium.CircleMarker(
                        location=[row['latitude'], row['longitude']],
                        radius=min(15, row['crash_count']/10),  # Scale circle size based on crash count
                        color='red',
                        fill=True,
                        fill_color='red',
                        fill_opacity=0.7,
                        popup=folium.Popup(popup_text, max_width=300)
                    ).add_to(m)
            
            # Display the map
            folium_static(m)
        else:
            st.warning("Map cannot be displayed due to missing location data")
    
    with tab2:
        # Create and display bar chart
        st.subheader("Top 10 Crash Intersections")
        fig = px.bar(
            intersection_data,
            x='crash_count',
            y=[f"{row['INTASTREETNAME']} & {row['INTBSTREETNAME']}" for _, row in intersection_data.iterrows()],
            orientation='h',
            title='Top 10 Crash Intersections',
            labels={'x': 'Number of Crashes', 'y': 'Intersection'},
            color='crash_count',
            color_continuous_scale=px.colors.sequential.Reds
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        # Display as a table
        st.subheader("Top 10 Crash Intersections")
        st.dataframe(intersection_data[['INTASTREETNAME', 'INTBSTREETNAME', 'crash_count']])
else:
    st.error("Could not load intersection data.")

# 2. Crashes by Hour with interactive options
st.header("Crashes by Hour of Day")

# Create chart type selector
chart_type = st.radio(
    "Select chart type:",
    ["Line Chart", "Bar Chart", "Area Chart"],
    horizontal=True
)

# Apply filters to query
hourly_query = f"""
SELECT HOUR, COUNT(*) as crash_count
FROM `datamining-assign.crash_data.processed_crash_data`
WHERE {where_clause}
GROUP BY HOUR
ORDER BY HOUR
"""

hourly_data = load_bigquery_data(hourly_query)

if not hourly_data.empty:
    # Check for missing hours and fill with zeros
    all_hours = pd.DataFrame({'HOUR': range(0, 24)})
    hourly_data = pd.merge(all_hours, hourly_data, on='HOUR', how='left').fillna(0)
    
    # Create different chart types based on selection
    if chart_type == "Bar Chart":
        fig = px.bar(
            hourly_data, 
            x='HOUR', 
            y='crash_count',
            title='Crashes by Hour of Day',
            labels={'crash_count': 'Number of Crashes', 'HOUR': 'Hour (24-hour format)'},
            color='crash_count',
            color_continuous_scale=px.colors.sequential.Viridis
        )
    elif chart_type == "Area Chart":
        fig = px.area(
            hourly_data, 
            x='HOUR', 
            y='crash_count',
            title='Crashes by Hour of Day',
            labels={'crash_count': 'Number of Crashes', 'HOUR': 'Hour (24-hour format)'}
        )
    else:  # Line Chart
        fig = px.line(
            hourly_data, 
            x='HOUR', 
            y='crash_count',
            markers=True,
            title='Crashes by Hour of Day',
            labels={'crash_count': 'Number of Crashes', 'HOUR': 'Hour (24-hour format)'}
        )
    
    # Highlight rush hours
    fig.add_vrect(x0=7, x1=9, fillcolor="yellow", opacity=0.2, line_width=0, annotation_text="Morning Rush")
    fig.add_vrect(x0=16, x1=19, fillcolor="orange", opacity=0.2, line_width=0, annotation_text="Evening Rush")
    
    # Add peak hour annotation
    peak_hour = hourly_data.loc[hourly_data['crash_count'].idxmax()]
    fig.add_annotation(
        x=peak_hour['HOUR'],
        y=peak_hour['crash_count'],
        text=f"Peak: {int(peak_hour['crash_count'])} crashes",
        showarrow=True,
        arrowhead=1
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Add option to download the data
    csv = hourly_data.to_csv(index=False)
    st.download_button(
        label="Download hourly data as CSV",
        data=csv,
        file_name="hourly_crash_data.csv",
        mime="text/csv"
    )
else:
    st.error("Could not load hourly crash data.")

# 3. Day-Hour Heatmap with fixed color scale
st.header("Day-Hour Crash Heatmap")

# Apply filters to query
day_hour_query = f"""
SELECT DAYOFWEEKNAME, HOUR, COUNT(*) as crash_count
FROM `datamining-assign.crash_data.processed_crash_data`
WHERE {where_clause}
GROUP BY DAYOFWEEKNAME, HOUR
ORDER BY 
    CASE 
        WHEN DAYOFWEEKNAME = 'Monday' THEN 1
        WHEN DAYOFWEEKNAME = 'Tuesday' THEN 2
        WHEN DAYOFWEEKNAME = 'Wednesday' THEN 3
        WHEN DAYOFWEEKNAME = 'Thursday' THEN 4
        WHEN DAYOFWEEKNAME = 'Friday' THEN 5
        WHEN DAYOFWEEKNAME = 'Saturday' THEN 6
        WHEN DAYOFWEEKNAME = 'Sunday' THEN 7
    END,
    HOUR
"""

day_hour_data = load_bigquery_data(day_hour_query)

if not day_hour_data.empty:
    # Create pivot table
    pivot_data = day_hour_data.pivot(index='DAYOFWEEKNAME', columns='HOUR', values='crash_count')
    
    # Reorder days of week
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    pivot_data = pivot_data.reindex(day_order)
    
    # Fill NaN values with 0
    pivot_data = pivot_data.fillna(0)
    
    # Create heatmap with fixed color scale
    fig = px.imshow(
        pivot_data,
        labels=dict(x="Hour of Day", y="Day of Week", color="Crash Count"),
        title="Crash Frequency by Day and Hour",
        color_continuous_scale="YlOrRd",
        text_auto='.0f'
    )
    
    # Update layout for better readability
    fig.update_layout(
        xaxis=dict(
            tickmode='linear',
            tick0=0,
            dtick=1
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Find peak day-hour combination
    max_idx = pivot_data.stack().idxmax()
    max_text = f"Peak: {max_idx[0]} at {max_idx[1]}:00 ({int(pivot_data.loc[max_idx[0], max_idx[1]])} crashes)"
    
    st.info(max_text)
else:
    st.error("Could not load day-hour heatmap data.")

# 4. Monthly Trends without year breakdown
st.header("Monthly Crash Trends")

# Apply filters to query
monthly_query = f"""
SELECT MONTHNAME, COUNT(*) as crash_count
FROM `datamining-assign.crash_data.processed_crash_data`
WHERE {where_clause}
GROUP BY MONTHNAME
ORDER BY CASE 
    WHEN MONTHNAME = 'January' THEN 1
    WHEN MONTHNAME = 'February' THEN 2
    WHEN MONTHNAME = 'March' THEN 3
    WHEN MONTHNAME = 'April' THEN 4
    WHEN MONTHNAME = 'May' THEN 5
    WHEN MONTHNAME = 'June' THEN 6
    WHEN MONTHNAME = 'July' THEN 7
    WHEN MONTHNAME = 'August' THEN 8
    WHEN MONTHNAME = 'September' THEN 9
    WHEN MONTHNAME = 'October' THEN 10
    WHEN MONTHNAME = 'November' THEN 11
    WHEN MONTHNAME = 'December' THEN 12
END
"""

monthly_data = load_bigquery_data(monthly_query)

if not monthly_data.empty:
    # Create line chart for overall monthly trend
    fig = px.line(
        monthly_data, 
        x='MONTHNAME', 
        y='crash_count',
        markers=True,
        title='Crashes by Month',
        labels={'crash_count': 'Number of Crashes', 'MONTHNAME': 'Month'}
    )
    
    # Reorder month names
    month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                   'July', 'August', 'September', 'October', 'November', 'December']
    fig.update_xaxes(categoryorder='array', categoryarray=month_order)
    
    # Find and highlight max month
    max_month = monthly_data.loc[monthly_data['crash_count'].idxmax()]
    fig.add_annotation(
        x=max_month['MONTHNAME'],
        y=max_month['crash_count'],
        text=f"Peak: {max_month['crash_count']} crashes",
        showarrow=True,
        arrowhead=1
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Add seasonal analysis
    st.subheader("Seasonal Analysis")
    
    # Define seasons
    seasons = {
        'Winter': ['December', 'January', 'February'],
        'Spring': ['March', 'April', 'May'],
        'Summer': ['June', 'July', 'August'],
        'Fall': ['September', 'October', 'November']
    }
    
    # Create seasonal data
    seasonal_data = pd.DataFrame({
        'Season': [],
        'crash_count': []
    })
    
    for season, months in seasons.items():
        season_crashes = monthly_data[monthly_data['MONTHNAME'].isin(months)]['crash_count'].sum()
        seasonal_data = pd.concat([seasonal_data, pd.DataFrame({'Season': [season], 'crash_count': [season_crashes]})])
    
    # Order seasons
    season_order = ['Winter', 'Spring', 'Summer', 'Fall']
    seasonal_data['Season'] = pd.Categorical(seasonal_data['Season'], categories=season_order, ordered=True)
    seasonal_data = seasonal_data.sort_values('Season')
    
    # Create seasonal chart
    fig = px.pie(
        seasonal_data,
        names='Season',
        values='crash_count',
        title='Crash Distribution by Season',
        color='Season',
        color_discrete_map={
            'Winter': '#A1C3D1',
            'Spring': '#B39EB5',
            'Summer': '#FFB6B9',
            'Fall': '#F9C784'
        }
    )
    
    st.plotly_chart(fig, use_container_width=True)
else:
    st.error("Could not load monthly crash data.")

# Footer
st.markdown("---")
st.markdown("San Jose Crash Data Analysis - CMPE 255 Assignment")