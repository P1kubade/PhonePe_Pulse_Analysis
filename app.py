import streamlit as st
import plotly.express as px
import pandas as pd
import json
import requests
import data_fetcher as df

# --- PAGE CONFIGURATION (PhonePe Theme) ---
st.set_page_config(page_title="PhonePe Pulse Analysis", layout="wide")

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    .main-header {font-size: 2.5rem; color: #5f259f; font-weight: 700; text-align: center; margin-bottom: 20px;}
    .metric-card {background-color: white; padding: 20px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); text-align: center;}
    .metric-label {font-size: 1rem; color: #666;}
    .metric-value {font-size: 1.8rem; font-weight: bold; color: #5f259f;}
    </style>
    """, unsafe_allow_html=True)

# --- LOAD GEOJSON FROM URL ---
@st.cache_data
def load_geojson():
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return json.loads(response.content)
    except Exception as e:
        st.error(f"Error loading Map data: {e}")
        return None

india_states = load_geojson()

# --- HELPER: MAP PLOTTING ---
def plot_india_map(data, color_column, hover_name, title, color_scale='Viridis'):
    if india_states is None:
        st.warning("Map data unavailable.")
        return None
    
    # State Name Standardization
    data['State'] = data['State'].str.replace("Andaman & Nicobar Islands", "Andaman & Nicobar")
    data['State'] = data['State'].str.title()
    
    fig = px.choropleth(
        data,
        geojson=india_states,
        locations='State',
        featureidkey='properties.ST_NM',
        color=color_column,
        hover_name=hover_name,
        color_continuous_scale=color_scale,
        title=title
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin={"r":0,"t":30,"l":0,"b":0}, height=500)
    return fig

# --- SIDEBAR ---
st.sidebar.image("C:/Users/kubad/Desktop/PROJECTS/Guvi_Project-1/image1.png", width=100)
st.sidebar.title("Navigation")
view_mode = st.sidebar.radio("Go to", ["Live Pulse (Home)", "Business Insights"])

st.sidebar.markdown("---")
st.sidebar.header("Filters")
year = st.sidebar.selectbox("Year", [2018, 2019, 2020, 2021, 2022, 2023, 2024], index=5)
quarter = st.sidebar.selectbox("Quarter", [1, 2, 3, 4], index=3)



# VIEW 1: LIVE PULSE (CLONE MODE)

if view_mode == "Live Pulse (Home)":
    st.markdown('<div class="main-header">PhonePe Pulse Analysis</div>', unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["Transactions", "Users"])

    with tab1:
        # 1. KPI Cards
        trend_data = df.trans_trend_national()
        current_data = trend_data[(trend_data['Year'] == year) & (trend_data['Quarter'] == quarter)]
        
        if not current_data.empty:
            total_trans = current_data['Total_Transactions'].values[0]
            total_amount = current_data['Total_Amount'].values[0]
            
            c1, c2, c3 = st.columns(3)
            with c1: st.markdown(f'<div class="metric-card"><div class="metric-label">All India Transactions</div><div class="metric-value">{total_trans:,.0f}</div></div>', unsafe_allow_html=True)
            with c2: st.markdown(f'<div class="metric-card"><div class="metric-label">Total Value (₹)</div><div class="metric-value">₹{total_amount/1e7:,.2f} Cr</div></div>', unsafe_allow_html=True)
            with c3: st.markdown(f'<div class="metric-card"><div class="metric-label">Avg. Transaction Value</div><div class="metric-value">₹{int(total_amount/total_trans)}</div></div>', unsafe_allow_html=True)
        
        st.markdown("---")

        # 2. Map & Top States
        c_map, c_list = st.columns([2, 1])
        with c_map:
            st.subheader(f"Transaction Heatmap ({year} Q{quarter})")
            map_data = df.trans_state_heatmap(year, quarter)
            if not map_data.empty:
                fig = plot_india_map(map_data, 'Total_Amount', 'State', "", 'Magma')
                st.plotly_chart(fig, width="stretch")
        
        with c_list:
            st.subheader("Top States")
            top_states = df.top_trans_states(year, quarter)
            if not top_states.empty:
                # FIX: Using standard dataframe without gradient to avoid matplotlib dependency issues if uninstalled
                st.dataframe(top_states[['State', 'Total_Count']], height=500, hide_index=True)

        # 3. Categories
        st.subheader("Payment Categories")
        cat_data = df.trans_category_split(year, quarter)
        if not cat_data.empty:
            st.plotly_chart(px.pie(cat_data, values='Total_Amount', names='Transaction_Type', hole=0.4), width="stretch")

    with tab2:
        # Users Tab
        user_growth = df.user_national_growth()
        curr_users = user_growth[(user_growth['Year'] == year) & (user_growth['Quarter'] == quarter)]
        
        if not curr_users.empty:
            st.markdown(f'<div class="metric-card"><div class="metric-label">Registered Users</div><div class="metric-value">{curr_users["Total_Users"].values[0]:,.0f}</div></div>', unsafe_allow_html=True)

        st.markdown("---")
        
        c_u_map, c_u_brand = st.columns([2, 1])
        with c_u_map:
            st.subheader("User Base Heatmap")
            # Aggregating user data for map
            user_map = df.user_state_brand_matrix(year).groupby('State')['Users'].sum().reset_index()
            fig_u = plot_india_map(user_map, 'Users', 'State', "", 'Viridis')
            st.plotly_chart(fig_u, width="stretch")

        with c_u_brand:
            st.subheader("Market Share")
            brand_data = df.user_brand_share(year)
            if not brand_data.empty:
                st.plotly_chart(px.pie(brand_data, values='Total_Users', names='Brand', hole=0.4), width="stretch")



# VIEW 2: BUSINESS INSIGHTS (FULL 45 CHARTS)
elif view_mode == "Business Insights":
    st.title("Deep Dive Analysis")
    
    scenario = st.sidebar.selectbox("Select Scenario", [
        "1. Transaction Dynamics", "2. Device & User Engagement", "3. Insurance Penetration",
        "4. Market Expansion", "5. User Engagement Deep Dive", "6. Insurance Engagement",
        "7. Transaction Leaderboards", "8. User Leaderboards", "9. Insurance Leaderboards"
    ])

    # --- SCENARIO 1: TRANSACTION DYNAMICS ---
    if scenario == "1. Transaction Dynamics":
        st.subheader("1. National Transaction Growth")
        st.plotly_chart(px.line(df.trans_trend_national(), x="Year", y="Total_Amount", title="Is the digital economy growing?"), width="stretch")
        
        c1, c2 = st.columns(2)
        with c1: 
            st.subheader("2. Category Split")
            st.plotly_chart(px.pie(df.trans_category_split(year, quarter), values='Total_Amount', names='Transaction_Type', title="Where are people spending?"), width="stretch")
        with c2:
            st.subheader("3. State Heatmap")
            st.plotly_chart(plot_india_map(df.trans_state_heatmap(year, quarter), 'Total_Amount', 'State', "Geographic Distribution"), width="stretch")
            
        st.subheader("4. Top 10 Districts (Volume)")
        st.plotly_chart(px.bar(df.trans_district_top10(year, quarter), x='District', y='Total_Count', color='Total_Count'), width="stretch")
        
        st.subheader("5. Average Transaction Value (ATV)")
        st.plotly_chart(px.area(df.trans_avg_value_trend(), x='Year', y='Avg_Value', title="Are transaction sizes increasing?"), width="stretch")

    # --- SCENARIO 2: DEVICE & USER ENGAGEMENT ---
    elif scenario == "2. Device & User Engagement":
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("1. Brand Market Share")
            st.plotly_chart(px.pie(df.user_brand_share(year), values='Total_Users', names='Brand', hole=0.4), width="stretch")
        with c2:
            st.subheader("2. User Growth Trajectory")
            st.plotly_chart(px.line(df.user_national_growth(), x='Year', y='Total_Users', markers=True), width="stretch")
            
        st.subheader("3. Brand Preference by State")
        st.plotly_chart(px.bar(df.user_state_brand_matrix(year), x="State", y="Users", color="Brand"), width="stretch")
        
        st.subheader("4. Engagement Bubble Chart")
        st.plotly_chart(px.scatter(df.user_app_opens_dist(year, quarter), x="Registered_Users", y="App_Opens", size="Registered_Users", color="District", hover_name="District"), width="stretch")

        st.subheader("5. Top 5 Mobile Brands")
        st.plotly_chart(px.bar(df.user_brand_leaderboard(year), x='Brand', y='Users', text='Users'), width="stretch")

    # --- SCENARIO 3: INSURANCE PENETRATION ---
    elif scenario == "3. Insurance Penetration":
        st.subheader("1. Insurance Market Growth")
        st.plotly_chart(px.line(df.ins_trend_national(), x="Year", y="Policies_Sold", markers=True), width="stretch")
        
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("2. State Penetration Map")
            st.plotly_chart(plot_india_map(df.ins_state_penetration(year), 'Policies_Sold', 'State', "Where is insurance popular?"), width="stretch")
        with c2:
            st.subheader("3. Avg Premium Size")
            st.plotly_chart(px.bar(df.ins_avg_premium(year).head(10), x='State', y='Avg_Premium', color='Avg_Premium'), width="stretch")
            
        st.subheader("4. District Hotspots")
        st.plotly_chart(px.bar(df.ins_district_hotspots(year), x='Policies_Sold', y='District', orientation='h'), width="stretch")

        st.subheader("5. YoY Growth Rate")
        st.plotly_chart(px.bar(df.ins_growth_vs_prev_year(year), x='Year', y='Total_Amount', text='Total_Amount'), width="stretch")

    # --- SCENARIO 4: MARKET EXPANSION ---
    elif scenario == "4. Market Expansion":
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("1. Fastest Growing States")
            st.plotly_chart(px.bar(df.market_growth_states(year).head(10), x='State', y='Total_Amount', title="Volume Leaders"), width="stretch")
        with c2:
            st.subheader("2. Laggard States")
            st.plotly_chart(px.bar(df.market_laggard_states(year), x='State', y='Total_Amount', title="Needs Attention"), width="stretch")
            
        st.subheader("3. YoY Growth Rate Leaders (%)")
        st.plotly_chart(px.bar(df.market_state_growth_yoy(year), x='State', y='Growth_Rate', color='Growth_Rate'), width="stretch")
        
        st.subheader("4. High Volume / Low Value Markets")
        st.plotly_chart(px.scatter(df.market_high_vol_low_val(year), x='Volume', y='Avg_Ticket_Size', text='State', size='Volume'), width="stretch")

        st.subheader("5. District Drilldown (Interactive)")
        target_state = st.selectbox("Select State", ["Maharashtra", "Karnataka", "Tamil Nadu", "Uttar Pradesh", "Bihar"], key="s4_select")
        st.plotly_chart(px.treemap(df.market_district_potential(target_state, year), path=['District'], values='Total_Amount'), width="stretch")

    # --- SCENARIO 5: USER ENGAGEMENT DEEP DIVE ---
    elif scenario == "5. User Engagement Deep Dive":
        st.subheader("1. Engagement Matrix (Opens vs Users)")
        st.plotly_chart(px.scatter(df.engage_opens_vs_reg(year, quarter), x="Reg_Users", y="App_Opens", size="Reg_Users", color="State"), width="stretch")
        
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("2. Total App Opens Ranking")
            st.plotly_chart(px.bar(df.engage_state_ranking(year).head(10), x='State', y='Total_Opens'), width="stretch")
        with c2:
            st.subheader("3. Efficiency (Opens Per User)")
            st.plotly_chart(px.bar(df.engage_state_efficiency(year).head(10), x='State', y='Opens_Per_User', color='Opens_Per_User'), width="stretch")

        st.subheader("4. District Engagement Drilldown")
        target_state_en = st.selectbox("Select State", ["Maharashtra", "Karnataka", "Delhi", "Telangana"], key="s5_select")
        st.plotly_chart(px.bar(df.engage_district_rate(year, target_state_en).head(10), x='District', y='Engagement_Rate'), width="stretch")

        st.subheader(f"5. Quarterly Consistency in {target_state_en}")
        st.plotly_chart(px.line(df.engage_consistency(target_state_en, year), x='Quarter', y='Total_Opens', markers=True), width="stretch")

    # --- SCENARIO 6: INSURANCE ENGAGEMENT ---
    elif scenario == "6. Insurance Engagement":
        st.subheader("1. The Opportunity Gap")
        st.caption("States with High Users but Low Insurance Sales = Opportunity")
        st.plotly_chart(px.scatter(df.ins_opportunity_gap(year), x="Total_Users", y="Policies_Sold", text="State", size="Total_Users"), width="stretch")
        
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("2. Seasonal Trends")
            st.plotly_chart(px.bar(df.ins_seasonal_trend(year), x='Quarter', y='Policies_Sold'), width="stretch")
        with c2:
            st.subheader("3. Fastest Growing States (YoY)")
            st.plotly_chart(px.bar(df.ins_state_growth(year), x='State', y='Growth_Pct', color='Growth_Pct'), width="stretch")

        st.subheader("4. District Penetration Drilldown")
        target_state_ins = st.selectbox("Select State", ["Maharashtra", "Karnataka", "West Bengal", "Gujarat"], key="s6_select")
        st.plotly_chart(px.treemap(df.ins_district_penetration(target_state_ins, year), path=['District'], values='Count'), width="stretch")

        st.subheader("5. Top 10 Insurance Districts (National)")
        st.plotly_chart(px.bar(df.ins_top_district_penetration(year), x='District', y='Total_Policies'), width="stretch")

    # --- SCENARIO 7: TRANSACTION LEADERBOARDS ---
    elif scenario == "7. Transaction Leaderboards":
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("1. Top 10 States")
            st.plotly_chart(px.bar(df.top_trans_states(year, quarter), x='State', y='Total_Count'), width="stretch")
        with c2:
            st.subheader("2. Top 10 Districts")
            st.plotly_chart(px.bar(df.top_trans_districts(year, quarter), x='District', y='Transaction_Count', color='Transaction_Count'), width="stretch")
            
        st.subheader("3. Top 10 Pincodes (Hyper-local)")
        st.plotly_chart(px.bar(df.top_trans_pincodes(year, quarter), x='Pincode', y='Transaction_Count'), width="stretch")

        c3, c4 = st.columns(2)
        with c3:
            st.subheader("4. Bottom 10 Districts")
            st.plotly_chart(px.bar(df.bottom_trans_districts(year, quarter), x='District', y='Transaction_Count'), width="stretch")
        with c4:
            st.subheader("5. State Pincode Coverage")
            st.plotly_chart(px.bar(df.trans_pincode_reach(year).head(10), x='State', y='Pincodes_Covered'), width="stretch")

    # --- SCENARIO 8: USER LEADERBOARDS ---
    elif scenario == "8. User Leaderboards":
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("1. Top 10 States")
            st.plotly_chart(px.bar(df.top_user_states(year, quarter), x='State', y='Total_Users'), width="stretch")
        with c2:
            st.subheader("2. Top 10 Districts")
            st.plotly_chart(px.bar(df.top_user_districts(year, quarter), x='District', y='Registered_Users'), width="stretch")
            
        st.subheader("3. Top 10 Pincodes (Density)")
        st.dataframe(df.top_user_density_pincode(year, quarter), width="stretch")

        c3, c4 = st.columns(2)
        with c3:
            st.subheader("4. Bottom 10 Districts")
            st.plotly_chart(px.bar(df.bottom_user_districts(year, quarter), x='District', y='Registered_Users'), width="stretch")
        with c4:
            st.subheader("5. Pincode Share")
            # Using data from top pincodes to show share
            st.plotly_chart(px.pie(df.top_user_density_pincode(year, quarter), names='Pincode', values='Registered_Users'), width="stretch")

    # --- SCENARIO 9: INSURANCE LEADERBOARDS ---
    elif scenario == "9. Insurance Leaderboards":
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("1. Top 10 States")
            st.plotly_chart(px.bar(df.top_ins_states(year, quarter), x='State', y='Policies_Sold'), width="stretch")
        with c2:
            st.subheader("2. Top 10 Districts")
            st.plotly_chart(px.bar(df.top_ins_districts(year, quarter), x='District', y='Insurance_Count', color='Insurance_Count'), width="stretch")
            
        st.subheader("3. Top 10 Pincodes (High Value)")
        st.plotly_chart(px.bar(df.top_ins_pincode_value(year, quarter), x='Pincode', y='Insurance_Amount'), width="stretch")

        c3, c4 = st.columns(2)
        with c3:
            st.subheader("4. Bottom 10 Districts")
            st.plotly_chart(px.bar(df.bottom_ins_districts(year, quarter), x='District', y='Insurance_Count'), width="stretch")
        with c4:
            st.subheader("5. Top Pincodes (High Count)")
            st.dataframe(df.top_ins_pincodes(year, quarter), width="stretch")


