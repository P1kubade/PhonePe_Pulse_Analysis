import pandas as pd
from sqlalchemy import create_engine

# --- CONFIGURATION ---
db_host = "localhost"
db_user = "root"
db_password = "#mySql123"  # UPDATE THIS WITH YOUR PASSWORD
db_name = "phonepe_pulse"

# Connect to Database
try:
    engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")
except Exception as e:
    print(f"Error connecting to database: {e}")

# ==========================================
# SCENARIO 1: Transaction Dynamics
# ==========================================
def trans_trend_national():
    """1.1 National Transaction Trends (Volume & Value)"""
    query = """
    SELECT Year, Quarter, SUM(Transaction_Count) as Total_Transactions, SUM(Transaction_Amount) as Total_Amount
    FROM aggregated_transaction
    GROUP BY Year, Quarter
    ORDER BY Year, Quarter;
    """
    return pd.read_sql(query, engine)

def trans_category_split(year, quarter):
    """1.2 Transaction Categories (Recharge vs P2P, etc.)"""
    query = f"""
    SELECT Transaction_Type, SUM(Transaction_Amount) as Total_Amount
    FROM aggregated_transaction
    WHERE Year = {year} AND Quarter = {quarter}
    GROUP BY Transaction_Type;
    """
    return pd.read_sql(query, engine)

def trans_state_heatmap(year, quarter):
    """1.3 State-wise Total Transactions for Geo Map"""
    query = f"""
    SELECT State, SUM(Transaction_Amount) as Total_Amount, SUM(Transaction_Count) as Total_Count
    FROM aggregated_transaction
    WHERE Year = {year} AND Quarter = {quarter}
    GROUP BY State;
    """
    return pd.read_sql(query, engine)

def trans_district_top10(year, quarter):
    """1.4 Top 10 Districts by Volume"""
    query = f"""
    SELECT District, SUM(Transaction_Count) as Total_Count
    FROM map_transaction
    WHERE Year = {year} AND Quarter = {quarter}
    GROUP BY District
    ORDER BY Total_Count DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

def trans_avg_value_trend():
    """1.5 Avg Transaction Value (ATV) Trend"""
    query = """
    SELECT Year, SUM(Transaction_Amount) / SUM(Transaction_Count) as Avg_Value
    FROM aggregated_transaction
    GROUP BY Year
    ORDER BY Year;
    """
    return pd.read_sql(query, engine)

# ==========================================
# SCENARIO 2: Device Dominance
# ==========================================
def user_brand_share(year):
    """2.1 Brand Market Share (Pie Chart)"""
    query = f"""
    SELECT Brand, SUM(User_Count) as Total_Users
    FROM aggregated_user
    WHERE Year = {year}
    GROUP BY Brand
    ORDER BY Total_Users DESC;
    """
    return pd.read_sql(query, engine)

def user_state_brand_matrix(year):
    """2.2 State vs Brand preference"""
    query = f"""
    SELECT State, Brand, SUM(User_Count) as Users
    FROM aggregated_user
    WHERE Year = {year}
    GROUP BY State, Brand;
    """
    return pd.read_sql(query, engine)

def user_national_growth():
    """2.3 User Growth Trend"""
    query = """
    SELECT Year, Quarter, SUM(Registered_Users) as Total_Users
    FROM map_user
    GROUP BY Year, Quarter
    ORDER BY Year, Quarter;
    """
    return pd.read_sql(query, engine)

def user_app_opens_dist(year, quarter):
    """2.4 App Opens Distribution by District (Bubble Chart Data)"""
    query = f"""
    SELECT District, Registered_Users, App_Opens
    FROM map_user
    WHERE Year = {year} AND Quarter = {quarter};
    """
    return pd.read_sql(query, engine)

def user_brand_leaderboard(year):
    """2.5 Top 5 Brands Growth (Bar Chart)"""
    query = f"""
    SELECT Brand, SUM(User_Count) as Users
    FROM aggregated_user
    WHERE Year = {year}
    GROUP BY Brand
    ORDER BY Users DESC LIMIT 5;
    """
    return pd.read_sql(query, engine)

# ==========================================
# SCENARIO 3: Insurance Penetration
# ==========================================
def ins_trend_national():
    """3.1 Insurance Market Growth"""
    query = """
    SELECT Year, Quarter, SUM(Transaction_Amount) as Total_Premium, SUM(Transaction_Count) as Policies_Sold
    FROM aggregated_insurance
    GROUP BY Year, Quarter
    ORDER BY Year, Quarter;
    """
    return pd.read_sql(query, engine)

def ins_state_penetration(year):
    """3.2 State-wise Insurance Adoption"""
    query = f"""
    SELECT State, SUM(Transaction_Count) as Policies_Sold
    FROM aggregated_insurance
    WHERE Year = {year}
    GROUP BY State
    ORDER BY Policies_Sold DESC;
    """
    return pd.read_sql(query, engine)

def ins_avg_premium(year):
    """3.3 Average Premium Size by State"""
    query = f"""
    SELECT State, SUM(Transaction_Amount) / SUM(Transaction_Count) as Avg_Premium
    FROM aggregated_insurance
    WHERE Year = {year}
    GROUP BY State;
    """
    return pd.read_sql(query, engine)

def ins_district_hotspots(year):
    """3.4 District Hotspots for Insurance"""
    query = f"""
    SELECT District, SUM(Insurance_Count) as Policies_Sold
    FROM map_insurance
    WHERE Year = {year}
    GROUP BY District
    ORDER BY Policies_Sold DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

def ins_growth_vs_prev_year(year):
    """3.5 Growth Comparison (Current Year vs Previous)"""
    # Note: Requires simple aggregation, calculated in Python for simplicity
    query = f"""
    SELECT Year, SUM(Transaction_Amount) as Total_Amount
    FROM aggregated_insurance
    WHERE Year IN ({year}, {year-1})
    GROUP BY Year;
    """
    return pd.read_sql(query, engine)

# ==========================================
# SCENARIO 4: Market Expansion (Growth & Laggards)
# ==========================================
def market_growth_states(year):
    """4.1 Fastest Growing States (Volume)"""
    query = f"""
    SELECT State, SUM(Transaction_Amount) as Total_Amount
    FROM aggregated_transaction
    WHERE Year = {year}
    GROUP BY State
    ORDER BY Total_Amount DESC;
    """
    return pd.read_sql(query, engine)

def market_laggard_states(year):
    """4.2 Lowest Performing States"""
    query = f"""
    SELECT State, SUM(Transaction_Amount) as Total_Amount
    FROM aggregated_transaction
    WHERE Year = {year}
    GROUP BY State
    ORDER BY Total_Amount ASC LIMIT 10;
    """
    return pd.read_sql(query, engine)

def market_district_potential(state, year):
    """4.3 District Drill-down for a specific State"""
    query = f"""
    SELECT District, SUM(Transaction_Amount) as Total_Amount
    FROM map_transaction
    WHERE State = '{state}' AND Year = {year}
    GROUP BY District;
    """
    return pd.read_sql(query, engine)

# 4.4 Growth Rate (YoY): Which states are growing fastest compared to last year?
def market_state_growth_yoy(year):
    query = f"""
    WITH CurrentYear AS (
        SELECT State, SUM(Transaction_Amount) as Curr_Total 
        FROM aggregated_transaction WHERE Year = {year} GROUP BY State
    ),
    LastYear AS (
        SELECT State, SUM(Transaction_Amount) as Prev_Total 
        FROM aggregated_transaction WHERE Year = {year-1} GROUP BY State
    )
    SELECT c.State, c.Curr_Total, l.Prev_Total, 
           ((c.Curr_Total - l.Prev_Total) / l.Prev_Total) * 100 as Growth_Rate
    FROM CurrentYear c
    JOIN LastYear l ON c.State = l.State
    ORDER BY Growth_Rate DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

# 4.5 High Volume / Low Value Markets (Low Margin Opportunities)
def market_high_vol_low_val(year):
    query = f"""
    SELECT State, SUM(Transaction_Count) as Volume, 
           (SUM(Transaction_Amount) / SUM(Transaction_Count)) as Avg_Ticket_Size
    FROM aggregated_transaction
    WHERE Year = {year}
    GROUP BY State
    ORDER BY Avg_Ticket_Size ASC LIMIT 10;
    """
    return pd.read_sql(query, engine)

# ==========================================
# SCENARIO 5: User Engagement (Detailed)
# ==========================================
def engage_opens_vs_reg(year, quarter):
    """5.1 Scatter Plot: Registered Users vs App Opens"""
    query = f"""
    SELECT State, SUM(Registered_Users) as Reg_Users, SUM(App_Opens) as App_Opens
    FROM map_user
    WHERE Year = {year} AND Quarter = {quarter}
    GROUP BY State;
    """
    return pd.read_sql(query, engine)

def engage_state_ranking(year):
    """5.2 User Engagement Ranking"""
    query = f"""
    SELECT State, SUM(App_Opens) as Total_Opens
    FROM map_user
    WHERE Year = {year}
    GROUP BY State
    ORDER BY Total_Opens DESC;
    """
    return pd.read_sql(query, engine)

# 5.3 Engagement Rate by District (App Opens per User)
def engage_district_rate(year, state):
    query = f"""
    SELECT District, 
           (SUM(App_Opens) / SUM(Registered_Users)) * 100 as Engagement_Rate
    FROM map_user
    WHERE Year = {year} AND State = '{state}'
    GROUP BY District
    ORDER BY Engagement_Rate DESC;
    """
    return pd.read_sql(query, engine)

# 5.4 State Engagement Efficiency (Top states by Opens per User)
def engage_state_efficiency(year):
    query = f"""
    SELECT State, SUM(App_Opens) as Total_Opens, SUM(Registered_Users) as Total_Users,
           (SUM(App_Opens) / SUM(Registered_Users)) as Opens_Per_User
    FROM map_user
    WHERE Year = {year}
    GROUP BY State
    ORDER BY Opens_Per_User DESC;
    """
    return pd.read_sql(query, engine)

# 5.5 User Retention Proxy (Consistency of Opens across Quarters)
def engage_consistency(state, year):
    query = f"""
    SELECT Quarter, SUM(App_Opens) as Total_Opens
    FROM map_user
    WHERE State = '{state}' AND Year = {year}
    GROUP BY Quarter
    ORDER BY Quarter;
    """
    return pd.read_sql(query, engine)

# ==========================================
# SCENARIO 6: Insurance Engagement (Deep Dive)
# ==========================================
def ins_district_penetration(state, year):
    """6.1 Insurance Deep Dive for specific State"""
    query = f"""
    SELECT District, SUM(Insurance_Count) as Count, SUM(Insurance_Amount) as Amount
    FROM map_insurance
    WHERE State = '{state}' AND Year = {year}
    GROUP BY District;
    """
    return pd.read_sql(query, engine)

# 6.2 Opportunity Gap: High Users vs. Low Insurance (The "Untapped" Market)
def ins_opportunity_gap(year):
    query = f"""
    SELECT u.State, u.Total_Users, i.Policies_Sold
    FROM 
        (SELECT State, SUM(User_Count) as Total_Users FROM aggregated_user WHERE Year = {year} GROUP BY State) u
    JOIN 
        (SELECT State, SUM(Transaction_Count) as Policies_Sold FROM aggregated_insurance WHERE Year = {year} GROUP BY State) i
    ON u.State = i.State
    ORDER BY u.Total_Users DESC;
    """
    return pd.read_sql(query, engine)

# 6.3 State Insurance Growth Rate (YoY)
def ins_state_growth(year):
    query = f"""
    WITH Curr AS (SELECT State, SUM(Transaction_Count) as Val FROM aggregated_insurance WHERE Year = {year} GROUP BY State),
         Prev AS (SELECT State, SUM(Transaction_Count) as Val FROM aggregated_insurance WHERE Year = {year-1} GROUP BY State)
    SELECT c.State, ((c.Val - p.Val)/p.Val)*100 as Growth_Pct
    FROM Curr c JOIN Prev p ON c.State = p.State
    ORDER BY Growth_Pct DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

# 6.4 Seasonal Insurance Trends (Q1 vs Q4)
def ins_seasonal_trend(year):
    query = f"""
    SELECT Quarter, SUM(Transaction_Count) as Policies_Sold
    FROM aggregated_insurance
    WHERE Year = {year}
    GROUP BY Quarter
    ORDER BY Quarter;
    """
    return pd.read_sql(query, engine)

# 6.5 Top District Penetration (Deep Dive)
def ins_top_district_penetration(year):
    query = f"""
    SELECT District, SUM(Insurance_Count) as Total_Policies
    FROM map_insurance
    WHERE Year = {year}
    GROUP BY District
    ORDER BY Total_Policies DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

# ==========================================
# SCENARIOS 7, 8, 9: "TOP" LEADERBOARDS
# ==========================================
def top_trans_districts(year, quarter):
    """7.1 Top 10 Districts (Transaction Volume)"""
    query = f"""
    SELECT Entity_Name as District, Transaction_Count, Transaction_Amount
    FROM top_transaction
    WHERE Year = {year} AND Quarter = {quarter} AND Metric_Type = 'District'
    ORDER BY Transaction_Count DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

def top_trans_pincodes(year, quarter):
    """7.2 Top 10 Pincodes (Transaction Volume)"""
    query = f"""
    SELECT Entity_Name as Pincode, Transaction_Count
    FROM top_transaction
    WHERE Year = {year} AND Quarter = {quarter} AND Metric_Type = 'Pincode'
    ORDER BY Transaction_Count DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

# 7.3 Top 10 States (Volume)
def top_trans_states(year, quarter):
    query = f"""
    SELECT State, SUM(Transaction_Count) as Total_Count
    FROM aggregated_transaction
    WHERE Year = {year} AND Quarter = {quarter}
    GROUP BY State
    ORDER BY Total_Count DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

# 7.4 Bottom 10 Districts (Least Digital Adoption)
def bottom_trans_districts(year, quarter):
    query = f"""
    SELECT Entity_Name as District, Transaction_Count
    FROM top_transaction
    WHERE Year = {year} AND Quarter = {quarter} AND Metric_Type = 'District'
    ORDER BY Transaction_Count ASC LIMIT 10;
    """
    return pd.read_sql(query, engine)

# 7.5 Pincode Reach (Count of Active Pincodes per State)
def trans_pincode_reach(year):
    query = f"""
    SELECT State, COUNT(DISTINCT Entity_Name) as Pincodes_Covered
    FROM top_transaction
    WHERE Year = {year} AND Metric_Type = 'Pincode'
    GROUP BY State
    ORDER BY Pincodes_Covered DESC;
    """
    return pd.read_sql(query, engine)

def top_user_districts(year, quarter):
    """8.1 Top 10 Districts (User Base)"""
    query = f"""
    SELECT Entity_Name as District, Registered_Users
    FROM top_user
    WHERE Year = {year} AND Quarter = {quarter} AND Metric_Type = 'District'
    ORDER BY Registered_Users DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

def top_user_pincodes(year, quarter):
    """8.2 Top 10 Pincodes (User Base)"""
    query = f"""
    SELECT Entity_Name as Pincode, Registered_Users
    FROM top_user
    WHERE Year = {year} AND Quarter = {quarter} AND Metric_Type = 'Pincode'
    ORDER BY Registered_Users DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

# 8.3 Top 10 States (Registered Users)
def top_user_states(year, quarter):
    query = f"""
    SELECT State, SUM(User_Count) as Total_Users
    FROM aggregated_user
    WHERE Year = {year} AND Quarter = {quarter}
    GROUP BY State
    ORDER BY Total_Users DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

# 8.4 Bottom 10 Districts (Growth Opportunities)
def bottom_user_districts(year, quarter):
    query = f"""
    SELECT Entity_Name as District, Registered_Users
    FROM top_user
    WHERE Year = {year} AND Quarter = {quarter} AND Metric_Type = 'District'
    ORDER BY Registered_Users ASC LIMIT 10;
    """
    return pd.read_sql(query, engine)

# 8.5 Pincode Density (Users per Pincode - Top 10)
def top_user_density_pincode(year, quarter):
    query = f"""
    SELECT Entity_Name as Pincode, Registered_Users
    FROM top_user
    WHERE Year = {year} AND Quarter = {quarter} AND Metric_Type = 'Pincode'
    ORDER BY Registered_Users DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

def top_ins_districts(year, quarter):
    """9.1 Top 10 Districts (Insurance)"""
    query = f"""
    SELECT Entity_Name as District, Insurance_Count
    FROM top_insurance
    WHERE Year = {year} AND Quarter = {quarter} AND Metric_Type = 'District'
    ORDER BY Insurance_Count DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

def top_ins_pincodes(year, quarter):
    """9.2 Top 10 Pincodes (Insurance)"""
    query = f"""
    SELECT Entity_Name as Pincode, Insurance_Count
    FROM top_insurance
    WHERE Year = {year} AND Quarter = {quarter} AND Metric_Type = 'Pincode'
    ORDER BY Insurance_Count DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

# 9.3 Top 10 States (Insurance Volume)
def top_ins_states(year, quarter):
    query = f"""
    SELECT State, SUM(Transaction_Count) as Policies_Sold
    FROM aggregated_insurance
    WHERE Year = {year} AND Quarter = {quarter}
    GROUP BY State
    ORDER BY Policies_Sold DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)

# 9.4 Bottom 10 Districts (Insurance Laggards)
def bottom_ins_districts(year, quarter):
    query = f"""
    SELECT Entity_Name as District, Insurance_Count
    FROM top_insurance
    WHERE Year = {year} AND Quarter = {quarter} AND Metric_Type = 'District'
    ORDER BY Insurance_Count ASC LIMIT 10;
    """
    return pd.read_sql(query, engine)

# 9.5 Pincode Leaderboard (Amount vs Count)
def top_ins_pincode_value(year, quarter):
    query = f"""
    SELECT Entity_Name as Pincode, Insurance_Amount
    FROM top_insurance
    WHERE Year = {year} AND Quarter = {quarter} AND Metric_Type = 'Pincode'
    ORDER BY Insurance_Amount DESC LIMIT 10;
    """
    return pd.read_sql(query, engine)