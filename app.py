# app.py — PhonePe Pulse (Home + Case Studies, clickable 3D map on Home)
# Run: streamlit run app.py

import json, requests
import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st
from sqlalchemy import create_engine
import os;
from dotenv import load_dotenv, dotenv_values 

# Load variables from .env file
load_dotenv()

PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")

# ----------------- PAGE -----------------
st.set_page_config(page_title="PhonePe Pulse — Dashboard & Case Studies", layout="wide")

# ----------------- DB -----------------
engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")

def run_df(sql: str, params=None) -> pd.DataFrame:
    return pd.read_sql(sql, engine, params=params or {})

# ----------------- GEOJSON (India) -----------------
INDIA_GEOJSON_URL = (
    "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/"
    "raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
)

@st.cache_data(show_spinner=False)
def load_geojson():
    r = requests.get(INDIA_GEOJSON_URL, timeout=30)
    r.raise_for_status()
    return r.json()

INDIA_GEOJSON = load_geojson()

# ----------------- NAME NORMALIZATION (DB <-> GeoJSON ST_NM) -----------------
NAME_TO_ST_NM = {
    "Andaman And Nicobar": "Andaman & Nicobar Islands",
    "Dadra And Nagar Haveli And Daman Diu": "Dadra and Nagar Haveli and Daman and Diu",
    "Nct Of Delhi": "Delhi",
    "Jammu And Kashmir": "Jammu & Kashmir",
    "Pondicherry": "Puducherry",
    "Orissa": "Odisha",
}
def to_st_nm(s: str) -> str:
    if not isinstance(s, str): return s
    return NAME_TO_ST_NM.get(s.strip(), s.strip())

# DB state on click
ST_NM_TO_NAME = {v: k for k, v in NAME_TO_ST_NM.items()}
def stnm_to_db(s: str) -> str:
    if not isinstance(s, str): return s
    return ST_NM_TO_NAME.get(s.strip(), s.strip())

# ----------------- SHARED HELPERS -----------------
@st.cache_data(show_spinner=False)
def years_quarters():
    df = run_df("SELECT DISTINCT year, quarter FROM aggregated_transaction ORDER BY year, quarter;")
    if df.empty:
        return [2021], [1,2,3,4]
    return sorted(df["year"].unique().tolist()), sorted(df["quarter"].unique().tolist())

@st.cache_data(show_spinner=False)
def quarters_for_year(y:int):
    df = run_df("SELECT DISTINCT quarter FROM aggregated_transaction WHERE year=%s ORDER BY quarter;", (y,))
    return df["quarter"].tolist() or [1,2,3,4]

def df_state_txn_amount(y:int, q:int) -> pd.DataFrame:
    df = run_df("""SELECT state, SUM(transaction_amount) AS value
                   FROM aggregated_transaction
                   WHERE year=%(y)s AND quarter=%(q)s
                   GROUP BY state;""", {"y":y,"q":q})
    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0.0)
    return df

def df_state_users(y:int, q:int) -> pd.DataFrame:
    df = run_df("""SELECT state, SUM(registered_users) AS value
                   FROM map_user
                   WHERE year=%(y)s AND quarter=%(q)s
                   GROUP BY state;""", {"y":y,"q":q})
    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0.0)
    return df

def india_txn_kpis(y:int,q:int):
    row = run_df("""SELECT SUM(transaction_amount) amt, SUM(transaction_count) cnt
                    FROM aggregated_transaction WHERE year=%(y)s AND quarter=%(q)s;""",
                 {"y":y,"q":q}).iloc[0]
    return float(row["amt"] or 0), int(row["cnt"] or 0)

def india_user_kpis(y:int,q:int):
    row = run_df("""SELECT SUM(registered_users) ru, SUM(app_opens) ao
                    FROM map_user WHERE year=%(y)s AND quarter=%(q)s;""",
                 {"y":y,"q":q}).iloc[0]
    return int(row["ru"] or 0), int(row["ao"] or 0)

def state_txn_kpis(state:str,y:int,q:int):
    row = run_df("""SELECT SUM(transaction_amount) amt, SUM(transaction_count) cnt
                    FROM aggregated_transaction
                    WHERE state=%(s)s AND year=%(y)s AND quarter=%(q)s;""",
                 {"s":state,"y":y,"q":q}).iloc[0]
    return float(row["amt"] or 0), int(row["cnt"] or 0)

def state_user_kpis(state:str,y:int,q:int):
    row = run_df("""SELECT SUM(registered_users) ru, SUM(app_opens) ao
                    FROM map_user
                    WHERE state=%(s)s AND year=%(y)s AND quarter=%(q)s;""",
                 {"s":state,"y":y,"q":q}).iloc[0]
    return int(row["ru"] or 0), int(row["ao"] or 0)

def by_category(state:str,y:int,q:int):
    return run_df("""SELECT transaction_type,
                            SUM(transaction_amount) AS amount,
                            SUM(transaction_count)  AS cnt
                     FROM aggregated_transaction
                     WHERE state=%(s)s AND year=%(y)s AND quarter=%(q)s
                     GROUP BY transaction_type ORDER BY amount DESC NULLS LAST;""",
                  {"s":state,"y":y,"q":q})

def state_series(state:str):
    df = run_df("""SELECT year, quarter, SUM(transaction_amount) AS amount
                   FROM aggregated_transaction WHERE state=%(s)s
                   GROUP BY year, quarter ORDER BY year, quarter;""",
                {"s":state})
    if not df.empty:
        df["period"] = df["year"].astype(str)+"-Q"+df["quarter"].astype(str)
    return df

def top_geo(state:str,y:int,q:int,etype:str):
    return run_df("""SELECT entity_name AS name, SUM(count) AS txns, SUM(amount) AS amount
                     FROM top_map
                     WHERE state=%(s)s AND year=%(y)s AND quarter=%(q)s AND entity_type=%(t)s
                     GROUP BY entity_name ORDER BY amount DESC NULLS LAST LIMIT 10;""",
                  {"s":state,"y":y,"q":q,"t":etype})

def districts_for(state:str,y:int,q:int, view:str):
    if view == "Transactions":
        df = run_df("""SELECT DISTINCT entity_name AS district
                       FROM top_map
                       WHERE state=%(s)s AND year=%(y)s AND quarter=%(q)s AND entity_type='District'
                       ORDER BY district;""", {"s":state,"y":y,"q":q})
    else:
        df = run_df("""SELECT DISTINCT name AS district
                       FROM map_user
                       WHERE state=%(s)s AND year=%(y)s AND quarter=%(q)s
                       ORDER BY district;""", {"s":state,"y":y,"q":q})
    return df["district"].tolist()

def pincodes_for(state:str,y:int,q:int, view:str):
    if view == "Transactions":
        df = run_df("""SELECT DISTINCT entity_name AS pincode
                       FROM top_map
                       WHERE state=%(s)s AND year=%(y)s AND quarter=%(q)s AND entity_type='Pincode'
                       ORDER BY pincode;""", {"s":state,"y":y,"q":q})
    else:
        df = run_df("""SELECT DISTINCT entity_name AS pincode
                       FROM top_user
                       WHERE state=%(s)s AND year=%(y)s AND quarter=%(q)s AND entity_type='Pincode'
                       ORDER BY pincode;""", {"s":state,"y":y,"q":q})
    return df["pincode"].tolist()

def choropleth(df_states_value: pd.DataFrame, title: str, color_scale="Blues"):
    d = df_states_value.copy()
    d["ST_NM"] = d["state"].map(to_st_nm)
    d = d[d["ST_NM"].notna() & (d["ST_NM"]!="")]
    fig = px.choropleth(
        d, geojson=INDIA_GEOJSON, featureidkey="properties.ST_NM",
        locations="ST_NM", color="value", color_continuous_scale=color_scale,
        hover_name="ST_NM", hover_data={"value":":.2f"}
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin=dict(l=0,r=0,t=40,b=0), height=520, title=title)
    st.plotly_chart(fig, use_container_width=True)

# ----------------- CLICKABLE MAP -----------------
def build_feature_collection(df_plot: pd.DataFrame) -> dict:
    vals = {to_st_nm(r["state"]): float(r["value"] or 0) for _, r in df_plot.iterrows()}
    vmin = min(vals.values()) if vals else 0.0
    vmax = max(vals.values()) if vals else 1.0
    rng  = (vmax - vmin) or 1.0

    feats = []
    for f in INDIA_GEOJSON["features"]:
        st_nm = f["properties"]["ST_NM"]
        v     = vals.get(st_nm, 0.0)
        norm  = (v - vmin)/rng
        g = json.loads(json.dumps(f))  
        g["properties"]["metric_value"] = v
        g["properties"]["height"] = 200000 + norm * 700000  
        g["properties"]["fill_r"] = 66
        g["properties"]["fill_g"] = 135
        g["properties"]["fill_b"] = 245
        g["properties"]["fill_a"] = 180
        feats.append(g)
    return {"type":"FeatureCollection", "features":feats}

def get_selected_state_from_event(event):
    """Parse pydeck selection payload and return DB-style state name (or None)."""
    sel = None
    if hasattr(event, "selection"):
        sel = event.selection
    elif isinstance(event, dict):
        sel = event.get("selection", event)
    if not isinstance(sel, dict):
        return None

    objs = []
    if "objects" in sel:
        o = sel["objects"]
        if isinstance(o, list):
            objs = o
        elif isinstance(o, dict):
            for v in o.values():
                if isinstance(v, list) and v:
                    objs = v; break
                if isinstance(v, dict):
                    objs = [v]; break
    elif "object" in sel:
        objs = [sel["object"]]
    if not objs:
        return None

    obj = objs[0]
    if isinstance(obj, dict) and "object" in obj:
        obj = obj["object"]
    props = obj.get("properties") if isinstance(obj, dict) else None
    if not isinstance(props, dict):
        return None

    st_nm = props.get("ST_NM") or props.get("state") or props.get("name")
    if isinstance(st_nm, str) and st_nm.strip():
        return stnm_to_db(st_nm.strip())
    return None

# =============================================================================
# HOME (Dashboard)
# =============================================================================
def render_home():
    st.title("Home — India Overview")

    years, _ = years_quarters()
    default_y = years[-1]
    y = st.selectbox("Year", years, index=years.index(default_y), key="home_y")
    q = st.selectbox("Quarter", quarters_for_year(y), index=len(quarters_for_year(y))-1, key="home_q")
    view = st.selectbox("View", ["Transactions", "Users"], index=0, key="home_view")

    left, right = st.columns([3,1], gap="large")

    # --- Left: 3D CLICKABLE pydeck map ---
    with left:
        if view == "Transactions":
            df_metric = df_state_txn_amount(y, q)
            st.subheader(f"3D Map — Total Payment Value • Q{q} {y}")
        else:
            df_metric = df_state_users(y, q)
            st.subheader(f"3D Map — Registered Users • Q{q} {y}")

        if df_metric.empty:
            st.info("No data for selected period.")
        else:
            df_plot = df_metric.copy()
            df_plot["ST_NM"] = df_plot["state"].map(to_st_nm)

            fc = build_feature_collection(df_plot)
            layer = pdk.Layer(
                "GeoJsonLayer",
                data=fc,
                id="india-states",
                pickable=True,
                auto_highlight=True,
                extruded=True,
                stroked=True,
                filled=True,
                get_elevation="properties.height",
                get_fill_color="[properties.fill_r, properties.fill_g, properties.fill_b, properties.fill_a]",
                get_line_color=[255,255,255],
                lineWidthMinPixels=1,
            )
            view_state = pdk.ViewState(latitude=22.7, longitude=78.9, zoom=3.6, bearing=0, pitch=35)
            deck = pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                map_style=None,
                tooltip={"text": "{ST_NM}\n₹{metric_value}"},
            )

            event = st.pydeck_chart(
                deck,
                use_container_width=True,
                height=520,
                on_select="rerun",
                selection_mode="single-object",
                key=f"home_map_{view}_{y}_{q}",
            )

            clicked = get_selected_state_from_event(event)
            if clicked:
                st.session_state["home_clicked_state"] = clicked

                
        # State dropdown (prefer clicked state; else default = max metric)
        if view == "Transactions":
            df_for_default = df_state_txn_amount(y, q)
        else:
            df_for_default = df_state_users(y, q)

        default_state = df_for_default.sort_values("value", ascending=False).iloc[0]["state"] if not df_for_default.empty else None
        states = run_df("SELECT DISTINCT state FROM aggregated_transaction WHERE year=%s AND quarter=%s ORDER BY state;", (y,q))["state"].tolist()

        preferred = st.session_state.get("home_clicked_state")
        if preferred in states:
            idx = states.index(preferred)
        elif default_state and default_state in states:
            idx = states.index(default_state)
        else:
            idx = 0 if states else 0

        state = st.selectbox("State", states, index=idx, key="home_state")

        # State KPIs + drilldowns
        if view == "Transactions":
            a_s, c_s = state_txn_kpis(state, y, q)
            st.metric("State — Total Payment Value", f"{a_s:,.0f}")
            st.metric("State — All Transactions", f"{c_s:,}")

            st.write("By category")
            df_cat = by_category(state, y, q)
            st.dataframe(df_cat, use_container_width=True, height=240)
            if not df_cat.empty:
                st.bar_chart(df_cat.set_index("transaction_type")["amount"])

            dlist = districts_for(state, y, q, "Transactions")
            plist = pincodes_for(state, y, q, "Transactions")
            st.selectbox("District", dlist or ["—"], key="home_txn_district")
            st.selectbox("Pincode",  plist or ["—"], key="home_txn_pincode")

            st.write("Top Districts (by amount)")
            st.dataframe(top_geo(state, y, q, "District"), use_container_width=True, height=220)
            st.write("Top Pincodes (by amount)")
            st.dataframe(top_geo(state, y, q, "Pincode"), use_container_width=True, height=220)

        else:
            ru_s, ao_s = state_user_kpis(state, y, q)
            st.metric("State — Registered Users", f"{ru_s:,}")
            st.metric("State — App Opens", f"{ao_s:,}")

            dlist = districts_for(state, y, q, "Users")
            plist = pincodes_for(state, y, q, "Users")
            st.selectbox("District", dlist or ["—"], key="home_user_district")
            st.selectbox("Pincode",  plist or ["—"], key="home_user_pincode")

            df_dist_u = run_df("""SELECT name AS district, SUM(registered_users) AS users, SUM(app_opens) AS app_opens
                                  FROM map_user
                                  WHERE state=%(s)s AND year=%(y)s AND quarter=%(q)s
                                  GROUP BY name ORDER BY users DESC NULLS LAST LIMIT 25;""",
                                {"s":state,"y":y,"q":q})
            st.write("Top Districts (Registered Users)")
            st.dataframe(df_dist_u, use_container_width=True, height=220)

            df_pin_u = run_df("""SELECT entity_name AS pincode, SUM(registered_users) AS users
                                 FROM top_user
                                 WHERE state=%(s)s AND year=%(y)s AND quarter=%(q)s AND entity_type='Pincode'
                                 GROUP BY entity_name ORDER BY users DESC NULLS LAST LIMIT 25;""",
                               {"s":state,"y":y,"q":q})
            st.write("Top Pincodes (Registered Users)")
            st.dataframe(df_pin_u, use_container_width=True, height=220)

    # --- Right: KPIs + details ---
    with right:
        if view == "Transactions":
            st.subheader("India totals (Transactions)")
            amt, cnt = india_txn_kpis(y, q)
            atv = (amt / cnt) if cnt else 0
            st.metric("Total Payment Value", f"{amt:,.0f}")
            st.metric("All Transactions", f"{cnt:,}")
            st.metric("Avg. Transaction Value", f"{atv:,.0f}")
        else:
            st.subheader("India totals (Users)")
            ru, ao = india_user_kpis(y, q)
            st.metric("Registered Users", f"{ru:,}")
            st.metric("App Opens", f"{ao:,}")

        st.divider()
        
# =============================================================================
# CASE STUDIES (each has 5 analyses)
# =============================================================================
def render_cs1(year:int, quarter:int):
    st.header("Decoding Transaction Dynamics")
    st.caption("Five focused analyses for the chosen Year and Quarter.")
    st.subheader("1) Total Transaction Amount — Choropleth (by State)")
    df = run_df("""SELECT state, SUM(transaction_amount)::numeric AS value
                   FROM aggregated_transaction
                   WHERE year=%(y)s AND quarter=%(q)s GROUP BY state;""", {"y":year,"q":quarter})
    choropleth(df, f"Total Transaction Amount — Q{quarter} {year}", "Blues")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("2) Payment Method Popularity — by Count")
        pie_cnt = run_df("""SELECT transaction_type, SUM(transaction_count) AS cnt
                            FROM aggregated_transaction
                            WHERE year=%(y)s AND quarter=%(q)s
                            GROUP BY transaction_type ORDER BY cnt DESC;""", {"y":year,"q":quarter})
        if not pie_cnt.empty:
            st.plotly_chart(px.pie(pie_cnt, values="cnt", names="transaction_type", hole=0.4),
                            use_container_width=True, height=420)
    with c2:
        st.subheader("3) Payment Method Popularity — by Amount")
        pie_amt = run_df("""SELECT transaction_type, SUM(transaction_amount)::numeric AS amt
                            FROM aggregated_transaction
                            WHERE year=%(y)s AND quarter=%(q)s
                            GROUP BY transaction_type ORDER BY amt DESC;""", {"y":year,"q":quarter})
        if not pie_amt.empty:
            st.plotly_chart(px.pie(pie_amt, values="amt", names="transaction_type", hole=0.4),
                            use_container_width=True, height=420)

    st.subheader("4) Top 10 States by Amount")
    top10 = run_df("""SELECT state, SUM(transaction_amount)::numeric AS amount
                      FROM aggregated_transaction
                      WHERE year=%(y)s AND quarter=%(q)s
                      GROUP BY state ORDER BY amount DESC NULLS LAST LIMIT 10;""",
                   {"y":year,"q":quarter})
    if not top10.empty:
        st.plotly_chart(px.bar(top10, x="state", y="amount"), use_container_width=True, height=420)

    st.subheader("5) Transactions by Category in a State")
    states = run_df("""SELECT DISTINCT state FROM aggregated_transaction
                       WHERE year=%(y)s AND quarter=%(q)s ORDER BY state;""", {"y":year,"q":quarter})["state"].tolist()
    pick = st.selectbox("State", states, key=f"cs1_state_{year}_{quarter}")
    if pick:
        dist = run_df("""SELECT transaction_type, SUM(transaction_amount)::numeric AS amount
                         FROM aggregated_transaction
                         WHERE state=%(s)s AND year=%(y)s AND quarter=%(q)s
                         GROUP BY transaction_type ORDER BY amount DESC;""",
                      {"s":pick,"y":year,"q":quarter})
        st.plotly_chart(px.line(dist, x="transaction_type", y="amount", markers=True),
                        use_container_width=True, height=420)

def render_cs2(year:int, quarter:int):
    """Device Dominance & Engagement — uses latest COMMON period across aggregated_user & map_user."""
    def latest_common_period():
        df = run_df("""
            WITH a AS (SELECT year, quarter FROM aggregated_user GROUP BY 1,2),
                 m AS (SELECT year, quarter FROM map_user        GROUP BY 1,2)
            SELECT year, quarter
            FROM a INNER JOIN m USING (year, quarter)
            ORDER BY year DESC, quarter DESC
            LIMIT 1;
        """)
        return (int(df.iloc[0]["year"]), int(df.iloc[0]["quarter"])) if not df.empty else (None, None)

    y_common, q_common = latest_common_period()
    if y_common is None:
        st.warning("No overlapping period between aggregated_user and map_user. Load data for both tables.")
        return

    st.header("Device Dominance & Engagement")
    st.caption(f"Period used for all analyses: Q{q_common} {y_common} (latest common across user tables)")

    brands = run_df("""
        SELECT brand, SUM(count) AS users, ROUND(AVG(percentage)*100, 2) AS avg_share_pct
        FROM aggregated_user
        WHERE year=%(y)s AND quarter=%(q)s
        GROUP BY brand
        ORDER BY users DESC;
    """, {"y": y_common, "q": q_common})
    if not brands.empty:
        st.subheader("1) Brand Dominance — Users")
        st.plotly_chart(px.bar(brands, x="brand", y="users"),
                        use_container_width=True, height=420)

    st.subheader("2) Engagement — App Opens per User (by State)")
    eng = run_df("""
        SELECT state,
               SUM(registered_users) AS reg_users,
               SUM(app_opens)        AS app_opens,
               ROUND(SUM(app_opens)::numeric / NULLIF(SUM(registered_users),0), 2) AS opens_per_user
        FROM map_user
        WHERE year=%(y)s AND quarter=%(q)s
        GROUP BY state
        ORDER BY opens_per_user DESC NULLS LAST;
    """, {"y": y_common, "q": q_common})
    if not eng.empty:
        st.plotly_chart(px.bar(eng.head(20), x="state", y="opens_per_user"),
                        use_container_width=True, height=420)

    st.subheader("3) Top Brand per State")
    tb = run_df("""
        WITH b AS (
          SELECT state, brand, SUM(count) AS users
          FROM aggregated_user
          WHERE year=%(y)s AND quarter=%(q)s
          GROUP BY state, brand
        ),
        r AS (
          SELECT state, brand, users,
                 ROW_NUMBER() OVER (PARTITION BY state ORDER BY users DESC) rn
          FROM b
        )
        SELECT state, brand AS top_brand, users AS top_brand_users
        FROM r WHERE rn = 1 ORDER BY top_brand_users DESC NULLS LAST;
    """, {"y": y_common, "q": q_common})
    st.dataframe(tb, use_container_width=True, height=360)

    av_brands = run_df("""
        SELECT DISTINCT brand
        FROM aggregated_user
        WHERE year=%(y)s AND quarter=%(q)s
        ORDER BY brand;
    """, {"y": y_common, "q": q_common})["brand"].dropna().tolist()
    st.subheader("4) Brand — Users over Time")
    pick = st.selectbox("Choose a brand", av_brands, key=f"cs2_brand_{y_common}_{q_common}")
    if pick:
        ts = run_df("""
            SELECT year, quarter, SUM(count) AS users
            FROM aggregated_user
            WHERE brand=%(b)s
            GROUP BY year, quarter
            ORDER BY year, quarter;
        """, {"b": pick})
        if not ts.empty:
            ts["period"] = ts["year"].astype(str) + "-Q" + ts["quarter"].astype(str)
            st.plotly_chart(px.line(ts, x="period", y="users", markers=True),
                            use_container_width=True, height=420)

    st.subheader("5) Brand Share vs Engagement")
    if pick:
        bse = run_df("""
            WITH share AS (
              SELECT au.state,
                     100.0 * SUM(CASE WHEN au.brand=%(b)s THEN au.count ELSE 0 END)
                     / NULLIF(SUM(au.count),0) AS brand_share_pct
              FROM aggregated_user au
              WHERE au.year=%(y)s AND au.quarter=%(q)s
              GROUP BY au.state
            ),
            eng AS (
              SELECT mu.state,
                     ROUND(SUM(mu.app_opens)::numeric / NULLIF(SUM(mu.registered_users),0), 2) AS opens_per_user
              FROM map_user mu
              WHERE mu.year=%(y)s AND mu.quarter=%(q)s
              GROUP BY mu.state
            )
            SELECT s.state, ROUND(s.brand_share_pct,2) AS brand_share_pct, e.opens_per_user
            FROM share s LEFT JOIN eng e USING (state)
            WHERE s.brand_share_pct IS NOT NULL
            ORDER BY brand_share_pct DESC NULLS LAST;
        """, {"b": pick, "y": y_common, "q": q_common})
        st.dataframe(bse, use_container_width=True, height=320)
        if not bse.empty:
            m = bse.rename(columns={"brand_share_pct": "value"}).copy()
            choropleth(m, f"{pick} — Brand share (%) by State", "Blues")

def render_cs3(year:int, quarter:int):
    st.header("Insurance Penetration & Growth")
    st.subheader("1) Insurance Amount — Choropleth")
    df = run_df("""SELECT state, SUM(insurance_amount)::numeric AS value
                   FROM aggregated_insurance
                   WHERE year=%(y)s AND quarter=%(q)s GROUP BY state;""", {"y":year,"q":quarter})
    choropleth(df, f"Insurance Amount — Q{quarter} {year}", "Purples")

    st.subheader("2) Insurance Type Share (All-India)")
    pie = run_df("""SELECT insurance_type, SUM(insurance_amount)::numeric AS amount
                    FROM aggregated_insurance
                    WHERE year=%(y)s AND quarter=%(q)s
                    GROUP BY insurance_type ORDER BY amount DESC;""", {"y":year,"q":quarter})
    if not pie.empty:
        st.plotly_chart(px.pie(pie, names="insurance_type", values="amount", hole=0.4),
                        use_container_width=True, height=420)

    states = run_df("""SELECT DISTINCT state FROM map_insurance
                       WHERE year=%(y)s AND quarter=%(q)s ORDER BY state;""",
                    {"y":year,"q":quarter})["state"].tolist()
    pick = st.selectbox("3) Top Districts (select State)", states, key=f"cs3_state_{year}_{quarter}")
    if pick:
        topd = run_df("""SELECT name AS district, SUM(insurance_amount)::numeric AS amount,
                                SUM(insurance_count) AS cnt
                         FROM map_insurance
                         WHERE state=%(s)s AND year=%(y)s AND quarter=%(q)s
                         GROUP BY name ORDER BY amount DESC NULLS LAST LIMIT 20;""",
                      {"s":pick,"y":year,"q":quarter})
        st.plotly_chart(px.bar(topd, x="district", y="amount"),
                        use_container_width=True, height=420)

    st.subheader("4) YoY Growth (Insurance Amount) by State")
    yoy = run_df("""
        WITH cur AS (
          SELECT state, SUM(insurance_amount) AS amt
          FROM aggregated_insurance WHERE year=%(y)s AND quarter=%(q)s
          GROUP BY state
        ),
        prev AS (
          SELECT state, SUM(insurance_amount) AS amt
          FROM aggregated_insurance WHERE year=%(y)s - 1 AND quarter=%(q)s
          GROUP BY state
        )
        SELECT c.state, c.amt::numeric AS cur_amount,
               p.amt::numeric AS prev_amount,
               ROUND(100*(c.amt - p.amt)/NULLIF(p.amt,0), 2) AS yoy_pct
        FROM cur c LEFT JOIN prev p USING (state)
        ORDER BY yoy_pct DESC NULLS LAST;""", {"y":year,"q":quarter})
    st.dataframe(yoy, use_container_width=True, height=360)

    st.subheader("5) Insurance vs Transactions — State Ratio")
    ratio = run_df("""
        WITH ins AS (
          SELECT state, SUM(insurance_amount) AS ins_amt
          FROM aggregated_insurance WHERE year=%(y)s AND quarter=%(q)s GROUP BY state
        ),
        txn AS (
          SELECT state, SUM(transaction_amount) AS txn_amt
          FROM aggregated_transaction WHERE year=%(y)s AND quarter=%(q)s GROUP BY state
        )
        SELECT COALESCE(t.state, i.state) AS state,
               i.ins_amt::numeric AS insurance_amount,
               t.txn_amt::numeric AS transaction_amount,
               ROUND(100 * i.ins_amt / NULLIF(t.txn_amt,0), 2) AS ins_vs_txn_pct
        FROM txn t FULL JOIN ins i ON i.state=t.state
        ORDER BY ins_vs_txn_pct DESC NULLS LAST;""", {"y":year,"q":quarter})
    st.dataframe(ratio, use_container_width=True, height=360)

def render_cs4(year:int, quarter:int):
    st.header("Transaction Analysis Across Geographies")
    st.subheader("1) Top Districts (by Amount)")
    d1 = run_df("""SELECT entity_name AS district, state, SUM(count) AS txns, SUM(amount)::numeric AS amount
                   FROM top_map
                   WHERE year=%(y)s AND quarter=%(q)s AND entity_type='District'
                   GROUP BY entity_name, state
                   ORDER BY amount DESC NULLS LAST LIMIT 25;""", {"y":year,"q":quarter})
    st.dataframe(d1, use_container_width=True, height=360)

    st.subheader("2) Top Pincodes (by Amount)")
    p1 = run_df("""SELECT entity_name AS pincode, state, SUM(count) AS txns, SUM(amount)::numeric AS amount
                   FROM top_map
                   WHERE year=%(y)s AND quarter=%(q)s AND entity_type='Pincode'
                   GROUP BY entity_name, state
                   ORDER BY amount DESC NULLS LAST LIMIT 25;""", {"y":year,"q":quarter})
    st.dataframe(p1, use_container_width=True, height=360)

    st.subheader("3) Top States (by Amount)")
    s1 = run_df("""SELECT entity_name AS state, SUM(count) AS txns, SUM(amount)::numeric AS amount
                   FROM top_map
                   WHERE year=%(y)s AND quarter=%(q)s AND entity_type='State'
                   GROUP BY entity_name ORDER BY amount DESC NULLS LAST;""", {"y":year,"q":quarter})
    if not s1.empty:
        st.plotly_chart(px.bar(s1.head(15), x="state", y="amount"),
                        use_container_width=True, height=420)

    states = run_df("""SELECT DISTINCT state FROM top_map
                       WHERE year=%(y)s AND quarter=%(q)s AND entity_type='District'
                       ORDER BY state;""", {"y":year,"q":quarter})["state"].tolist()
    pick = st.selectbox("4) District Share inside a State", states, key=f"cs4_state_{year}_{quarter}")
    if pick:
        share = run_df("""
            WITH s AS (
              SELECT entity_name AS district, SUM(amount) AS amt
              FROM top_map
              WHERE entity_type='District' AND state=%(s)s AND year=%(y)s AND quarter=%(q)s
              GROUP BY entity_name
            ),
            tot AS (SELECT SUM(amt) total_amt FROM s)
            SELECT district, amt::numeric AS amount,
                   ROUND(100 * amt / NULLIF(t.total_amt,0), 2) AS share_pct
            FROM s CROSS JOIN tot t
            ORDER BY amount DESC NULLS LAST;""", {"s":pick,"y":year,"q":quarter})
        st.dataframe(share, use_container_width=True, height=360)

    st.subheader("5) District YoY Growth (selected state)")
    if pick:
        yoy = run_df("""
            WITH cur AS (
              SELECT entity_name AS district, SUM(amount) AS amt
              FROM top_map
              WHERE entity_type='District' AND state=%(s)s
                AND year=%(y)s AND quarter=%(q)s
              GROUP BY entity_name
            ),
            prev AS (
              SELECT entity_name AS district, SUM(amount) AS amt
              FROM top_map
              WHERE entity_type='District' AND state=%(s)s
                AND year=%(y)s - 1 AND quarter=%(q)s
              GROUP BY entity_name
            )
            SELECT c.district, c.amt::numeric AS cur_amount,
                   p.amt::numeric AS prev_amount,
                   ROUND(100*(c.amt - p.amt)/NULLIF(p.amt,0), 2) AS yoy_pct
            FROM cur c LEFT JOIN prev p USING (district)
            ORDER BY yoy_pct DESC NULLS LAST;""", {"s":pick,"y":year,"q":quarter})
        st.dataframe(yoy, use_container_width=True, height=360)

def render_cs5(year:int, quarter:int):
    st.header("User Registration Analysis")
    st.subheader("1) Registered Users — Choropleth")
    ru = run_df("""SELECT state, SUM(registered_users)::numeric AS value
                   FROM map_user WHERE year=%(y)s AND quarter=%(q)s GROUP BY state;""", {"y":year,"q":quarter})
    choropleth(ru, f"Registered Users — Q{quarter} {year}", "Greens")

    st.subheader("2) Top Districts by Registered Users — pick State")
    states = run_df("""SELECT DISTINCT state FROM map_user
                       WHERE year=%(y)s AND quarter=%(q)s ORDER BY state;""",
                    {"y":year,"q":quarter})["state"].tolist()
    pick = st.selectbox("State", states, key=f"cs5_state_{year}_{quarter}")
    if pick:
        dist = run_df("""SELECT name AS district, SUM(registered_users) AS users
                         FROM map_user
                         WHERE state=%(s)s AND year=%(y)s AND quarter=%(q)s
                         GROUP BY name ORDER BY users DESC LIMIT 25;""",
                      {"s":pick,"y":year,"q":quarter})
        st.plotly_chart(px.bar(dist, x="district", y="users"),
                        use_container_width=True, height=420)

    st.subheader("3) Top Pincodes by Registered Users")
    pz = run_df("""SELECT entity_name AS pincode, state, SUM(registered_users) AS users
                   FROM top_user
                   WHERE year=%(y)s AND quarter=%(q)s AND entity_type='Pincode'
                   GROUP BY entity_name, state
                   ORDER BY users DESC LIMIT 25;""", {"y":year,"q":quarter})
    st.dataframe(pz, use_container_width=True, height=360)

    st.subheader("4) App Opens per User — by State")
    aopu = run_df("""SELECT state, SUM(registered_users) AS reg_users, SUM(app_opens) AS app_opens,
                            ROUND(SUM(app_opens)::numeric / NULLIF(SUM(registered_users),0), 2) AS opens_per_user
                     FROM map_user WHERE year=%(y)s AND quarter=%(q)s
                     GROUP BY state ORDER BY opens_per_user DESC NULLS LAST;""",
                  {"y":year,"q":quarter})
    if not aopu.empty:
        st.plotly_chart(px.bar(aopu, x="state", y="opens_per_user"),
                        use_container_width=True, height=420)

    st.subheader("5) Quarterly Registration Trend — pick State")
    pick2 = st.selectbox("Trend State", states or [], key=f"cs5_state_trend_{year}_{quarter}")
    if pick2:
        tr = run_df("""SELECT year, quarter, SUM(registered_users) AS users
                       FROM map_user WHERE state=%(s)s
                       GROUP BY year, quarter ORDER BY year, quarter;""", {"s":pick2})
        if not tr.empty:
            tr["period"] = tr["year"].astype(str)+"-Q"+tr["quarter"].astype(str)
            st.plotly_chart(px.bar(tr, x="period", y="users"),
                            use_container_width=True, height=420)

# =============================================================================
# ROUTER (Tabs)
# =============================================================================
tab_home, tab_cs = st.tabs(["Home", "Case Studies"])

with tab_home:
    render_home()

with tab_cs:
    st.title("Business Case Studies")
    years, _ = years_quarters()
    default_y = years[-1]
    cs_year = st.selectbox("Year", years, index=years.index(default_y), key="cs_y")
    cs_quarter = st.selectbox("Quarter", quarters_for_year(cs_year),
                              index=len(quarters_for_year(cs_year))-1, key="cs_q")
    case_map = {
        "Decoding Transaction Dynamics": render_cs1,
        "Device Dominance & Engagement": render_cs2,
        "Insurance Penetration & Growth": render_cs3,
        "Transaction Analysis Across Geographies": render_cs4,
        "User Registration Analysis": render_cs5,
    }
    case_name = st.selectbox("Choose Case Study", list(case_map.keys()), key="cs_case")
    st.markdown("---")
    case_map[case_name](cs_year, cs_quarter)

st.caption("ETL → PostgreSQL (`phonepe_db`) → Streamlit. Home shows a 3D India overview with Transactions/Users; Case Studies render five analyses each. Click a state on the 3D map to set the right-panel dropdown. (Requires Streamlit 1.39+)")
