import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# -------------------------
# CONFIGURATION GÉNÉRALE
# -------------------------
st.set_page_config(layout="wide")

# CSS pour background + effet "glass" sur blocs
st.markdown("""
    <style>
        .stApp {
            background-image: url('https://assets.amazon.science/dims4/default/92125e7/2147483647/strip/true/crop/1440x810+0+75/resize/1440x810!/quality/90/?url=http%3A%2F%2Famazon-topics-brightspot.s3.amazonaws.com%2Fscience%2F33%2Ff5%2Fedc35fd14fa08e73b70294062a1a%2F2022-f1-car-silverstone-on-track.jpg');
            background-size: cover;
            background-position: center;
            background-attachment: fixed;
        }
        .block-container {
            background-color: rgba(255, 255, 255, 0.7) !important;
            border-radius: 20px;
            padding: 2rem;
        }
        .transparent-box {
            background: rgba(255, 255, 255, 0.5);
            border-radius: 16px;
            padding: 1rem;
            margin-bottom: 1rem;
        }
    </style>
""", unsafe_allow_html=True)

# -------------------------
# CHARGEMENT DONNÉES
# -------------------------
engine = create_engine("postgresql://postgres:postgres@localhost:5432/f1stream_db")

race_results_df = pd.read_sql("SELECT * FROM race_results", engine)
drivers_df = pd.read_sql("SELECT * FROM drivers", engine)

merged_df = race_results_df.merge(drivers_df, on="driver_number", how="left")
total_gps = merged_df["grand_prix"].nunique()

# -------------------------
# CALCUL CLASSEMENT + VICTOIRES
# -------------------------
victories_df = merged_df[merged_df["position"] == 1]
victory_count = victories_df.groupby("driver_number").size().to_dict()

points_by_driver = (
    merged_df.groupby(["driver_number", "driver_name", "headshot_url"])["points"]
    .sum()
    .reset_index()
)

points_by_driver["wins"] = points_by_driver["driver_number"].apply(lambda x: victory_count.get(x, 0))
points_by_driver["win_rate"] = points_by_driver["wins"] / total_gps * 100
points_by_driver["win_rate"] = points_by_driver["win_rate"].round(2)

points_by_driver = points_by_driver.sort_values("points", ascending=False)

# -------------------------
# AFFICHE CHAMPION SI FINI
# -------------------------
if total_gps >= 22 and not points_by_driver.empty:
    champ = points_by_driver.iloc[0]
    st.markdown(f"""
    <div style='text-align:center; padding: 2rem; background-color: rgba(0,0,0,0.7); border-radius: 1rem;'>
        <h1 style='color:gold; font-size:3rem;'>🏆 CHAMPION DU MONDE F1 2023</h1>
        <h2 style='color:white;'>{champ['driver_name']} ({champ['driver_number']})</h2>
        <h3 style='color:white;'>avec {champ['points']} points</h3>
    </div>
    """, unsafe_allow_html=True)
else:
    st.title("🏎️ F1 - Live Classement")

st.markdown(f"**📅 Grands Prix disputés : {total_gps}/22**")

# -------------------------
# CLASSEMENT GÉNÉRAL
# -------------------------
col1, col2 = st.columns([2, 2])

with col1:
    st.subheader("📊 Classement Général")
    for _, row in points_by_driver.iterrows():
        with st.container():
            st.markdown('<div class="transparent-box">', unsafe_allow_html=True)
            c1, c2, c3 = st.columns([1, 5, 2])
            with c1:
                if row["headshot_url"]:
                    st.image(row["headshot_url"], width=50)
            with c2:
                st.markdown(f"**{row['driver_name']}** — {row['points']} pts")
            with c3:
                st.markdown(f"🏁 {row['wins']} victoires<br>🔥 {row['win_rate']}%", unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)

# -------------------------
# DÉTAIL PAR COURSE
# -------------------------
with col2:
    st.subheader("🗓️ Détail par Grand Prix")
    gps = merged_df["grand_prix"].dropna().unique()
    selected_gp = st.selectbox("📍 Sélectionner un Grand Prix", gps[::-1])

    gp_df = merged_df[merged_df["grand_prix"] == selected_gp]
    gp_df = gp_df.sort_values("position")

    for _, row in gp_df.iterrows():
        with st.container():
            st.markdown('<div class="transparent-box">', unsafe_allow_html=True)
            c1, c2 = st.columns([1, 5])
            with c1:
                if row["headshot_url"]:
                    st.image(row["headshot_url"], width=50)
            with c2:
                st.markdown(
                    f"**{row['driver_name']}** — Pos: {row['position']} | "
                    f"Gap: {row['gap_to_leader'] or 'N/A'}"
                )
            st.markdown('</div>', unsafe_allow_html=True)

# -------------------------
# PODIUM
# -------------------------
st.subheader("🥇 Podium")
podium_icons = ["🥇", "🥈", "🥉"]
podium = points_by_driver.head(3)

for i, (_, row) in enumerate(podium.iterrows()):
    icon = podium_icons[i] if i < len(podium_icons) else ""
    st.markdown(f"{icon} **{row['driver_name']}** ({row['driver_number']}) — {row['points']} pts")
    if row["headshot_url"]:
        st.image(row["headshot_url"], width=100)
