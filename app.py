import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# --------------------------------------------------
# CONFIGURACIÓN GENERAL
# --------------------------------------------------
st.set_page_config(
    page_title="Análisis Pit-Stops F1",
    layout="wide"
)

st.title("Análisis de Pit-Stops en Fórmula 1")
st.markdown(
    """
    Proyecto Final - Adquisición de Datos  
    Grado en Ingeniería Matemática e Inteligencia Artificial
    """
)

# --------------------------------------------------
# CARGA DE DATOS
# --------------------------------------------------
@st.cache_data
def load_data():
    return pd.read_csv("data/final_merged.csv")

df = load_data()

# --------------------------------------------------
# DETECTAR COLUMNA DE POSICIÓN
# --------------------------------------------------
possible_position_cols = ["Position", "Pos", "Pos.", "Finish"]
position_col = None

for col in possible_position_cols:
    if col in df.columns:
        position_col = col
        break

if position_col is None:
    st.error(f"No se encontró columna de posición. Columnas: {list(df.columns)}")
    st.stop()

# --------------------------------------------------
# LIMPIEZA BÁSICA
# --------------------------------------------------
df[position_col] = pd.to_numeric(df[position_col], errors="coerce")
df["NPitstops"] = pd.to_numeric(df["NPitstops"], errors="coerce")
df["Season"] = pd.to_numeric(df["Season"], errors="coerce")
df["MedianPitStopDuration"] = pd.to_numeric(
    df["MedianPitStopDuration"], errors="coerce"
)

df = df.dropna(subset=[position_col, "NPitstops", "Season"])

# --------------------------------------------------
# SIDEBAR – FILTROS Y NAVEGACIÓN
# --------------------------------------------------
st.sidebar.title("Filtros")

section = st.sidebar.radio(
    "Selecciona apartado",
    ["Número de pit-stops", "Duración del pit-stop"]
)

# Temporadas
seasons = sorted(df["Season"].unique())
season_sel = st.sidebar.multiselect(
    "Temporadas",
    seasons,
    default=seasons
)

df = df[df["Season"].isin(season_sel)]

# Pilotos
if "Driver" in df.columns:
    drivers = sorted(df["Driver"].dropna().unique())
    driver_sel = st.sidebar.multiselect(
        "Pilotos (opcional)",
        drivers,
        default=[]
    )
    if driver_sel:
        df = df[df["Driver"].isin(driver_sel)]

# ==================================================
# APARTADO 4.A – NÚMERO DE PIT-STOPS
# ==================================================
if section.startswith("Apartado 4.A"):

    st.header("Número de pit-stops vs posición final")

    df_plot = df[(df[position_col] >= 1) & (df[position_col] <= 20)]
    col1, col2 = st.columns(2)

    # BOXPLOT
    with col1:
        st.subheader("Distribución de posiciones")
        fig1, ax1 = plt.subplots(figsize=(5, 4))
        df_plot.boxplot(column=position_col, by="NPitstops", ax=ax1)
        ax1.set_xlabel("Número de pit-stops")
        ax1.set_ylabel("Posición final")
        ax1.set_title("")
        plt.suptitle("")
        st.pyplot(fig1)

    # BARRAS
    with col2:
        st.subheader("Posición media")
        summary = (
        df_plot.groupby("NPitstops")[position_col]
        .agg(Posición_media="mean", Observaciones="count")
        .reset_index()
        .sort_values("NPitstops")
    )

        fig2, ax2 = plt.subplots(figsize=(5, 4))
        ax2.bar(summary["NPitstops"], summary["Posición_media"])
        ax2.set_xlabel("Número de pit-stops")
        ax2.set_ylabel("Posición media")
        st.pyplot(fig2)

    st.subheader("Resumen numérico")
    st.dataframe(summary, use_container_width=True)

# ==================================================
# APARTADO 4.B – DURACIÓN DEL PIT-STOP
# ==================================================
else:

    st.header("Duración del pit-stop vs posición final")


    df_b = df[
    (df["NPitstops"] > 0) &
    (df["MedianPitStopDuration"].notna()) &
    (df[position_col] >= 1) &
    (df[position_col] <= 20)
    ]

    # SCATTER
    st.subheader("Duración media del pit-stop vs posición final")

    fig_b, ax_b = plt.subplots(figsize=(6, 4))

    x = df_b["MedianPitStopDuration"]
    y = df_b[position_col]

    ax_b.scatter(x, y, alpha=0.4, label="Observaciones")

    # Línea de regresión
    coef = np.polyfit(x, y, 1)
    poly = np.poly1d(coef)

    x_line = np.linspace(x.min(), x.max(), 100)
    y_line = poly(x_line)

    ax_b.plot(
        x_line,
        y_line,
        color="red",
        linewidth=2,
        label="Regresión lineal"
    )

    ax_b.set_xlabel("Duración media del pit-stop (s)")
    ax_b.set_ylabel("Posición final")
    ax_b.legend()

    st.pyplot(fig_b)

    # CORRELACIÓN
    st.subheader("Correlación")

    corr_b = np.corrcoef(
        df_b["MedianPitStopDuration"],
        df_b[position_col]
    )[0, 1]

    st.metric("Coeficiente de correlación (Pearson)", f"{corr_b:.3f}")
