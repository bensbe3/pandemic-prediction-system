import streamlit as st
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import numpy as np

# Configuration de la base de données
DB_HOST = "localhost"
DB_NAME = "pandemic_db"
DB_USER = "postgres"
DB_PASS = "postgres"

# Configuration de la page
st.set_page_config(
    page_title="Prédiction des Pandémies",
    page_icon="🦠",
    layout="wide"
)

# Fonction pour se connecter à la base de données (sans cache)
def connect_to_db():
    """Se connecter à la base de données PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données: {e}")
        return None

# Cache la fonction de récupération des données mais pas la connexion
@st.cache_data(ttl=300)
def get_predictions():
    """Récupérer les prédictions depuis la base de données"""
    conn = connect_to_db()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT 
            entity, code, year, reported_deaths, predicted_deaths, created_at,
            COALESCE(model_rmse, 0) as model_rmse, 
            COALESCE(model_r2, 0) as model_r2
        FROM cholera_predictions
        ORDER BY entity, year
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        # Si les colonnes n'existent pas encore, essayer une requête simplifiée
        try:
            simplified_query = """
            SELECT entity, code, year, reported_deaths, predicted_deaths, created_at
            FROM cholera_predictions
            ORDER BY entity, year
            """
            df = pd.read_sql(simplified_query, conn)
            # Ajouter des colonnes fictives pour éviter les erreurs
            df["model_rmse"] = 0
            df["model_r2"] = 0
            conn.close()
            return df
        except Exception as inner_e:
            st.error(f"Erreur lors de la récupération des données: {e} puis {inner_e}")
            if conn:
                conn.close()
            return pd.DataFrame()

def calculate_metrics(df):
    """Calculer des métriques sur les prédictions"""
    if df.empty:
        return {"total_cases": 0, "accuracy": 0, "top_country": "N/A", "top_country_cases": 0}
    
    metrics = {}
    
    # Total des cas
    metrics["total_cases"] = int(df["reported_deaths"].sum())
    
    # Utiliser R² comme métrique de précision si disponible
    if "model_r2" in df.columns and df["model_r2"].mean() > 0:
        r2_mean = df["model_r2"].mean()
        # Convertir R² en pourcentage de précision (R² de 1 = 100% précis)
        metrics["accuracy"] = max(0, min(100, 100 * r2_mean))
    else:
        # Calcul de précision simplifiée
        valid_data = df[(df["reported_deaths"] > 0) & (df["predicted_deaths"] > 0)].copy()
        
        if len(valid_data) > 0:
            valid_data["error"] = abs(valid_data["predicted_deaths"] - valid_data["reported_deaths"])
            valid_data["error_pct"] = valid_data["error"] / valid_data["reported_deaths"]
            mean_error = valid_data["error_pct"].mean()
            metrics["accuracy"] = max(0, min(100, 100 * (1 - mean_error)))
        else:
            metrics["accuracy"] = 0
    
    # Pays le plus touché
    country_totals = df.groupby("entity")["reported_deaths"].sum().sort_values(ascending=False)
    if not country_totals.empty:
        metrics["top_country"] = country_totals.index[0]
        metrics["top_country_cases"] = int(country_totals.iloc[0])
    else:
        metrics["top_country"] = "N/A"
        metrics["top_country_cases"] = 0
    
    return metrics
def main():
    """Application Streamlit principale"""
    st.title("Prédiction des Pandémies - Surveillance du Choléra")
    
    # Sidebar pour les filtres
    st.sidebar.header("Filtres")
    
    # Ajouter un dark mode toggle
    mode = st.sidebar.radio("Thème", ["Clair", "Sombre"])
    if mode == "Sombre":
        st.markdown("""
        <style>
        .reportview-container {
            background-color: #1E1E1E;
            color: white;
        }
        .sidebar .sidebar-content {
            background-color: #2E2E2E;
            color: white;
        }
        h1, h2, h3 {
            color: #FF4B4B;
        }
        </style>
        """, unsafe_allow_html=True)
    
    # Charger les données
    with st.spinner("Chargement des données..."):
        df = get_predictions()
    
    if df.empty:
        st.warning("Aucune donnée de prédiction n'est disponible. Veuillez exécuter le pipeline Spark Streaming.")
        
        # Afficher des instructions pour résoudre les problèmes courants
        st.info("""
        ### Dépannage
        
        Si aucune donnée n'est affichée, vérifiez les points suivants:
        
        1. Assurez-vous que PostgreSQL est en cours d'exécution: `sudo systemctl status postgresql`
        2. Vérifiez que la base de données a été créée: `sudo -u postgres psql -c "\\l"`
        3. Vérifiez les logs Spark pour les erreurs: `cat logs/consumer.log`
        4. Vérifiez les logs du producteur Kafka: `cat logs/producer.log`
        
        Si la base de données n'est pas créée, vous pouvez l'initialiser avec:
        ```
        sudo -u postgres psql -c "CREATE DATABASE pandemic_db;"
        sudo -u postgres psql -d pandemic_db -c "CREATE TABLE IF NOT EXISTS cholera_predictions (
            id SERIAL PRIMARY KEY,
            entity VARCHAR(100) NOT NULL,
            code VARCHAR(10) NOT NULL,
            year INTEGER NOT NULL,
            reported_deaths INTEGER NOT NULL,
            predicted_deaths DOUBLE PRECISION NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );"
        ```
        """)
        return
    
    # Filtres
    all_entities = sorted(df["entity"].unique())
    default_entities = all_entities[:5] if len(all_entities) >= 5 else all_entities
    
    entities = st.sidebar.multiselect(
        "Sélectionner les pays",
        options=all_entities,
        default=default_entities
    )
    
    min_year, max_year = int(df["year"].min()), int(df["year"].max())
    years = st.sidebar.slider(
        "Plage d'années",
        min_value=min_year,
        max_value=max_year,
        value=(min_year, max_year)
    )
    
    # Option avancée pour filtrer les cas
    show_advanced = st.sidebar.checkbox("Options avancées")
    min_deaths = 0
    if show_advanced:
        min_deaths = st.sidebar.number_input(
            "Nombre minimum de décès",
            min_value=0,
            max_value=int(df["reported_deaths"].max()),
            value=0
        )
    
    # Filtrer les données
    filtered_df = df.copy()
    if entities:
        filtered_df = filtered_df[filtered_df["entity"].isin(entities)]
    
    filtered_df = filtered_df[
        (filtered_df["year"] >= years[0]) &
        (filtered_df["year"] <= years[1]) &
        (filtered_df["reported_deaths"] >= min_deaths)
    ]
    
    # Calculer des métriques
    metrics = calculate_metrics(filtered_df)
    
    # Afficher un tableau de bord en haut
    st.subheader("Tableau de bord")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total de cas", f"{metrics['total_cases']:,}")
    
    with col2:
        st.metric("Précision des prédictions", f"{metrics['accuracy']:.1f}%")
    
    with col3:
        st.metric("Pays le plus touché", metrics['top_country'])
    
    with col4:
        st.metric("Nombre de pays analysés", len(filtered_df["entity"].unique()))
    
    # Onglets pour organiser le contenu
    tab1, tab2, tab3 = st.tabs(["Données", "Visualisations", "Analyse de tendance"])
    
    with tab1:
        # Option de téléchargement
        if not filtered_df.empty:
            csv = filtered_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Télécharger les données (CSV)",
                csv,
                "pandemic_predictions.csv",
                "text/csv",
                key='download-csv'
            )
        
        # Afficher les données filtrées
        st.dataframe(filtered_df, use_container_width=True)
    
    with tab2:
        if not filtered_df.empty:
            # Visualisations
            col1, col2 = st.columns(2)
            
            with col1:
                # Graphique des cas réels vs prédits par pays
                fig1 = px.line(
                    filtered_df,
                    x="year",
                    y=["reported_deaths", "predicted_deaths"],
                    color="entity",
                    title="Cas réels vs prédits par pays",
                    labels={"value": "Décès", "variable": "Type", "year": "Année"}
                )
                
                # Améliorer la mise en page
                fig1.update_layout(
                    legend_title_text="Pays",
                    hovermode="x unified",
                    height=500
                )
                
                st.plotly_chart(fig1, use_container_width=True)
            
            with col2:
                # Carte choroplèthe des cas
                fig2 = px.choropleth(
                    filtered_df.groupby("code").agg({"reported_deaths": "sum"}).reset_index(),
                    locations="code",
                    color="reported_deaths",
                    hover_name="code",
                    title="Répartition géographique des cas de choléra",
                    color_continuous_scale=px.colors.sequential.Reds,
                    projection="natural earth"
                )
                
                fig2.update_layout(
                    coloraxis_colorbar=dict(
                        title="Cas rapportés",
                    ),
                    height=500
                )
                
                st.plotly_chart(fig2, use_container_width=True)
            
            # Ajouter un graphique circulaire pour la distribution par pays
            col1, col2 = st.columns(2)
            
            with col1:
                # Top 10 des pays
                top_countries = filtered_df.groupby("entity")["reported_deaths"].sum().nlargest(10)
                fig4 = px.pie(
                    names=top_countries.index,
                    values=top_countries.values,
                    title="Top 10 des pays les plus touchés",
                    hole=0.4
                )
                fig4.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig4, use_container_width=True)
            
            with col2:
                # Précision des prédictions par pays
                if len(filtered_df) > 0:
                    # Calculer l'erreur relative par pays
                    error_by_country = filtered_df[filtered_df["reported_deaths"] > 0].copy()
                    if not error_by_country.empty:
                        error_by_country["error_pct"] = abs(error_by_country["predicted_deaths"] - error_by_country["reported_deaths"]) / error_by_country["reported_deaths"]
                        error_summary = error_by_country.groupby("entity")["error_pct"].mean().reset_index()
                        error_summary["accuracy"] = 100 - (error_summary["error_pct"] * 100)
                        error_summary = error_summary.sort_values("accuracy", ascending=False).head(10)
                        
                        fig5 = px.bar(
                            error_summary,
                            x="entity", 
                            y="accuracy",
                            title="Précision des prédictions par pays (Top 10)",
                            labels={"entity": "Pays", "accuracy": "Précision (%)"}
                        )
                        fig5.update_layout(xaxis_tickangle=-45)
                        st.plotly_chart(fig5, use_container_width=True)
    
    with tab3:
        if not filtered_df.empty:
            # Section d'analyse de tendance
            
            # Grouper par année
            yearly_trend = filtered_df.groupby("year").agg({
                "reported_deaths": "sum",
                "predicted_deaths": "sum"
            }).reset_index()
            
            # Graphique à barres avec des traces de tendance
            fig3 = px.bar(
                yearly_trend,
                x="year",
                y=["reported_deaths", "predicted_deaths"],
                barmode="group",
                title="Tendance annuelle des cas de choléra",
                labels={"value": "Décès", "variable": "Type", "year": "Année"}
            )
            
            # Ajouter des lignes de tendance
            if len(yearly_trend) >= 3:  # Vérifier qu'il y a assez de points pour une moyenne mobile
                # Calculer les moyennes mobiles manuellement pour éviter les erreurs potentielles
                window_size = min(5, len(yearly_trend))
                rolling_real = []
                rolling_predicted = []
                
                for i in range(len(yearly_trend)):
                    start_idx = max(0, i - window_size + 1)
                    real_window = yearly_trend["reported_deaths"].iloc[start_idx:i+1]
                    pred_window = yearly_trend["predicted_deaths"].iloc[start_idx:i+1]
                    rolling_real.append(real_window.mean())
                    rolling_predicted.append(pred_window.mean())
                
                fig3.add_trace(
                    go.Scatter(
                        x=yearly_trend["year"], 
                        y=rolling_real,
                        mode='lines',
                        name='Moyenne mobile - Réel',
                        line=dict(color='red', width=2)
                    )
                )
                
                fig3.add_trace(
                    go.Scatter(
                        x=yearly_trend["year"], 
                        y=rolling_predicted,
                        mode='lines',
                        name='Moyenne mobile - Prédit',
                        line=dict(color='blue', width=2)
                    )
                )
            
            # Améliorer la mise en page
            fig3.update_layout(
                legend_title_text="Type de données",
                hovermode="x unified",
                height=500
            )
            
            st.plotly_chart(fig3, use_container_width=True)
            
            # Analyse saisonnière (si les données couvrent une période suffisante)
            if max_year - min_year >= 10:
                st.subheader("Analyse par décennie")
                
                # Créer des groupes par décennie
                filtered_df['decade'] = (filtered_df['year'] // 10) * 10
                decade_data = filtered_df.groupby('decade').agg({
                    'reported_deaths': 'sum',
                    'entity': lambda x: len(set(x))  # Nombre de pays touchés
                }).reset_index()
                
                decade_data.columns = ['Décennie', 'Cas totaux', 'Pays touchés']
                
                # Graphique à barres des cas par décennie
                fig6 = px.bar(
                    decade_data,
                    x='Décennie',
                    y=['Cas totaux', 'Pays touchés'],
                    barmode='group',
                    title="Évolution par décennie",
                    labels={"value": "Nombre", "variable": "Métrique"}
                )
                
                st.plotly_chart(fig6, use_container_width=True)
    
    # Informations sur la dernière mise à jour
    if not df.empty and 'created_at' in df.columns:
        st.sidebar.info(f"Dernière mise à jour: {df['created_at'].max()}")
    
    # Ajouter un footer
    st.markdown("---")
    st.markdown(
        """
        <div style="text-align: center; opacity: 0.7; font-size: 0.8em;">
        Système de prédiction des pandémies | Développé avec Hadoop, Kafka, Spark et Streamlit
        </div>
        """, 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
