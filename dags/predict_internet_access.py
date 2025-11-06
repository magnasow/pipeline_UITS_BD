import pandas as pd
from sklearn.linear_model import LinearRegression
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def predict_and_insert():
    # 1️⃣ Charger les données CSV
    data = pd.read_csv("/opt/airflow/dags/taux_acces_internet.csv")
    
    # 2️⃣ Préparation des données
    data['Time'] = pd.to_datetime(data['Time'])
    data['year'] = data['Time'].dt.year
    
    # 3️⃣ Choix du pays
    country = "Niger"
    X = data[['year']]
    y = data[country]
    
    # 4️⃣ Entraînement du modèle
    model = LinearRegression()
    model.fit(X, y)
    
    # 5️⃣ Prédictions futures
    future_years = pd.DataFrame({"year": [2026, 2027, 2028, 2029, 2030]})
    future_predictions = model.predict(future_years)
    
    # 6️⃣ Création du DataFrame résultat avec colonne date_annee
    results = pd.DataFrame({
        "pays": country,
        "annee": future_years["year"],
        "date_annee": pd.to_datetime(future_years["year"], format="%Y"), 
        "valeur_predite": future_predictions
    })
    
    # 7️⃣ Connexion PostgreSQL via Airflow Hook
    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()
    
    # 8️⃣ Insertion ou mise à jour
    for _, row in results.iterrows():
        cur.execute("""
            INSERT INTO predictions_taux_internet (pays, annee, date_annee, valeur_predite, date_prediction)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (pays, annee)
            DO UPDATE SET
                valeur_predite = EXCLUDED.valeur_predite,
                date_annee = EXCLUDED.date_annee,
                date_prediction = EXCLUDED.date_prediction;
        """, (
            row["pays"], 
            int(row["annee"]), 
            row["date_annee"],   # en DATE
            float(row["valeur_predite"]), 
            datetime.now()
        ))
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Prédictions insérées ou mises à jour avec succès.")
