import os
from flask import Flask, jsonify
from flask_cors import CORS
import sqlite3

app = Flask(__name__)
CORS(app)

# ✅ YOUR EXACT DATABASE PATH
DB_FILE = r"C:\Users\phadt\Documents\IST440W-main\vehicle_oils.db"


def get_db_connection():
    print("Using DB:", DB_FILE)  # 🔍 debug line
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/api/vehicles", methods=["GET"])
def get_vehicles():
    conn = get_db_connection()
    cursor = conn.cursor()

    rows = cursor.execute("SELECT * FROM vehicle_oils").fetchall()
    conn.close()

    vehicles = {}

    for row in rows:
        key = f"{row['year']}_{row['make']}_{row['model']}_{row['engine']}"

        if key not in vehicles:
            vehicles[key] = {
                "year": row["year"],
                "make": row["make"],
                "model": row["model"],
                "engine": row["engine"],
                "displayName": f"{row['year']} {row['make']} {row['model']}",
                "capacity": {
                    "with_filter": {
                        "quarts": row["capacity_with_filter_quarts"],
                        "liters": row["capacity_with_filter_liters"]
                    }
                },
                "oils": []
            }

        vehicles[key]["oils"].append({
            "oil_type": row["oil_type"],
            "recommendation_level": row["recommendation_level"],
            "temperature": row["temperature"]
        })

    return jsonify(list(vehicles.values()))


if __name__ == "__main__":
    app.run(debug=True)