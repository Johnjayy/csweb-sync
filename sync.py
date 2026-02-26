import time
import zlib
import json
from sqlalchemy import create_engine, text
from arcgis.gis import GIS
from arcgis.features import FeatureLayer, Feature
from arcgis.geometry import Point


def sync_to_arcgis():

    # ---------------------------------------------------
    # 1. CONNECT TO ARCGIS ONLINE
    # ---------------------------------------------------
    print("Connecting to ArcGIS Online...")
    gis = GIS("https://www.arcgis.com", "JohnGEO1", "EvLs4777@")
    print("Connected to ArcGIS Online ✓")

    feature_layer_url = "https://services8.arcgis.com/oTalEaSXAuyNT7xf/arcgis/rest/services/NBS/FeatureServer/0"
    flayer = FeatureLayer(feature_layer_url)

    # ---------------------------------------------------
    # 2. CONNECT TO MYSQL
    # ---------------------------------------------------
    print("Connecting to MySQL...")
    engine = create_engine("mysql+pymysql://root:PJJWurMTBNsJEZoKbungZKfUjAxkLStR@tramway.proxy.rlwy.net:17273/csweb")
    print("Connected to MySQL ✓")

    # ---------------------------------------------------
    # 3. READ ACTIVE CASES
    # ---------------------------------------------------
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, caseids, questionnaire, modified_time
            FROM questions_dict
            WHERE deleted = 0
        """)).fetchall()

    print(f"Total active cases found: {len(rows)}")

    # ---------------------------------------------------
    # 4. DECOMPRESS
    # ---------------------------------------------------
    def decompress_case(blob):
        raw = bytes(blob)
        return zlib.decompress(raw[4:])

    parsed_cases = []

    for row in rows:
        try:
            decompressed = decompress_case(row[2])
            data = json.loads(decompressed.decode("utf-8"))

            case = {
                "db_id": row[0],
                "STATE_ID": data.get("QUESTIONS_REC", {}).get("STATE_ID", ""),
                "LGA_ID": data.get("QUESTIONS_REC", {}).get("LGA_ID", ""),
                "GPS_LAT": data.get("COORDINATE", {}).get("GPS_LAT", None),
                "GPS_LON": data.get("COORDINATE", {}).get("GPS_LON", None),
            }

            parsed_cases.append(case)

        except Exception as e:
            print(f"Skipping case {row[0]}: {e}")

    # ---------------------------------------------------
    # 5. BUILD FEATURES
    # ---------------------------------------------------
    features = []

    for case in parsed_cases:
        lat = case.get("GPS_LAT")
        lon = case.get("GPS_LON")

        if lat is None or lon is None:
            continue

        geometry = Point({
            "x": float(lon),
            "y": float(lat),
            "spatialReference": {"wkid": 4326}
        })

        attributes = {
            "state_id": case["STATE_ID"],
            "lga_id": case["LGA_ID"],
        }

        features.append(Feature(geometry=geometry, attributes=attributes))

    print(f"Features ready: {len(features)}")

    # ---------------------------------------------------
    # 6. UPLOAD
    # ---------------------------------------------------
    if features:
        flayer.delete_features(where="1=1")

        result = flayer.edit_features(adds=features)
        print("Upload result:", result)

    else:
        print("No features to upload.")


# ---------------------------------------------------
# LOOP (THIS IS THE NEW PART)
# ---------------------------------------------------

def main():
    while True:
        try:
            print("\n--- Running Sync ---")
            sync_to_arcgis()
            print("--- Sync Finished ---")
        except Exception as e:
            print("ERROR:", e)

        time.sleep(60)  # Wait 60 seconds


if __name__ == "__main__":
    main()