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
    engine = create_engine("mysql+pymysql://root:PhcvfayhkDYbLZFiHcQYHliOwzDOAJVA@caboose.proxy.rlwy.net:15224/railway")
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
    # 4. DECOMPRESS & LOG SKIP REASONS
    # ---------------------------------------------------
    def decompress_case(blob):
        raw = bytes(blob)
        return zlib.decompress(raw[4:])

    parsed_cases = []

    for row in rows:
        try:
            decompressed = decompress_case(row[2])
            data = json.loads(decompressed.decode("utf-8"))

            lat = data.get("COORDINATE", {}).get("GPS_LAT", None)
            lon = data.get("COORDINATE", {}).get("GPS_LON", None)
            state_id = data.get("QUESTIONS_REC", {}).get("STATE_ID", "")
            lga_id = data.get("QUESTIONS_REC", {}).get("LGA_ID", "")

            # Log every case with its GPS status
            if lat is None or lon is None:
                print(f"  ⚠ Case ID {row[0]} (caseids={row[1]}) — SKIPPED: No GPS coordinates (LAT={lat}, LON={lon})")
            else:
                print(f"  ✓ Case ID {row[0]} (caseids={row[1]}) — OK: LAT={lat}, LON={lon}, STATE={state_id}, LGA={lga_id}")

            parsed_cases.append({
                "db_id": row[0],
                "STATE_ID": state_id,
                "LGA_ID": lga_id,
                "GPS_LAT": lat,
                "GPS_LON": lon,
            })

        except Exception as e:
            print(f"  ✗ Case ID {row[0]} (caseids={row[1]}) — SKIPPED: Decompress/parse error: {e}")

    # ---------------------------------------------------
    # 5. BUILD FEATURES
    # ---------------------------------------------------
    features = []
    for case in parsed_cases:
        lat = case.get("GPS_LAT")
        lon = case.get("GPS_LON")
        if lat is None or lon is None:
            continue
        try:
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
        except Exception as e:
            print(f"  ✗ Case ID {case['db_id']} — SKIPPED: Geometry error: {e}")

    print(f"Features ready to upload: {len(features)}")

    # ---------------------------------------------------
    # 6. UPLOAD
    # ---------------------------------------------------
    if features:
        flayer.delete_features(where="1=1")
        result = flayer.edit_features(adds=features)
        success = sum(1 for r in result.get("addResults", []) if r.get("success"))
        failed = len(result.get("addResults", [])) - success
        print(f"Upload complete — Success: {success}, Failed: {failed}")
    else:
        print("No features with GPS to upload.")

# ---------------------------------------------------
# MAIN LOOP — runs every 60 seconds
# ---------------------------------------------------
def main():
    print("=== CSWeb → ArcGIS Sync Service Started ===")
    while True:
        try:
            print(f"\n--- Running Sync ---")
            sync_to_arcgis()
            print("--- Sync Finished ---")
        except Exception as e:
            print(f"ERROR during sync: {e}")
        print("Waiting 60 seconds...")
        time.sleep(60)

if __name__ == "__main__":
    main()

