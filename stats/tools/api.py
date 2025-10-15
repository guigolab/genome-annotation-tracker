import requests
import os

API_URL = os.getenv("API_URL", "http://localhost:94/annotrieve/api/v0")
AUTH_KEY = os.getenv("AUTH_KEY")

def get_annotations_without_stats():
    """
    Fetch all the existing annotations without stats from the api
    """
    annotations = requests.get(f"{API_URL}/annotations?has_stats=false&limit=20000")
    return annotations.json().get("results", [])

def update_annotation(annotation_id: str, stats: dict):
    """
    Update the annotation with the stats
    """
    payload = {
        "features_statistics": stats,
        "auth_key": AUTH_KEY
    }
    response = requests.put(f"{API_URL}/annotations/{annotation_id}/stats", json=payload)
    if response.status_code != 200:
        raise Exception(f"Failed to update the annotation with the stats: {response.text}")