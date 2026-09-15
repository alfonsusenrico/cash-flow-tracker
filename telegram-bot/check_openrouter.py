import requests

def main():
    resp = requests.get("https://openrouter.ai/api/v1/models")
    if resp.status_code != 200:
        print("Failed to fetch models")
        return
    data = resp.json()
    for model in data.get("data", []):
        if "deepseek" in model["id"].lower():
            print(f"Model ID: {model['id']}")
            print(f"  Architecture: {model.get('architecture', {})}")
            print(f"  Top Provider: {model.get('top_provider', {})}")

if __name__ == "__main__":
    main()
