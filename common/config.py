from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]

def load_config(path="config.yaml"):
    p = Path(path)
    if not p.is_absolute():
        p = ROOT / p
    with open(p) as f:
        return yaml.safe_load(f)

def resolve(path):
    p = Path(path)
    return p if p.is_absolute() else ROOT / p