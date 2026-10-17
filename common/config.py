import json, os
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]

def load_config(path="config.yaml"):
    p = Path(path)
    if not p.is_absolute():
        p = ROOT / p
    with open(p) as f:
        cfg = yaml.safe_load(f)
    ov = os.environ.get("BDA_OVERRIDES")      # set by run_pipeline --set key=value
    if ov:
        cfg.update(json.loads(ov))
    return cfg

def resolve(path):
    p = Path(path)
    return p if p.is_absolute() else ROOT / p