import os
from dotenv import load_dotenv

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(BASE_DIR, "config", ".env"), override=True)

TD_HOST = os.getenv("TD_HOST")
TD_USER = os.getenv("TD_USER")
TD_PASS = os.getenv("TD_PASS")
TD_DB   = os.getenv("TD_DB", "DEMO_USER")
TD_charset=os.getenv("charset")


TD_TRAIN_TABLE = os.getenv("TD_TRAIN_TABLE", "train_data")
TD_EMB_TABLE   = os.getenv("TD_EMB_TABLE", "train_embeddings_fc") 
TD_GPC_TABLE   = os.getenv("TD_GPC_TABLE", "gpc_labels_fc")

TD_TRAIN = f"{TD_DB}.{TD_TRAIN_TABLE}"
TD_EMB   = f"{TD_DB}.{TD_EMB_TABLE}"
TD_GPC   = f"{TD_DB}.{TD_GPC_TABLE}"