import os

from dotenv import load_dotenv

load_dotenv()

NA_DEFAULT_DOMAIN = "observeinc.com"
EU_DEFAULT_DOMAIN = "eu-1.observeinc.com"


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(
            f"Missing required environment variable: {name}. "
            "Set it in .env or the process environment."
        )
    return value


def get_credentials(instance: str = "NA") -> dict:
    if instance.upper() == "EU":
        return {
            "customer_id": _require_env("EU_OBSERVE_CUSTOMER_ID"),
            "access_key": _require_env("EU_OBSERVE_ACCESS_KEY"),
            "dataset_id": _require_env("EU_OBSERVE_DATASET_ID"),
            "instance_id": _require_env("EU_OBSERVE_INSTANCE_ID"),
            "domain": os.environ.get("EU_OBSERVE_DOMAIN", EU_DEFAULT_DOMAIN),
        }

    return {
        "customer_id": _require_env("NA_OBSERVE_CUSTOMER_ID"),
        "access_key": _require_env("NA_OBSERVE_ACCESS_KEY"),
        "dataset_id": _require_env("NA_OBSERVE_DATASET_ID"),
        "instance_id": _require_env("NA_OBSERVE_INSTANCE_ID"),
        "domain": os.environ.get("NA_OBSERVE_DOMAIN", NA_DEFAULT_DOMAIN),
    }
