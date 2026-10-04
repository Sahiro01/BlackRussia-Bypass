import argparse
import configparser

from core import logger
from relay.relay import BypassRelay


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BRBYPASS relay")

    parser.add_argument(
        "--config",
        default="config.ini",
        help="Путь к config.ini (по умолчанию: config.ini)",
    )

    return parser.parse_args()


def load_config(config_path: str) -> dict:
    import os as _os
    import shutil as _shutil
    if not _os.path.isfile(config_path):
        example = _os.path.join(
            _os.path.dirname(config_path) or ".", "config.example.ini")
        if _os.path.isfile(example):
            _shutil.copyfile(example, config_path)
            print(f"config.ini not found, created from {example} — edit it and restart")
        else:
            print(f"config.ini not found ({config_path}), starting with defaults")
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf-8-sig")

    listen_host = config.get("Bypass", "listen_host", fallback="127.0.0.1")
    listen_port = config.getint("Bypass", "listen_port", fallback=7777)

    br_host = config.get("BlackRussia", "host", fallback="80.66.82.114")
    br_port = config.getint("BlackRussia", "port", fallback=5125)


    password = config.get("Auth", "password", fallback="")
    auth_auto = config.getboolean("Auth", "auth_auto", fallback=True)
    reg_auto = config.getboolean("Auth", "reg_auto", fallback=False)
    email = config.get("Auth", "email", fallback="")
    referral = config.get("Auth", "referral", fallback="")
    gender = config.getint("Auth", "gender", fallback=0)
    skin = config.getint("Auth", "skin", fallback=78)
    if gender not in (0, 1):
        gender = 0

    verbose = config.getboolean("Log", "verbose", fallback=True)

    relay_toggle_controllable = config.getboolean(
        "Relay", "toggle_player_controllable", fallback=True
    )
    damage_ignore = config.getboolean(
        "Relay", "damage_ignore", fallback=False
    )

    return {
        "listen_host": listen_host,
        "listen_port": listen_port,
        "br_host": br_host,
        "br_port": br_port,
        "password": password,
        "auth_auto": auth_auto,
        "reg_auto": reg_auto,
        "email": email,
        "referral": referral,
        "gender": gender,
        "skin": skin,
        "verbose": verbose,
        "relay_toggle_controllable": relay_toggle_controllable,
        "damage_ignore": damage_ignore,
    }


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)

    logger.set_verbose(cfg["verbose"])
    logger.start_async()

    logger.info("=== BRBYPASS: SA-MP <-> Black Russia relay ===")

    relay = BypassRelay(
        listen_host=cfg["listen_host"],
        listen_port=cfg["listen_port"],
        br_host=cfg["br_host"],
        br_port=cfg["br_port"],
        password=cfg["password"],
        auth_auto=cfg["auth_auto"],
        reg_auto=cfg["reg_auto"],
        email=cfg["email"],
        referral=cfg["referral"],
        gender=cfg["gender"],
        skin=cfg["skin"],
        relay_toggle_controllable=cfg["relay_toggle_controllable"],
        damage_ignore=cfg["damage_ignore"],
    )

    try:
        relay.start()
    except KeyboardInterrupt:
        logger.info("Stopping BRBYPASS...")
    finally:
        relay.stop()
        logger.stop_async()


if __name__ == "__main__":
    main()
