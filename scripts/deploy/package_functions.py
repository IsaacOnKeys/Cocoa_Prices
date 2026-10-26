"""Prepare self-contained function roots and ZIPs without deploying anything."""

import argparse
from pathlib import Path
from zipfile import ZIP_STORED, ZipFile, ZipInfo

REPO_ROOT = Path(__file__).resolve().parents[2]
FUNCTIONS = {
    "cocoa": ("functions/cocoa", "publish_price"),
    "oil": ("functions/oil", "publish_fred_data"),
    "weather": ("functions/weather", "publish_weather_data"),
}


def package_function(feed, *, repo_root=REPO_ROOT, output_dir=None):
    """Copy only runtime inputs, preserving bytes and fixing ZIP metadata."""
    source_dir, _ = FUNCTIONS[feed]
    sources = {
        "main.py": repo_root / source_dir / "main.py",
        "requirements.txt": repo_root / source_dir / "requirements.txt",
        f"{feed}_schema.avsc": repo_root / "schemas" / f"{feed}_schema.avsc",
    }
    payloads = {name: path.read_bytes() for name, path in sources.items()}
    output_dir = Path(output_dir) if output_dir is not None else repo_root / ".build/functions"
    runtime_root = output_dir / feed
    if runtime_root.exists() and any(
        path.name not in sources or not path.is_file() or path.is_symlink()
        for path in runtime_root.iterdir()
    ):
        raise ValueError("Prepared function root contains unexpected files; use a clean output directory")
    runtime_root.mkdir(parents=True, exist_ok=True)
    for name, payload in payloads.items():
        (runtime_root / name).write_bytes(payload)

    archive = output_dir / f"{feed}.zip"
    with ZipFile(archive, "w", compression=ZIP_STORED) as bundle:
        for name, payload in sorted(payloads.items()):
            info = ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            info.compress_type = ZIP_STORED
            bundle.writestr(info, payload)
    return runtime_root, archive


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("feed", choices=[*FUNCTIONS, "all"], nargs="?", default="all")
    args = parser.parse_args()
    for feed in FUNCTIONS if args.feed == "all" else [args.feed]:
        runtime_root, archive = package_function(feed)
        print(f"{feed}: {runtime_root}\n  archive: {archive}")


if __name__ == "__main__":
    main()
