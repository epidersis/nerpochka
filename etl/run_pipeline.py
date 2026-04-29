from loaders import load_agreements, load_buau, load_gz, load_rchb
from transformers import build_mart


def main() -> None:
    print("run_pipeline: start")
    load_rchb.run()
    load_gz.run()
    load_agreements.run()
    load_buau.run()
    build_mart.run()
    print("run_pipeline: done")


if __name__ == "__main__":
    main()
