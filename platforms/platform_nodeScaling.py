import argparse
import os
import re

"""
Scale down platform XML radical (node range) by predefined scaling factors.

Example:
python platform_scaling.py
"""

SCALING_FACTORS = [2, 4, 8, 16, 32]
ATTRIBUTE_RE_TEMPLATE = r'({attr}\s*=\s*")([^"]*)(")'


def _validate_scale(value: float) -> None:
    if value <= 0:
        raise ValueError(f"Scaling factor must be > 0. Got: {value}")


def _scale_radical_text(text: str, scale: float) -> str:
    if "-" not in text:
        return text

    start_str, end_str = text.split("-", maxsplit=1)
    start_str = start_str.strip()
    end_str = end_str.strip()

    if not start_str.isdigit() or not end_str.isdigit():
        return text

    start_value = int(start_str)
    end_value = int(end_str)

    scaled_start = max(1, int(start_value / scale))
    scaled_end = max(1, int(end_value / scale))

    if scaled_end < scaled_start:
        scaled_end = scaled_start

    return f"{scaled_start}-{scaled_end}"


def _replace_attribute_value(line: str, attr_name: str, transform) -> str:
    pattern = re.compile(ATTRIBUTE_RE_TEMPLATE.format(attr=re.escape(attr_name)))

    def _replace(match: re.Match) -> str:
        return f"{match.group(1)}{transform(match.group(2))}{match.group(3)}"

    return pattern.sub(_replace, line)


def scale_platform(input_path: str, output_path: str, scaling_factor: float) -> None:
    _validate_scale(scaling_factor)

    with open(input_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    updated_lines = []
    in_cluster_tag = False

    for line in lines:
        updated_line = line

        if "<cluster" in updated_line:
            in_cluster_tag = True

        if in_cluster_tag:
            updated_line = _replace_attribute_value(
                updated_line,
                "radical",
                lambda value: _scale_radical_text(value, scaling_factor),
            )

        if in_cluster_tag and ">" in updated_line:
            in_cluster_tag = False

        updated_lines.append(updated_line)

    with open(output_path, "w", encoding="utf-8") as f:
        f.writelines(updated_lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scale platform XML radical (node range) only."
    )
    parser.add_argument(
        "--input",
        default="platforms/AmSC.xml",
        help="Input platform XML path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    for factor in SCALING_FACTORS:
        output_fname = f"AmSC_scaled_down_{factor}.xml"
        output_path = os.path.join(os.path.dirname(args.input), output_fname)

        scale_platform(
            input_path=args.input,
            output_path=output_path,
            scaling_factor=factor,
        )

        print(f"Scaled platform XML written to: {output_path}")


if __name__ == "__main__":
    main()