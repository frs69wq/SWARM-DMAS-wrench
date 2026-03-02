import argparse
import os
import re

"""
Command-line utility to scale down platform XML values for radical range, speed, memory_amount_in_gb, and storage_amount_in_gb.

documentation:
- The script reads an input platform XML file (default: platforms/amsc.xml), scales the specified attributes by the provided divisors, and writes the updated XML to an output file (default: AmSC_scaled.xml).
- The scaling is done by dividing the original values by the provided scales. For example, if the radical scale divisor is 8, then a radical range of "10624" would be scaled to "1328".
- The script uses regular expressions to identify and scale numeric values with optional units (e.g., "100GB") and radical ranges.

How to run:
python platform_scaling.py --radical 8 
or 
python platform_scaling.py --radical 8 --speed 2 --memory_amount_in_gb 4 --storage_amount_in_gb 4 --input platforms/amsc.xml --output AmSC_scaled.xml
"""

NUMERIC_WITH_UNIT_RE = re.compile(
	r"^\s*([+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)(\s*[A-Za-z].*)?$"
)

ATTRIBUTE_RE_TEMPLATE = r'({attr}\s*=\s*")([^"]*)(")'


def _validate_scale(name: str, value: float) -> None:
	if value <= 0:
		raise ValueError(f"{name} must be > 0. Got: {value}")


def _format_number(value: float) -> str:
	return f"{value:.12g}"


def _scale_numeric_text(text: str, scale: float) -> str:
	match = NUMERIC_WITH_UNIT_RE.match(text)
	if not match:
		return text

	numeric_part = float(match.group(1))
	unit_part = match.group(2) or ""
	scaled_value = numeric_part / scale
	return f"{_format_number(scaled_value)}{unit_part}"


def _scale_radical_text(text: str, scale: float) -> str:
	if "-" not in text:
		return text

	pieces = text.split("-", maxsplit=1)
	if len(pieces) != 2:
		return text

	start_str, end_str = pieces[0].strip(), pieces[1].strip()
	if not start_str.isdigit() or not end_str.isdigit():
		return text

	start_value = int(start_str)
	end_value = int(end_str)

	scaled_start = int(start_value / scale)
	scaled_end = int(end_value / scale)

	if start_value > 0 and scaled_start == 0:
		scaled_start = 1
	if end_value > 0 and scaled_end == 0:
		scaled_end = 1

	if scaled_end < scaled_start:
		scaled_end = scaled_start

	return f"{scaled_start}-{scaled_end}"


def _replace_attribute_value(line: str, attr_name: str, transform) -> str:
	pattern = re.compile(ATTRIBUTE_RE_TEMPLATE.format(attr=re.escape(attr_name)))

	def _replace(match: re.Match) -> str:
		return f"{match.group(1)}{transform(match.group(2))}{match.group(3)}"

	return pattern.sub(_replace, line)


def _resolve_input_path(preferred_path: str) -> str:
	if os.path.exists(preferred_path):
		return preferred_path

	candidate_paths = [
		"platforms/amsc.xml",
		"platforms/AmSC.xml",
	]
	for path in candidate_paths:
		if os.path.exists(path):
			return path

	raise FileNotFoundError(
		f"Could not find input XML file. Tried: {preferred_path}, {', '.join(candidate_paths)}"
	)


def scale_platform(
	input_path: str,
	output_path: str,
	radical_scale: float,
	speed_scale: float,
	memory_scale: float,
	storage_scale: float,
) -> None:
	_validate_scale("radical", radical_scale)
	_validate_scale("speed", speed_scale)
	_validate_scale("memory_amount_in_gb", memory_scale)
	_validate_scale("storage_amount_in_gb", storage_scale)

	with open(input_path, "r", encoding="utf-8") as input_file:
		lines = input_file.readlines()

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
				lambda value: _scale_radical_text(value, radical_scale),
			)
			updated_line = _replace_attribute_value(
				updated_line,
				"speed",
				lambda value: _scale_numeric_text(value, speed_scale),
			)

		if "<prop" in updated_line and 'id="memory_amount_in_gb"' in updated_line:
			updated_line = _replace_attribute_value(
				updated_line,
				"value",
				lambda value: _scale_numeric_text(value, memory_scale),
			)
		elif "<prop" in updated_line and 'id="storage_amount_in_gb"' in updated_line:
			updated_line = _replace_attribute_value(
				updated_line,
				"value",
				lambda value: _scale_numeric_text(value, storage_scale),
			)

		if in_cluster_tag and ">" in updated_line:
			in_cluster_tag = False

		updated_lines.append(updated_line)

	with open(output_path, "w", encoding="utf-8") as output_file:
		output_file.writelines(updated_lines)


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(
		description=(
			"Scale platform XML values for radical range, speed, memory_amount_in_gb, "
			"and storage_amount_in_gb."
		)
	)
	parser.add_argument("--radical", type=float, required=True, help="Radical scale divisor")
	parser.add_argument(
		"--speed",
		type=float,
		required=False,
		default=1.0,
		help="Speed scale divisor (default: 1.0)",
	)
	parser.add_argument(
		"--memory_amount_in_gb",
		type=float,
		required=False,
		default=1.0,
		help="Memory scale divisor (default: 1.0)",
	)
	parser.add_argument(
		"--storage_amount_in_gb",
		type=float,
		required=False,
		default=1.0,
		help="Storage scale divisor (default: 1.0)",
	)
	parser.add_argument(
		"--input",
		default="platforms/amsc.xml",
		help="Input platform XML path (default: platforms/amsc.xml)",
	)
	parser.add_argument(
		"--output",
		default="AmSC_scaled.xml",
		help="Output platform XML path (default: AmSC_scaled.xml)",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	input_path = _resolve_input_path(args.input)
	# output file in the folder
	output_path = os.path.join(os.path.dirname(input_path), args.output)
	scale_platform(
		input_path=input_path,
		output_path=output_path,
		radical_scale=args.radical,
		speed_scale=args.speed,
		memory_scale=args.memory_amount_in_gb,
		storage_scale=args.storage_amount_in_gb,
	)

	print(f"Scaled platform XML written to: {output_path}")


if __name__ == "__main__":
	main()
