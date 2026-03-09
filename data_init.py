from __future__ import annotations

import random
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


# Fake data resources

FIRST_NAMES = [
    "Ana", "Luka", "Sara", "Matej", "Nika", "Jan", "Tina", "Rok",
    "Eva", "David", "Maja", "Nejc", "Kaja", "Miha", "Nina", "Žan",
    "Urška", "Marko", "Tjaša", "Aljaž"
]

LAST_NAMES = [
    "Novak", "Kovač", "Horvat", "Zupan", "Potočnik", "Kranjc", "Mlakar",
    "Bizjak", "Pirnat", "Oblak", "Šmit", "Klemenčič", "Vidmar", "Kos",
    "Turk", "Jereb", "Kastelic", "Božič", "Korošec", "Golob"
]

EU_COUNTRIES = {
    "Austria",
    "Belgium",
    "Bulgaria",
    "Croatia",
    "Cyprus",
    "Czech Republic",
    "Denmark",
    "Estonia",
    "Finland",
    "France",
    "Germany",
    "Greece",
    "Hungary",
    "Ireland",
    "Italy",
    "Latvia",
    "Lithuania",
    "Luxembourg",
    "Malta",
    "Netherlands",
    "Poland",
    "Portugal",
    "Romania",
    "Slovakia",
    "Slovenia",
    "Spain",
    "Sweden"
}

COUNTRY_TO_INSTITUTIONS = {
    "Slovenia": [
        "University of Primorska",
        "University of Ljubljana",
        "University of Maribor",
        "University of Nova Gorica",
    ],
    "Croatia": [
        "University of Zagreb",
        "University of Rijeka",
        "University of Split",
    ],
    "Italy": [
        "University of Bologna",
        "Sapienza University of Rome",
        "University of Padua",
    ],
    "Austria": [
        "University of Vienna",
        "Graz University of Technology",
        "University of Innsbruck",
    ],
    "Germany": [
        "University of Cologne",
        "LMU Munich",
        "University of Heidelberg",
    ],
    "Spain": [
        "University of Granada",
        "University of Barcelona",
        "Complutense University of Madrid",
    ],
    "France": [
        "Sorbonne University",
        "University of Lyon",
        "University of Bordeaux",
    ],
    "Ireland": [
        "Trinity College Dublin",
        "University College Dublin",
        "University of Galway",
    ],
    "Poland": [
        "University of Warsaw",
        "Jagiellonian University",
        "University of Gdansk",
    ],
    "Netherlands": [
        "University of Amsterdam",
        "Leiden University",
        "Utrecht University",
    ],
    "Serbia": [
        "University of Belgrade",
        "University of Novi Sad",
    ],
    "Switzerland": [
        "University of Zurich",
        "University of Geneva",
        "EPFL",
    ],
    "USA": [
        "University of Kansas",
        "University of California, Berkeley",
        "Arizona State University",
    ],
    "Bosnia and Herzegovina": [
        "University of Sarajevo",
        "University of Mostar",
    ],
    "Montenegro": [
        "University of Montenegro",
    ],
}

FACULTY_TO_STUDY_PROGRAMMES = {
    "UP FHŠ": ["Psychology", "Sociology", "History", "Geography", "Media Studies"],
    "UP FM": ["Management", "Finance", "Economics", "International Business"],
    "UP FAMNIT": ["Mathematics", "Computer Science", "Biodiversity", "Data Science"],
    "UP FVZ": ["Nursing", "Physiotherapy", "Health Sciences"],
    "UP PEF": ["Education", "Preschool Education", "Special Education"],
    "UP IAM": ["Media Studies", "Cultural Studies"],
    "UP FTS - TURISTICA": ["Tourism", "Sustainable Tourism", "Hospitality Management"],
}


# -----------------------------
# Helper functions
# -----------------------------

def random_date_in_year(year: int) -> date:
    """Return a random date within the given calendar year."""
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def academic_year_from_calendar_year(year: int) -> str:
    """
    For yearly output file YYYY, use academic year YYYY-1/YYYY.
    Example: file 2026 -> academic_year = 2025/2026
    """
    return f"{year - 1}/{year}"


def create_enrollment_number(year: int, file_index: int, row_index: int) -> str:
    """
    Generates a unique-looking enrollment number per file and row.
    Example: 202600100001
    """
    return f"{year}{file_index:02d}{row_index:05d}"


def is_eu_country(country: str) -> str:
    return "EU-27" if country in EU_COUNTRIES else "Non-EU"


def choose_partner_institution(country: str) -> str:
    institutions = COUNTRY_TO_INSTITUTIONS.get(country)
    if institutions:
        return random.choice(institutions)
    return f"{country} Partner Institution"


def choose_study_programme(faculty: str) -> str:
    programmes = FACULTY_TO_STUDY_PROGRAMMES.get(faculty)
    if programmes:
        return random.choice(programmes)
    return "General Studies"


def safe_base_filename(output_dir: Path, base_filename: str | None) -> str:
    return base_filename if base_filename else output_dir.name


def normalize_scalar(value: Any) -> Any:
    """
    Converts simple shorthands:
    - plain list -> random choice
    - plain scalar -> constant
    """
    return value


# -----------------------------
# Generic row generation
# -----------------------------

def generate_value(
    column: str,
    spec: Any,
    row: dict[str, Any],
    *,
    year: int,
    file_index: int,
    row_index: int,
) -> Any:
    """
    Supported spec formats:

    1) List:
       {"faculty": ["UP FHŠ", "UP FM"]}
       -> random choice from list

    2) Constant:
       {"number": 1}
       -> always 1

    3) Dict spec:
       {
           "type": "choice",
           "values": [...]
       }
       {
           "type": "weighted_choice",
           "values": [...],
           "weights": [...]
       }
       {
           "type": "generated",
           "generator": "first_name" | "last_name" | "enrollment_number" |
                        "date" | "graduation_date" | "academic_year" |
                        "year_from" | "year_to" | "eu_noneu" |
                        "partner_institution" | "study_programme"
       }
       {
           "type": "constant",
           "value": ...
       }
    """
    spec = normalize_scalar(spec)

    if isinstance(spec, list):
        return random.choice(spec)

    if not isinstance(spec, dict):
        return spec

    spec_type = spec.get("type", "choice")

    if spec_type == "constant":
        return spec["value"]

    if spec_type == "choice":
        return random.choice(spec["values"])

    if spec_type == "weighted_choice":
        return random.choices(spec["values"], weights=spec["weights"], k=1)[0]

    if spec_type == "generated":
        generator = spec["generator"]

        if generator == "first_name":
            return random.choice(FIRST_NAMES)

        if generator == "last_name":
            return random.choice(LAST_NAMES)

        if generator == "enrollment_number":
            return create_enrollment_number(year, file_index, row_index)

        if generator == "date":
            return random_date_in_year(year)

        if generator == "graduation_date":
            return random_date_in_year(year)

        if generator == "academic_year":
            return academic_year_from_calendar_year(year)

        if generator == "year_from":
            return year

        if generator == "year_to":
            # If mobility is long-term and date is late in the year,
            # allow crossing into next year; otherwise keep same year.
            short_long = row.get("short_long_term")
            generated_date = row.get("date")
            if short_long == "Long-term" and isinstance(generated_date, date) and generated_date.month >= 10:
                return year + 1
            return year

        if generator == "eu_noneu":
            country = row.get("country")
            return is_eu_country(country) if country else "Unknown"

        if generator == "partner_institution":
            country = row.get("country")
            return choose_partner_institution(country) if country else "Unknown Partner Institution"

        if generator == "study_programme":
            faculty = row.get("faculty")
            return choose_study_programme(faculty) if faculty else "General Studies"

        raise ValueError(f"Unknown generator: {generator}")

    raise ValueError(f"Unsupported spec type: {spec_type}")


def generate_records_for_year(
    schema: dict[str, Any],
    records_per_file: int,
    year: int,
    file_index: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for row_index in range(1, records_per_file + 1):
        row: dict[str, Any] = {}

        # Generate in the exact schema order
        for column, spec in schema.items():
            row[column] = generate_value(
                column,
                spec,
                row,
                year=year,
                file_index=file_index,
                row_index=row_index,
            )

        rows.append(row)

    df = pd.DataFrame(rows)

    # Excel-friendly date formatting
    for date_col in ("date", "graduation_date"):
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col]).dt.date

    return df

# Main reusable

def generate_yearly_excel_files(
    schema: dict[str, Any],
    output_path: str | Path,
    records_per_file: int,
    number_of_excel_files: int = 5,
    base_filename: str | None = None,
    seed: int | None = 42,
) -> list[Path]:
    """
    Generate yearly Excel files for the last N years.

    Parameters
    ----------
    schema:
        Dictionary of column_name -> generation spec
    output_path:
        Folder where Excel files will be created
    records_per_file:
        Number of rows in each yearly file
    number_of_excel_files:
        Number of yearly files, default 5
    base_filename:
        Base file name. If None, uses output folder name.
    seed:
        Optional random seed for reproducibility

    Returns
    -------
    List of created file paths
    """
    if seed is not None:
        random.seed(seed)

    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    base = safe_base_filename(output_dir, base_filename)
    current_year = date.today().year
    start_year = current_year - number_of_excel_files + 1
    years = list(range(start_year, current_year + 1))

    created_files: list[Path] = []

    for file_index, year in enumerate(years, start=1):
        df = generate_records_for_year(
            schema=schema,
            records_per_file=records_per_file,
            year=year,
            file_index=file_index,
        )

        file_path = output_dir / f"{base}_{year}.xlsx"
        df.to_excel(file_path, index=False)
        created_files.append(file_path)

    return created_files


# Example schemas

MOBILITY_SCHEMA: dict[str, Any] = {
    "enrollment_number": {"type": "generated", "generator": "enrollment_number"},
    "faculty": [
        "UP FHŠ",
        "UP FM",
        "UP FAMNIT",
        "UP FVZ",
        "UP PEF",
        "UP IAM",
        "UP FTS - TURISTICA",
    ],
    "last_name": {"type": "generated", "generator": "last_name"},
    "first_name": {"type": "generated", "generator": "first_name"},
    "number": {"type": "constant", "value": 1},
    "gender": ["F", "M"],
    "status": {"type": "constant", "value": "student"},
    "employee_category": {"type": "constant", "value": ""},
    "mobility_direction": ["incoming", "outgoing"],
    "mobility_programme": [
        "Erasmus+ KA131",
        "Erasmus+ KA171",
        "Erasmus+ KA2",
        "CEEPUS",
        "Interstate agreement",
        "COST",
        "ARIS bilateral agreement",
        "Marie Curie & ARIS scheme",
        "Freemover",
        "Inter-university agreement",
        "Fulbright",
        "T4EU",
        "Other projects",
        "Other",
    ],
    "mobility_type": [
        "Study",
        "Traineeship",
        "Teaching",
        "Training",
        "Conference",
        "Meeting",
        "Research",
        "Research and teaching",
        "Other",
    ],
    "mobility_form": ["Summer school", "BIP / KIP", ""],
    "country": [
        "Slovenia",
        "Croatia",
        "Italy",
        "Austria",
        "Germany",
        "Spain",
        "France",
        "Ireland",
        "Poland",
        "Netherlands",
        "Serbia",
        "Switzerland",
        "USA",
        "Bosnia and Herzegovina",
        "Montenegro",
    ],
    "eu_noneu": {"type": "generated", "generator": "eu_noneu"},
    "partner_institution": {"type": "generated", "generator": "partner_institution"},
    "study_programme": {"type": "generated", "generator": "study_programme"},
    "degree_level": ["Bachelor", "Master", "PhD"],
    "full_parttime": ["Full-time", "Part-time"],
    "academic_year": {"type": "generated", "generator": "academic_year"},
    "year_from": {"type": "generated", "generator": "year_from"},
    "date": {"type": "generated", "generator": "date"},
    "short_long_term": ["Short-term", "Long-term"],
    "year_to": {"type": "generated", "generator": "year_to"},
}

GRADUATES_SCHEMA: dict[str, Any] = {
    "enrollment_number": {"type": "generated", "generator": "enrollment_number"},
    "last_name": {"type": "generated", "generator": "last_name"},
    "first_name": {"type": "generated", "generator": "first_name"},
    "study_type": [
        "Bachelor programme",
        "Master programme",
        "Doctoral programme",
    ],
    "faculty": [
        "UP FHŠ",
        "UP FM",
        "UP FAMNIT",
        "UP FVZ",
        "UP PEF",
        "UP IAM",
        "UP FTS - TURISTICA",
    ],
    "graduation_date": {"type": "generated", "generator": "graduation_date"},
}


# Example usage

if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / "data"

    mobility_dir = data_dir / "mobility"
    graduates_dir = data_dir / "graduates"

    created_mobility = generate_yearly_excel_files(
        schema=MOBILITY_SCHEMA,
        output_path=mobility_dir,
        records_per_file=200,
        number_of_excel_files=5,
        base_filename=None,   # defaults to folder name, so: mobility_YYYY.xlsx
        seed=42,
    )

    created_graduates = generate_yearly_excel_files(
        schema=GRADUATES_SCHEMA,
        output_path=graduates_dir,
        records_per_file=150,
        number_of_excel_files=5,
        base_filename=None,   # defaults to folder name, so: graduates_YYYY.xlsx
        seed=99,
    )

    print("Created mobility files:")
    for path in created_mobility:
        print(f"  - {path}")

    print("\nCreated graduates files:")
    for path in created_graduates:
        print(f"  - {path}")