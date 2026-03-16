from __future__ import annotations

import random
from datetime import date
from pathlib import Path

import pandas as pd


FIRST_NAMES = [
    "Ana", "Luka", "Sara", "Matej", "Nika", "Jan", "Tina", "Rok",
    "Eva", "David", "Maja", "Nejc", "Kaja", "Miha", "Nina", "Zan",
    "Urska", "Marko", "Tjasa", "Aljaz",
]

LAST_NAMES = [
    "Novak", "Kovac", "Horvat", "Zupan", "Potocnik", "Kranjc", "Mlakar",
    "Bizjak", "Pirnat", "Oblak", "Smit", "Klemencic", "Vidmar", "Kos",
    "Turk", "Jereb", "Kastelic", "Bozic", "Korosec", "Golob",
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
    "Sweden",
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
    "UP FHS": ["Psychology", "Sociology", "History", "Geography", "Media Studies"],
    "UP FM": ["Management", "Finance", "Economics", "International Business"],
    "UP FAMNIT": ["Mathematics", "Computer Science", "Biodiversity", "Data Science"],
    "UP FVZ": ["Nursing", "Physiotherapy", "Health Sciences"],
    "UP PEF": ["Education", "Preschool Education", "Special Education"],
    "UP IAM": ["Media Studies", "Cultural Studies"],
    "UP FTS - TURISTICA": ["Tourism", "Sustainable Tourism", "Hospitality Management"],
}

FACULTIES = list(FACULTY_TO_STUDY_PROGRAMMES.keys())

MOBILITY_PROGRAMMES = [
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
]

MOBILITY_TYPES = [
    "Study",
    "Traineeship",
    "Teaching",
    "Training",
    "Conference",
    "Meeting",
    "Research",
    "Research and teaching",
    "Other",
]

COUNTRIES = list(COUNTRY_TO_INSTITUTIONS.keys())

DEGREE_LEVELS = ["Bachelor", "Master", "PhD"]
STUDY_TYPES = ["University", "Professional"]
FULL_PARTTIME = ["Full-time", "Part-time"]
SHORT_LONG_TERM = ["Short-term", "Long-term"]
GENERALIZED_DEGREE_LEVELS = ["Undergraduate", "Graduate"]

MOBILITY_PARTICIPATION_RATE = 0.30


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


def create_enrollment_number(graduation_year: int, student_index: int) -> str:
    return f"S{graduation_year}{student_index:05d}"


def random_date_in_year(year: int, month_start: int = 1, month_end: int = 12) -> date:
    month = random.randint(month_start, month_end)
    day = random.randint(1, 28)
    return date(year, month, day)


def academic_year_from_calendar_year(year: int) -> str:
    return f"{year - 1}/{year}"


def choose_degree_level() -> str:
    return random.choices(
        DEGREE_LEVELS,
        weights=[0.65, 0.28, 0.07],
        k=1,
    )[0]


def raw_degree_from_generalized(generalized_degree_level: str) -> str:
    if generalized_degree_level == "Undergraduate":
        return "Bachelor"

    return random.choices(
        ["Master", "PhD"],
        weights=[0.85, 0.15],
        k=1,
    )[0]


def choose_study_type(degree_level: str) -> str:
    if degree_level == "Bachelor":
        return random.choices(STUDY_TYPES, weights=[0.6, 0.4], k=1)[0]
    return "University"


def cohort_mobility_participants(student_ids: list[str]) -> set[str]:
    participant_count = round(len(student_ids) * MOBILITY_PARTICIPATION_RATE)
    return set(random.sample(student_ids, participant_count))


def distributed_counts(total: int, bucket_count: int) -> list[int]:
    base_count = total // bucket_count
    remainder = total % bucket_count

    return [
        base_count + (1 if bucket_index < remainder else 0)
        for bucket_index in range(bucket_count)
    ]


def build_student_profiles(
    graduation_years: list[int],
    graduates_per_year: int,
) -> list[dict[str, object]]:
    profiles: list[dict[str, object]] = []
    qi_buckets = [
        (faculty, generalized_degree_level)
        for faculty in FACULTIES
        for generalized_degree_level in GENERALIZED_DEGREE_LEVELS
    ]

    for graduation_year in graduation_years:
        students_per_bucket = distributed_counts(graduates_per_year, len(qi_buckets))
        student_index = 1

        for (faculty, generalized_degree_level), bucket_size in zip(qi_buckets, students_per_bucket):
            for _ in range(bucket_size):
                degree_level = raw_degree_from_generalized(generalized_degree_level)
                enrollment_number = create_enrollment_number(graduation_year, student_index)
                student_index += 1

                profiles.append(
                    {
                        "enrollment_number": enrollment_number,
                        "graduation_year": graduation_year,
                        "last_name": random.choice(LAST_NAMES),
                        "first_name": random.choice(FIRST_NAMES),
                        "gender": random.choice(["F", "M"]),
                        "faculty": faculty,
                        "study_programme": choose_study_programme(faculty),
                        "degree_level": degree_level,
                        "generalized_degree_level": generalized_degree_level,
                        "study_type": choose_study_type(degree_level),
                        "full_parttime": random.choices(FULL_PARTTIME, weights=[0.85, 0.15], k=1)[0],
                    }
                )

    return profiles


def build_graduates_dataframe(
    profiles: list[dict[str, object]],
    graduation_year: int,
) -> pd.DataFrame:
    rows = []

    for profile in profiles:
        if profile["graduation_year"] != graduation_year:
            continue

        rows.append(
            {
                "enrollment_number": profile["enrollment_number"],
                "last_name": profile["last_name"],
                "first_name": profile["first_name"],
                "study_type": profile["study_type"],
                "faculty": profile["faculty"],
                "degree_level": profile["degree_level"],
                "graduation_date": random_date_in_year(graduation_year, month_start=6, month_end=9),
            }
        )

    return pd.DataFrame(rows)


def select_mobility_years(graduation_year: int, data_years: list[int]) -> list[int]:
    earliest_year = max(min(data_years), graduation_year - 4)
    return [year for year in data_years if earliest_year <= year <= graduation_year]


def build_mobility_rows_for_profile(
    profile: dict[str, object],
    data_years: list[int],
    has_mobility: bool,
) -> list[dict[str, object]]:
    if not has_mobility:
        return []

    graduation_year = int(profile["graduation_year"])
    possible_years = select_mobility_years(graduation_year, data_years)
    if not possible_years:
        return []

    event_count = random.choices([1, 2], weights=[0.8, 0.2], k=1)[0]
    sampled_years = sorted(random.sample(possible_years, k=min(event_count, len(possible_years))))

    rows: list[dict[str, object]] = []
    for event_year in sampled_years:
        country = random.choice(COUNTRIES)
        short_long_term = random.choices(SHORT_LONG_TERM, weights=[0.7, 0.3], k=1)[0]
        event_date = random_date_in_year(event_year)
        year_to = event_year
        if short_long_term == "Long-term" and event_date.month >= 10:
            year_to = min(event_year + 1, graduation_year)

        rows.append(
            {
                "enrollment_number": profile["enrollment_number"],
                "faculty": profile["faculty"],
                "last_name": profile["last_name"],
                "first_name": profile["first_name"],
                "number": 1,
                "gender": profile["gender"],
                "status": "student",
                "employee_category": "",
                "mobility_direction": random.choices(["incoming", "outgoing"], weights=[0.15, 0.85], k=1)[0],
                "mobility_programme": random.choice(MOBILITY_PROGRAMMES),
                "mobility_type": random.choice(MOBILITY_TYPES),
                "mobility_form": random.choices(["Summer school", "BIP / KIP", ""], weights=[0.2, 0.25, 0.55], k=1)[0],
                "country": country,
                "eu_noneu": is_eu_country(country),
                "partner_institution": choose_partner_institution(country),
                "study_programme": profile["study_programme"],
                "degree_level": profile["degree_level"],
                "full_parttime": profile["full_parttime"],
                "academic_year": academic_year_from_calendar_year(event_year),
                "year_from": event_year,
                "date": event_date,
                "short_long_term": short_long_term,
                "year_to": year_to,
            }
        )

    return rows


def build_mobility_dataframes(
    profiles: list[dict[str, object]],
    data_years: list[int],
) -> dict[int, pd.DataFrame]:
    mobility_rows_by_year = {year: [] for year in data_years}

    profiles_by_cohort: dict[tuple[int, str, str], list[dict[str, object]]] = {}
    for profile in profiles:
        key = (
            int(profile["graduation_year"]),
            str(profile["faculty"]),
            str(profile["generalized_degree_level"]),
        )
        profiles_by_cohort.setdefault(key, []).append(profile)

    for _, cohort_profiles in profiles_by_cohort.items():
        cohort_student_ids = [str(profile["enrollment_number"]) for profile in cohort_profiles]
        participant_count = round(len(cohort_student_ids) * MOBILITY_PARTICIPATION_RATE)
        participant_count = max(1, min(len(cohort_student_ids) - 1, participant_count))
        participant_ids = set(random.sample(cohort_student_ids, participant_count))

        for profile in cohort_profiles:
            rows = build_mobility_rows_for_profile(
                profile,
                data_years,
                has_mobility=str(profile["enrollment_number"]) in participant_ids,
            )
            for row in rows:
                mobility_rows_by_year[int(row["year_from"])].append(row)

    return {
        year: pd.DataFrame(rows)
        for year, rows in mobility_rows_by_year.items()
    }


def write_yearly_excel_files(
    yearly_dataframes: dict[int, pd.DataFrame],
    output_dir: Path,
    base_filename: str,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    created_files: list[Path] = []

    for year, df in sorted(yearly_dataframes.items()):
        file_path = output_dir / f"{base_filename}_{year}.xlsx"
        df.to_excel(file_path, index=False)
        created_files.append(file_path)

    return created_files


def generate_yearly_excel_files(
    output_path: str | Path,
    records_per_file: int,
    number_of_excel_files: int = 5,
    base_filename: str | None = None,
    seed: int | None = 42,
) -> tuple[list[Path], list[Path]]:
    if seed is not None:
        random.seed(seed)

    output_root = Path(output_path)
    output_root.mkdir(parents=True, exist_ok=True)

    current_year = date.today().year
    start_year = current_year - number_of_excel_files + 1
    data_years = list(range(start_year, current_year + 1))

    profiles = build_student_profiles(
        graduation_years=data_years,
        graduates_per_year=records_per_file,
    )

    graduates_output_dir = output_root / "graduates"
    mobility_output_dir = output_root / "mobility"

    graduates_yearly = {
        year: build_graduates_dataframe(profiles, year)
        for year in data_years
    }
    mobility_yearly = build_mobility_dataframes(profiles, data_years)

    graduates_files = write_yearly_excel_files(
        graduates_yearly,
        graduates_output_dir,
        base_filename="graduates" if base_filename is None else f"{base_filename}_graduates",
    )
    mobility_files = write_yearly_excel_files(
        mobility_yearly,
        mobility_output_dir,
        base_filename="mobility" if base_filename is None else f"{base_filename}_mobility",
    )

    return mobility_files, graduates_files


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent
    data_dir = base_dir / "data"

    created_mobility, created_graduates = generate_yearly_excel_files(
        output_path=data_dir,
        records_per_file=150,
        number_of_excel_files=5,
        base_filename=None,
        seed=42,
    )

    print("Created mobility files:")
    for path in created_mobility:
        print(f"  - {path}")

    print("\nCreated graduates files:")
    for path in created_graduates:
        print(f"  - {path}")
