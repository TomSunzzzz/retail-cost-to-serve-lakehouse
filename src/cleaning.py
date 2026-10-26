"""Reusable cleaning helpers for the Silver layer."""
from pyspark.sql import Column
from pyspark.sql import functions as F

_ACCENTED = "áàâãäéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ"
_PLAIN = "aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN"

# Abbreviations sellers typed instead of a city name.
CITY_ALIASES = {
    "sp": "sao paulo",
    "sao paulo sp": "sao paulo",
    "sbc": "sao bernardo do campo",
    "rj": "rio de janeiro",
    "bh": "belo horizonte",
}

BRAZIL_STATES = (
    "ac|al|ap|am|ba|ce|df|es|go|ma|mt|ms|mg|pa|pb|pr|pe|pi|rj|rn|rs|ro|rr|sc|sp|se|to"
)


def strip_accents(col: Column) -> Column:
    return F.translate(col, _ACCENTED, _PLAIN)


def normalize_city(col: Column) -> Column:
    """Turn free-text city input into one consistent spelling.

    'Auriflama/SP' -> 'auriflama', 'andira-pr' -> 'andira', 'Rio de Janeiro, Rio de Janeiro, Brasil' -> 'rio de janeiro',
    'Teresópolis' -> 'teresopolis', "Santa Barbara d´Oeste" -> "santa barbara d'oeste".
    A value made only of digits (a zip code typed into the city field) becomes null.
    """
    c = F.lower(F.trim(col))
    c = strip_accents(c)
    c = F.regexp_replace(c, "[´`’]", "'")
    # keep only the part before a separator that introduces the state or country
    c = F.split(c, r"\s*/\s*|\s*,\s*|\s+-\s+|\s*\\\s*")[0]
    c = F.regexp_replace(F.trim(c), r"\s+", " ")
    # 'andira-pr': drop a state code glued on with a hyphen. Not with a space:
    # 'sento se' is a real city whose last word happens to be a state code.
    c = F.regexp_replace(c, rf"-({BRAZIL_STATES})$", "")
    mapping = F.create_map(*[F.lit(x) for kv in CITY_ALIASES.items() for x in kv])
    c = F.coalesce(mapping[c], c)
    return F.when(c.rlike(r"^[0-9]+$") | (c == ""), F.lit(None)).otherwise(c)


def to_ts(col_name: str) -> Column:
    return F.to_timestamp(F.col(col_name), "yyyy-MM-dd HH:mm:ss")


def to_money(col_name: str) -> Column:
    return F.col(col_name).cast("decimal(12,2)")
