"""PII masking helpers shared by the ETL transformations and local tests."""


def mask_email(email: object) -> str | None:
    """Keep a small recognizable prefix while hiding most of an email address."""

    if email is None:
        return None
    value = str(email).strip()
    if "@" not in value:
        return "***"
    local, domain = value.split("@", 1)
    visible = local[:2] if len(local) > 2 else local[:1]
    return f"{visible}***@{domain}"


def mask_phone(phone: object) -> str | None:
    """Keep only the first and last two digits of a phone number."""

    if phone is None:
        return None
    digits = "".join(character for character in str(phone) if character.isdigit())
    if len(digits) < 4:
        return "***"
    return f"{digits[:2]}{'*' * (len(digits) - 4)}{digits[-2:]}"
