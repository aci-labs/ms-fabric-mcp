from uuid import UUID


def _is_valid_uuid(
    guid: str,
):
    """
    Validates if a string is a valid GUID in version 4

    Parameters
    ----------
    guid : str
        GUID to be validated.

    Returns
    -------
    bool
        Boolean that indicates if the string is a GUID or not.
    """

    try:
        UUID(str(guid), version=4)
        return True
    except ValueError:
        return False
