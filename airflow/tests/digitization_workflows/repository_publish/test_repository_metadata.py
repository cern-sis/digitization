import pytest

from digitization_workflows.repository_publish import metadata
from digitization_workflows.repository_publish.metadata import MissingDateError

ATOM_URL = "https://archives.cern.ch/index.php/council-minutes-1954"


def description(**fields):
    return {
        "title": "Council minutes",
        "reference_code": "CERN-ARCH-CM-1954",
        "dates": [{"start_date": "1954-01-01", "end_date": "1954-12-31"}],
        **fields,
    }


@pytest.mark.parametrize(
    "dates, expected",
    [
        (
            [{"start_date": "1954-01-01", "end_date": "1960-12-31"}],
            "1954-01-01/1960-12-31",
        ),
        ([{"start_date": "1954-03-02", "end_date": "1954-03-02"}], "1954-03-02"),
        ([{"start_date": "1954-00-00", "end_date": "1960-00-00"}], "1954/1960"),
        (
            [{"start_date": "1960-01-01", "end_date": "1954-01-01"}],
            "1954-01-01/1960-01-01",
        ),
        ([{"start_date": "1954-05-00"}], "1954-05"),
        ([{"date": "ca. 1954-1958"}], "1954/1958"),
        ([{"date": "1954"}], "1954"),
        ([{"start_date": "1954-13-01"}], "1954"),
        ([{"start_date": "1954-02-31"}], "1954"),
        ([{"start_date": "1954-02-28"}], "1954-02-28"),
        (
            [
                {"date": "1990", "type": "Accumulation"},
                {"date": "1954", "type": "Creation"},
            ],
            "1954",
        ),
    ],
)
def test_publication_date_from_atom_dates(dates, expected):
    assert metadata.publication_date(dates) == expected


@pytest.mark.parametrize(
    "dates", [None, [], [{"date": "undated"}], [{"start_date": "0000-00-00"}]]
)
def test_publication_date_missing_raises(dates):
    with pytest.raises(MissingDateError):
        metadata.publication_date(dates)


def test_atom_to_rdm_maps_the_description():
    record = metadata.atom_to_rdm(
        description(
            creators=[
                {"authorized_form_of_name": "CERN Council"},
                {"authorized_form_of_name": "CERN Council"},
                {"authorized_form_of_name": ""},
            ],
            scope_and_content="Minutes of the sessions.",
        ),
        slug="council-minutes-1954",
        atom_url=ATOM_URL,
        resource_type="publication-meetingminutes",
    )

    assert record["access"] == {"record": "public", "files": "public"}
    assert record["files"] == {"enabled": True}
    assert record["metadata"] == {
        "resource_type": {"id": "publication-meetingminutes"},
        "title": "Council minutes",
        "publication_date": "1954-01-01/1954-12-31",
        "creators": [
            {"person_or_org": {"type": "organizational", "name": "CERN Council"}}
        ],
        "publisher": "CERN",
        "description": "Reference code: CERN-ARCH-CM-1954<br>Minutes of the sessions.",
        "related_identifiers": [
            {
                "identifier": ATOM_URL,
                "scheme": "url",
                "relation_type": {"id": "isdescribedby"},
                "resource_type": {"id": "other"},
            }
        ],
    }


def test_atom_to_rdm_defaults():
    record = metadata.atom_to_rdm(
        description(title="", reference_code=None),
        slug="council-minutes-1954",
        atom_url=ATOM_URL,
    )

    assert record["metadata"]["title"] == "council-minutes-1954"
    assert record["metadata"]["resource_type"] == {"id": "publication"}
    assert record["metadata"]["creators"] == [
        {"person_or_org": {"type": "organizational", "name": "CERN"}}
    ]
    assert "description" not in record["metadata"]


def test_atom_to_rdm_does_not_mutate_the_description():
    original = description()
    snapshot = {**original, "dates": [dict(d) for d in original["dates"]]}

    metadata.atom_to_rdm(original, slug="s", atom_url=ATOM_URL)

    assert original == snapshot


def test_description_is_escaped_html():
    record = metadata.atom_to_rdm(
        description(reference_code="A&B", scope_and_content="<b>x</b>\r\nline 2"),
        slug="s",
        atom_url=ATOM_URL,
    )

    assert record["metadata"]["description"] == (
        "Reference code: A&amp;B<br>&lt;b&gt;x&lt;/b&gt;<br>line 2"
    )


ARCHIVE_URL = "https://archives.cern.ch/index.php/cern-arch-test-002"


def test_a_cern_arch_slug_is_linked_with_the_cern_archives_scheme():
    """CDS shows it as "CERN Archives" and links it to the AtoM URL of the slug."""
    record = metadata.atom_to_rdm(
        description(), slug="cern-arch-test-002", atom_url=ARCHIVE_URL
    )

    assert record["metadata"]["related_identifiers"] == [
        {
            "identifier": "CERN-ARCH-TEST-002",
            "scheme": "archive",
            "relation_type": {"id": "isdescribedby"},
            "resource_type": {"id": "other"},
        }
    ]


@pytest.mark.parametrize(
    "slug, expected",
    [
        ("cern-arch-test-002", "CERN-ARCH-TEST-002"),
        ("CERN-ARCH-CM-1954-01", "CERN-ARCH-CM-1954-01"),
        ("council-minutes-1954", None),
        ("cern-arch", None),
        ("cern-arch-a_b", None),
    ],
)
def test_archive_identifier(slug, expected):
    assert metadata.archive_identifier(slug) == expected


def test_description_identifiers_include_the_url_of_older_records():
    """Records created before the CERN Archives scheme have the AtoM URL."""
    assert metadata.description_identifiers("cern-arch-test-002", ARCHIVE_URL) == [
        "CERN-ARCH-TEST-002",
        ARCHIVE_URL,
    ]
    assert metadata.description_identifiers("council-minutes-1954", ATOM_URL) == [
        ATOM_URL
    ]


def test_merge_metadata_replaces_the_old_atom_url_with_the_archive_identifier():
    current = {
        "related_identifiers": [related("https://cds.cern.ch/1"), related(ARCHIVE_URL)]
    }
    archive = {"identifier": "CERN-ARCH-TEST-002", "scheme": "archive"}

    merged = metadata.merge_metadata(
        current,
        {"related_identifiers": [archive]},
        replaced=["CERN-ARCH-TEST-002", ARCHIVE_URL],
    )

    assert merged["related_identifiers"] == [related("https://cds.cern.ch/1"), archive]


def test_merge_metadata_replaces_an_identifier_typed_in_another_case():
    archive = {"identifier": "CERN-ARCH-TEST-002", "scheme": "archive"}
    typed = {"identifier": "cern-arch-test-002", "scheme": "archive"}

    merged = metadata.merge_metadata(
        {"related_identifiers": [typed]}, {"related_identifiers": [archive]}
    )

    assert merged["related_identifiers"] == [archive]


def related(identifier):
    return {"identifier": identifier, "scheme": "url"}


def test_merge_metadata_replaces_the_mapped_fields_only():
    current = {
        "title": "Old title",
        "subjects": [{"subject": "CERN Archives"}],
        "languages": [{"id": "eng"}],
        "related_identifiers": [related("https://cds.cern.ch/1"), related(ATOM_URL)],
    }
    mapped = {"title": "New title", "related_identifiers": [related(ATOM_URL)]}

    merged = metadata.merge_metadata(current, mapped)

    assert merged["title"] == "New title"
    assert merged["subjects"] == [{"subject": "CERN Archives"}]
    assert merged["languages"] == [{"id": "eng"}]
    assert merged["related_identifiers"] == [
        related("https://cds.cern.ch/1"),
        related(ATOM_URL),
    ]


def test_merge_metadata_adds_a_missing_atom_url():
    merged = metadata.merge_metadata({}, {"related_identifiers": [related(ATOM_URL)]})

    assert merged["related_identifiers"] == [related(ATOM_URL)]


def test_merge_metadata_does_not_mutate_its_inputs():
    current = {"related_identifiers": [related("a")]}
    mapped = {"related_identifiers": [related("b")]}

    metadata.merge_metadata(current, mapped)

    assert current == {"related_identifiers": [related("a")]}
    assert mapped == {"related_identifiers": [related("b")]}
