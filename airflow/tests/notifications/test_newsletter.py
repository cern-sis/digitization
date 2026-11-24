from datetime import date

import pytest

from notifications import config, newsletter

START, END = date(2026, 11, 2), date(2026, 11, 9)


def doc(pid, title="Title", subjects=()):
    return {
        "pid": pid,
        "url": f"https://catalogue.library.cern/literature/{pid}",
        "title": title,
        "subjects": [{"value": value, "scheme": scheme} for value, scheme in subjects],
    }


@pytest.mark.parametrize(
    "subjects, expected",
    [
        ([("530.145(07)", "UDC")], ["Physics"]),
        ([("53(07)", "UDC")], ["Physics"]),
        ([("510.6", "DEWEY")], ["Mathematics"]),
        ([("005.1", "UDC")], ["Administration/Management"]),
        ([("005.1", "DEWEY")], ["Information Technology"]),
        ([("004.8", "UDC"), ("53", "UDC")], ["Information Technology", "Physics"]),
        ([("94", "UDC")], [config.OTHER_TOPIC]),
        ([], [config.OTHER_TOPIC]),
    ],
)
def test_classify(subjects, expected):
    assert newsletter.classify(doc("p", subjects=subjects)) == expected


def test_classify_scheme_must_match_when_rule_has_one():
    # 005* is only Administration/Management for UDC and only IT for DEWEY.
    assert newsletter.classify(doc("p", subjects=[("005", "LCC")])) == [
        config.OTHER_TOPIC
    ]


def test_group_by_topic_orders_topics_dedupes_and_sorts_titles():
    documents = [
        doc("p2", "b physics", [("53", "UDC")]),
        doc("p1", "A physics and IT", [("53", "UDC"), ("004", "UDC")]),
        doc("p1", "A physics and IT", [("53", "UDC"), ("004", "UDC")]),
        doc("p3", "History", [("94", "UDC")]),
        doc("p4", "Two physics subjects", [("53", "UDC"), ("530", "UDC")]),
    ]

    groups = newsletter.group_by_topic(documents)

    assert list(groups) == ["Information Technology", "Physics", config.OTHER_TOPIC]
    assert [d["pid"] for d in groups["Physics"]] == ["p1", "p2", "p4"]
    assert [d["pid"] for d in groups["Information Technology"]] == ["p1"]
    assert [d["pid"] for d in groups[config.OTHER_TOPIC]] == ["p3"]


def test_group_by_topic_empty():
    assert newsletter.group_by_topic([]) == {}


def test_build_subject():
    assert newsletter.build_subject(START, END) == (
        "Latest books/e-books (02 Nov 2026 – 08 Nov 2026)"
    )


def test_render_html_lists_links_and_escapes():
    groups = {"Physics": [doc("abc12-de345", "Quarks & <Leptons>")]}

    html = newsletter.render_html(groups, START, END)

    assert "<h3>Physics</h3>" in html
    assert (
        '<li><a href="https://catalogue.library.cern/literature/abc12-de345">'
        "Quarks &amp; &lt;Leptons&gt;</a></li>"
    ) in html
    assert "more" not in html
    assert html.endswith(
        "<p><small>This is an automated message, please do not reply.</small></p>\n"
    )


def test_render_limits_results_per_topic():
    groups = {"Physics": [doc(f"p{i}", f"Book {i}") for i in range(5)]}

    html = newsletter.render_html(groups, START, END, limit=3)
    text = newsletter.render_text(groups, START, END, limit=3)

    assert html.count("<li><a ") == 3
    assert "<li>+ 2 more</li>" in html
    assert text.count("\n- ") == 3
    assert "+ 2 more" in text


def test_render_text():
    groups = {"Mathematics": [doc("m1", "Algebra")]}

    text = newsletter.render_text(groups, START, END)

    assert text == (
        "Latest books/e-books\n"
        "New in the CERN Library catalogue, 02 Nov 2026 – 08 Nov 2026.\n"
        "\n"
        "Mathematics:\n"
        "- Algebra: https://catalogue.library.cern/literature/m1\n"
        "\n"
        "This is an automated message, please do not reply.\n"
    )


@pytest.mark.parametrize(
    "today, expected",
    [
        (date(2026, 11, 9), (date(2026, 11, 2), date(2026, 11, 9))),  # Monday
        (date(2026, 11, 11), (date(2026, 11, 2), date(2026, 11, 9))),  # Wednesday
        (date(2026, 11, 15), (date(2026, 11, 2), date(2026, 11, 9))),  # Sunday
    ],
)
def test_previous_week(today, expected):
    assert newsletter.previous_week(today) == expected


def test_get_newsletter_week_prefers_week_start_param():
    assert newsletter.get_newsletter_week(
        week_start="2026-10-05", run_date=date(2026, 11, 9)
    ) == (date(2026, 10, 5), date(2026, 10, 12))


def test_get_newsletter_week_uses_the_week_before_the_run_date():
    # A scheduled run of Monday 9 Nov covers 2-8 Nov, even when it is retried later.
    expected = (date(2026, 11, 2), date(2026, 11, 9))
    assert newsletter.get_newsletter_week(run_date=date(2026, 11, 9)) == expected
    assert newsletter.get_newsletter_week(run_date=date(2026, 11, 9)) != (
        newsletter.get_newsletter_week(run_date=date(2026, 11, 16))
    )


def test_get_newsletter_week_without_run_date_uses_today():
    assert newsletter.get_newsletter_week() == newsletter.previous_week(date.today())


def test_render_lists_everything_without_limit():
    groups = {"Physics": [doc(f"p{i}", f"Book {i}") for i in range(30)]}

    html = newsletter.render_html(groups, START, END, limit=None)
    text = newsletter.render_text(groups, START, END, limit=None)

    assert html.count("<li><a ") == 30
    assert "more" not in html
    assert text.count("\n- ") == 30


def test_render_text_is_not_html_escaped():
    groups = {"Physics": [doc("p1", "Quarks & <Leptons>")]}

    text = newsletter.render_text(groups, START, END)

    assert "- Quarks & <Leptons>: " in text
