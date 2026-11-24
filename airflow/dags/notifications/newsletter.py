"""Group the new documents by topic and build the newsletter content."""

from datetime import date, timedelta
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from notifications import config

templates = Environment(
    loader=FileSystemLoader(Path(__file__).parent / "templates"),
    autoescape=select_autoescape(["html"]),
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=True,
)


def classify(document: dict, topics=config.TOPICS) -> list[str]:
    """Titles of the topics a document belongs to, or ``[OTHER_TOPIC]`` if none."""
    subjects = [
        (str(s.get("value", "")), s.get("scheme")) for s in document.get("subjects", [])
    ]
    matched = [
        title
        for title, rules in topics
        if any(
            value.startswith(prefix) and (scheme is None or scheme == subject_scheme)
            for value, subject_scheme in subjects
            for prefix, scheme in rules
        )
    ]
    return matched or [config.OTHER_TOPIC]


def group_by_topic(
    documents: list[dict], topics=config.TOPICS
) -> dict[str, list[dict]]:
    order = [title for title, _ in topics] + [config.OTHER_TOPIC]
    groups = {title: {} for title in order}
    for document in documents:
        for title in classify(document, topics):
            groups[title].setdefault(document["pid"], document)

    return {
        title: sorted(docs.values(), key=lambda d: d.get("title", "").casefold())
        for title, docs in groups.items()
        if docs
    }


def previous_week(today: date) -> tuple[date, date]:
    this_monday = today - timedelta(days=today.weekday())
    return this_monday - timedelta(days=7), this_monday


def get_newsletter_week(
    week_start: str = None, run_date: date = None
) -> tuple[date, date]:
    """Period covered by a run, as [start, end).

    The ``week_start`` param (``YYYY-MM-DD``, 7 days from that day) if given,
    otherwise the full week before the date the run was scheduled or triggered.
    """
    if week_start:
        start = date.fromisoformat(week_start)
        return start, start + timedelta(days=7)
    return previous_week(run_date or date.today())


def _period_label(start: date, end: date) -> str:
    last_day = end - timedelta(days=1)
    return f"{start:%d %b %Y} – {last_day:%d %b %Y}"


def build_subject(start: date, end: date) -> str:
    return f"{config.EMAIL_TITLE} ({_period_label(start, end)})"


def _shown_and_hidden(docs: list[dict], limit: int = None):
    """Documents to list and how many are left out. ``limit=None`` lists all of them."""
    if limit is None:
        return docs, 0
    return docs[:limit], max(len(docs) - limit, 0)


def _template_context(
    groups: dict[str, list[dict]], start: date, end: date, limit: int
) -> dict:
    topics = []
    for title, docs in groups.items():
        shown, hidden = _shown_and_hidden(docs, limit)
        documents = [
            {
                "title": doc.get("title", ""),
                "url": doc["url"],
            }
            for doc in shown
        ]
        topics.append({"title": title, "documents": documents, "hidden": hidden})
    return {
        "title": config.EMAIL_TITLE,
        "period": _period_label(start, end),
        "footer": config.EMAIL_FOOTER,
        "topics": topics,
    }


def render_html(
    groups: dict[str, list[dict]],
    start: date,
    end: date,
    limit: int = config.MAX_RESULTS_PER_TOPIC,
) -> str:
    context = _template_context(groups, start, end, limit)
    return templates.get_template("newsletter.html").render(context)


def render_text(
    groups: dict[str, list[dict]],
    start: date,
    end: date,
    limit: int = config.MAX_RESULTS_PER_TOPIC,
) -> str:
    context = _template_context(groups, start, end, limit)
    return templates.get_template("newsletter.txt").render(context)
