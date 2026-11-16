import logging
from datetime import date, timedelta

import pendulum
from airflow.sdk import Param, dag, task

from notifications import newsletter
from notifications.catalogue import LibraryCatalogue
from notifications.mailer import SmtpMailer, build_message

logger = logging.getLogger(__name__)


@dag(
    dag_id="library_newsletter",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Zurich"),
    schedule="0 8 * * 1",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["notifications", "library", "newsletter"],
    params={
        "execution_mode": Param(
            "Send",
            type="string",
            enum=["Dry Run", "Send"],
            description="Dry Run only prints the email in the logs. "
            "Send emails it to the recipients of the newsletter_smtp connection.",
        ),
        "week_start": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Optional Monday (YYYY-MM-DD) of the week to send. "
            "Empty: the scheduled week, or the previous full week for manual runs.",
        ),
    },
)
def library_newsletter_dag():
    @task(task_id="get_newsletter_week")
    def get_newsletter_week(**context) -> dict:
        start, end = newsletter.get_newsletter_week(
            week_start=context["params"]["week_start"],
            run_date=context["dag_run"].run_after.date(),
        )
        logger.info(f"Newsletter period: {start} to {end} (end excluded)")
        return {"start": start.isoformat(), "end": end.isoformat()}

    @task(task_id="fetch_new_documents")
    def fetch_new_documents(period: dict) -> list[dict]:
        catalogue = LibraryCatalogue()
        pids = catalogue.fetch_new_document_pids(
            date.fromisoformat(period["start"]), date.fromisoformat(period["end"])
        )
        if not pids:
            logger.info("No new items/e-items in the period.")
            return []
        return catalogue.fetch_documents(pids)

    @task(task_id="build_newsletter")
    def build_newsletter(period: dict, documents: list[dict]) -> dict:
        """Groups the documents by topic and create email."""
        if not documents:
            logger.info("No documents to publish. Skipping.")
            return {}

        start, end = (date.fromisoformat(period[k]) for k in ("start", "end"))
        groups = newsletter.group_by_topic(documents)

        return {
            "subject": newsletter.build_subject(start, end),
            "html": newsletter.render_html(groups, start, end),
            "text": newsletter.render_text(groups, start, end),
        }

    @task(task_id="send_newsletter")
    def send_newsletter(content: dict, **context):
        params = context["params"]
        mode = params["execution_mode"]

        if not content:
            logger.info("Nothing to send.")
            return

        if mode == "Dry Run":
            logger.info(
                "Dry-run mode active. Email not sent. Content:\n" + content["text"]
            )
            return

        mailer = SmtpMailer()
        message = build_message(
            subject=content["subject"],
            html=content["html"],
            text=content["text"],
            sender=mailer.sender,
            recipients=mailer.recipients,
        )
        mailer.send(message)

    period = get_newsletter_week()
    documents = fetch_new_documents(period)
    content = build_newsletter(period, documents)
    send_newsletter(content)


library_newsletter_dag_instance = library_newsletter_dag()
