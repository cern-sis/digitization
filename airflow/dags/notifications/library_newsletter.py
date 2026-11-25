import logging
from datetime import date, timedelta

import pendulum
from airflow.sdk import Param, dag, task

from notifications import config, mailer, newsletter
from notifications.catalogue import LibraryCatalogue

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
        "force_resend": Param(
            False,
            type="boolean",
            description="Send the newsletter even if it was already sent.",
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
        logger.info("Newsletter period: %s to %s (end excluded)", start, end)
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
        unsubscribe_url = mailer.unsubscribe_url()

        return {
            "subject": newsletter.build_subject(start, end),
            "html": newsletter.render_html(
                groups, start, end, unsubscribe_url=unsubscribe_url
            ),
            "text": newsletter.render_text(
                groups, start, end, unsubscribe_url=unsubscribe_url
            ),
        }

    @task(task_id="send_newsletter")
    def send_newsletter(period: dict, content: dict, **context):
        params = context["params"]
        mode = params["execution_mode"]

        if not content:
            logger.info("Nothing to send.")
            return

        if mode == "Dry Run":
            logger.info(
                "Dry-run mode active. Email not sent. Content:\n%s", content["text"]
            )
            return

        smtp = mailer.SmtpMailer()
        message_id = mailer.message_id(
            date.fromisoformat(period["start"]),
            smtp.sender,
            resend_at=context["dag_run"].run_after if params["force_resend"] else None,
        )
        history = mailer.load_history()
        if message_id in history:
            logger.info(
                "%s was already sent on %s. Skipping. "
                "To send it again, trigger the DAG with force_resend.",
                message_id,
                history[message_id],
            )
            return

        message = mailer.build_message(
            subject=content["subject"],
            html=content["html"],
            text=content["text"],
            sender=smtp.sender,
            recipients=smtp.recipients,
            message_id=message_id,
        )
        smtp.send(message)

        try:
            mailer.save_history(
                mailer.add_to_history(
                    history, message_id, pendulum.now("Europe/Zurich")
                )
            )
        except Exception:
            logger.exception(
                "Could not record %s in the '%s' Variable.",
                message_id,
                config.SENT_HISTORY_VARIABLE,
            )

    period = get_newsletter_week()
    documents = fetch_new_documents(period)
    content = build_newsletter(period, documents)
    send_newsletter(period, content)


library_newsletter_dag_instance = library_newsletter_dag()
