import pendulum

from notifications import library_newsletter
from notifications.library_newsletter import library_newsletter_dag_instance as dag


def test_dag_loads_with_expected_tasks():
    assert dag.dag_id == "library_newsletter"
    assert set(dag.task_ids) == {
        "get_newsletter_week",
        "fetch_new_documents",
        "build_newsletter",
        "send_newsletter",
    }
    assert dag.params["execution_mode"] == "Send"
    assert dag.params.get_param("execution_mode").schema["enum"] == ["Dry Run", "Send"]
    assert dag.params["force_resend"] is False


def test_tasks_retry_on_transient_errors():
    for task in dag.tasks:
        assert task.retries == 2


class FakeMailer:
    sender = "library-newsletter@cern.ch"
    recipients = ["library-newsletter@cern.ch"]
    sent = []

    def send(self, message):
        FakeMailer.sent.append(message)


class FakeDagRun:
    run_after = pendulum.datetime(2026, 11, 18, 11, 45, 3)


def run_send(monkeypatch, history, force_resend=False):
    saved = []
    FakeMailer.sent = []
    monkeypatch.setattr(library_newsletter.mailer, "SmtpMailer", FakeMailer)
    monkeypatch.setattr(library_newsletter.mailer, "load_history", lambda: history)
    monkeypatch.setattr(library_newsletter.mailer, "save_history", saved.append)

    dag.get_task("send_newsletter").python_callable(
        {"start": "2026-11-09", "end": "2026-11-16"},
        {"subject": "S", "html": "<p>x</p>", "text": "x"},
        params={"execution_mode": "Send", "force_resend": force_resend},
        dag_run=FakeDagRun(),
    )
    return saved


def test_send_records_the_message_id(monkeypatch):
    saved = run_send(monkeypatch, {})

    assert len(FakeMailer.sent) == 1
    message_id = FakeMailer.sent[0]["Message-ID"]
    assert message_id == "<library-newsletter-2026-11-09@cern.ch>"
    assert list(saved[0]) == [message_id]


def test_send_skips_a_newsletter_already_sent(monkeypatch):
    history = {"<library-newsletter-2026-11-09@cern.ch>": "2026-11-16T08:00:12+01:00"}

    saved = run_send(monkeypatch, history)

    assert FakeMailer.sent == []
    assert saved == []


def test_force_resend_sends_again_with_its_own_message_id(monkeypatch):
    history = {"<library-newsletter-2026-11-09@cern.ch>": "2026-11-16T08:00:12+01:00"}

    saved = run_send(monkeypatch, history, force_resend=True)

    resend_id = "<library-newsletter-2026-11-09.resend-20261118T114503@cern.ch>"
    assert FakeMailer.sent[0]["Message-ID"] == resend_id
    assert set(saved[0]) == set(history) | {resend_id}


def test_retry_of_a_forced_resend_does_not_send_again(monkeypatch):
    history = {
        "<library-newsletter-2026-11-09@cern.ch>": "2026-11-16T08:00:12+01:00",
        "<library-newsletter-2026-11-09.resend-20261118T114503@cern.ch>": (
            "2026-11-18T12:45:04+01:00"
        ),
    }

    saved = run_send(monkeypatch, history, force_resend=True)

    assert FakeMailer.sent == []
    assert saved == []
