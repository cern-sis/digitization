from datetime import date

import pendulum
import pytest

from notifications import config, mailer


def test_build_message_has_text_and_html_parts():
    message = mailer.build_message(
        subject="Latest books/e-books",
        html="<p>Hello</p>",
        text="Hello",
        sender="library@cern.ch",
        recipients=["a@cern.ch", "b@cern.ch"],
    )

    assert message["Subject"] == "Latest books/e-books"
    assert message["From"] == "library@cern.ch"
    assert message["To"] == "a@cern.ch, b@cern.ch"
    assert message.get_body(("plain",)).get_content().strip() == "Hello"
    assert message.get_body(("html",)).get_content().strip() == "<p>Hello</p>"
    assert message["Message-ID"] is None


def test_build_message_sets_the_message_id():
    message = mailer.build_message(
        "S", "<p>x</p>", "x", "a@cern.ch", ["b@cern.ch"], message_id="<id@cern.ch>"
    )

    assert message["Message-ID"] == "<id@cern.ch>"


class FakeSMTP:
    instances = []
    fail_on = None

    def __init__(self, host, port, timeout=None):
        self.host, self.port = host, port
        self.actions = []
        FakeSMTP.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if self.fail_on == "quit":
            raise mailer.smtplib.SMTPResponseException(421, b"closing")
        return False

    def starttls(self):
        self.actions.append("starttls")

    def login(self, user, password):
        self.actions.append(("login", user, password))

    def send_message(self, message):
        if self.fail_on == "send":
            raise mailer.smtplib.SMTPServerDisconnected("lost")
        self.actions.append(("send", message["Subject"]))


class FakeConnection:
    def __init__(self, host, port, login=None, password=None, extra=None):
        self.host, self.port, self.login, self.password = host, port, login, password
        self.extra_dejson = extra or {}


def make_mailer(monkeypatch, connection, fail_on=None):
    monkeypatch.setattr(FakeSMTP, "fail_on", fail_on)
    monkeypatch.setattr(mailer.smtplib, "SMTP", FakeSMTP)
    monkeypatch.setattr(mailer.BaseHook, "get_connection", lambda conn_id: connection)
    return mailer.SmtpMailer()


def test_send_with_starttls_and_login(monkeypatch):
    smtp_mailer = make_mailer(
        monkeypatch,
        FakeConnection(
            "smtp.example",
            587,
            "u",
            "p",
            extra={"from": "library@cern.ch", "to": "test-library-newsletter@cern.ch"},
        ),
    )
    message = mailer.build_message(
        "S", "<p>x</p>", "x", smtp_mailer.sender, ["to@cern.ch"]
    )

    smtp_mailer.send(message)

    smtp = FakeSMTP.instances[-1]
    assert smtp_mailer.sender == "library@cern.ch"
    assert (smtp.host, smtp.port) == ("smtp.example", 587)
    assert smtp.actions == ["starttls", ("login", "u", "p"), ("send", "S")]


def test_send_without_auth_or_tls(monkeypatch):
    smtp_mailer = make_mailer(
        monkeypatch,
        FakeConnection(
            "localhost", 1025, extra={"starttls": False, "to": "to@cern.ch"}
        ),
    )
    message = mailer.build_message("S", "<p>x</p>", "x", "a@cern.ch", ["to@cern.ch"])

    smtp_mailer.send(message)

    assert FakeSMTP.instances[-1].actions == [("send", "S")]


def test_sender_defaults_to_login(monkeypatch):
    smtp_mailer = make_mailer(
        monkeypatch,
        FakeConnection(
            "smtp.example", None, "me@cern.ch", "p", extra={"to": "to@cern.ch"}
        ),
    )

    assert smtp_mailer.sender == "me@cern.ch"
    assert smtp_mailer.port == 587


def test_recipients_come_from_the_connection(monkeypatch):
    smtp_mailer = make_mailer(
        monkeypatch,
        FakeConnection("smtp.example", 587, extra={"to": "a@cern.ch, b@cern.ch,"}),
    )

    assert smtp_mailer.recipients == ["a@cern.ch", "b@cern.ch"]


def test_missing_recipients_fail(monkeypatch):
    with pytest.raises(ValueError, match="'to' extra"):
        make_mailer(monkeypatch, FakeConnection("smtp.example", 587))


def test_error_before_the_email_is_sent_fails(monkeypatch):
    smtp_mailer = make_mailer(
        monkeypatch, FakeConnection("h", 25, extra={"to": "to@cern.ch"}), "send"
    )
    message = mailer.build_message("S", "<p>x</p>", "x", "a@cern.ch", ["to@cern.ch"])

    with pytest.raises(mailer.smtplib.SMTPServerDisconnected):
        smtp_mailer.send(message)


def test_error_when_closing_after_the_email_is_sent_does_not_fail(monkeypatch):
    smtp_mailer = make_mailer(
        monkeypatch, FakeConnection("h", 25, extra={"to": "to@cern.ch"}), "quit"
    )
    message = mailer.build_message("S", "<p>x</p>", "x", "a@cern.ch", ["to@cern.ch"])

    smtp_mailer.send(message)

    assert FakeSMTP.instances[-1].actions[-1] == ("send", "S")


def test_message_id_is_fixed_per_week():
    first = mailer.message_id(date(2026, 11, 9), "library-newsletter@cern.ch")
    again = mailer.message_id(date(2026, 11, 9), "library-newsletter@cern.ch")

    assert first == again == "<library-newsletter-2026-11-09@cern.ch>"
    assert mailer.message_id(date(2026, 11, 16), "a@cern.ch") != first


def test_resend_message_id_is_fixed_per_run():
    resend_at = pendulum.datetime(2026, 11, 18, 11, 45, 3)

    assert (
        mailer.message_id(date(2026, 11, 9), "a@cern.ch", resend_at=resend_at)
        == "<library-newsletter-2026-11-09.resend-20261118T114503@cern.ch>"
    )


def test_add_to_history_records_the_send_and_drops_old_entries():
    history = {
        "<library-newsletter-2026-09-07@cern.ch>": "2026-09-14T08:00:05+02:00",
        "<library-newsletter-2026-11-02@cern.ch>": "2026-11-09T08:00:11+01:00",
    }
    now = pendulum.datetime(2026, 11, 16, 8, 0, 12, tz="Europe/Zurich")

    updated = mailer.add_to_history(
        history, "<library-newsletter-2026-11-09@cern.ch>", now, keep_days=60
    )

    assert updated == {
        "<library-newsletter-2026-11-02@cern.ch>": "2026-11-09T08:00:11+01:00",
        "<library-newsletter-2026-11-09@cern.ch>": "2026-11-16T08:00:12+01:00",
    }


def test_history_is_stored_as_json_in_the_variable(monkeypatch):
    stored = {}

    def fake_set(key, value, serialize_json=False):
        stored[key] = (value, serialize_json)

    def fake_get(key, default=None, deserialize_json=False):
        return stored.get(key, (default,))[0]

    monkeypatch.setattr(mailer.Variable, "get", fake_get)
    monkeypatch.setattr(mailer.Variable, "set", fake_set)

    assert mailer.load_history() == {}
    mailer.save_history({"<id@cern.ch>": "2026-11-16T08:00:12+01:00"})

    assert stored[config.SENT_HISTORY_VARIABLE][1] is True
    assert mailer.load_history() == {"<id@cern.ch>": "2026-11-16T08:00:12+01:00"}
