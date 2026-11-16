import pytest

from notifications import mailer


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


class FakeSMTP:
    instances = []

    def __init__(self, host, port, timeout=None):
        self.host, self.port = host, port
        self.actions = []
        FakeSMTP.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def starttls(self):
        self.actions.append("starttls")

    def login(self, user, password):
        self.actions.append(("login", user, password))

    def send_message(self, message):
        self.actions.append(("send", message["Subject"]))


class FakeConnection:
    def __init__(self, host, port, login=None, password=None, extra=None):
        self.host, self.port, self.login, self.password = host, port, login, password
        self.extra_dejson = extra or {}


def make_mailer(monkeypatch, connection):
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
