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


def test_tasks_retry_on_transient_errors():
    for task in dag.tasks:
        assert task.retries == 2
