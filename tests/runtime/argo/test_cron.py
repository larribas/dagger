from dagger.runtime.argo.cron import Cron, CronConcurrencyPolicy


def test__cron__representation():
    extra_spec_options = {"extra": "options"}
    cron = Cron(
        schedule="0 1 * * 0",
        starting_deadline_seconds=2,
        concurrency_policy=CronConcurrencyPolicy.REPLACE,
        timezone="Europe/Madrid",
        successful_jobs_history_limit=4,
        failed_jobs_history_limit=6,
        extra_spec_options=extra_spec_options,
    )
    assert (
        repr(cron)
        == f"Cron(schedule=0 1 * * 0, starting_deadline_seconds=2, concurrency_policy=Replace, timezone=Europe/Madrid, successful_jobs_history_limit=4, failed_jobs_history_limit=6, extra_spec_options={repr(extra_spec_options)})"
    )


def test__cron__eq():
    schedule = "0 1 * * 0"
    timezone = "Europe/Madrid"
    concurrency_policy = CronConcurrencyPolicy.REPLACE
    extra_spec_options = {"extra": "options"}
    cron = Cron(
        schedule=schedule,
        starting_deadline_seconds=2,
        concurrency_policy=concurrency_policy,
        timezone=timezone,
        successful_jobs_history_limit=4,
        failed_jobs_history_limit=6,
        extra_spec_options=extra_spec_options,
    )
    assert cron != Cron(schedule)
    assert cron == Cron(
        schedule=schedule,
        starting_deadline_seconds=2,
        concurrency_policy=concurrency_policy,
        timezone=timezone,
        successful_jobs_history_limit=4,
        failed_jobs_history_limit=6,
        extra_spec_options=extra_spec_options,
    )
