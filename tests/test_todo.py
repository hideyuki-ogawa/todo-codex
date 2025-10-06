from __future__ import annotations

import sqlite3
from collections.abc import Generator
from typing import Callable

import pytest

from todo import (
    InMemoryTaskRepository,
    SQLiteTaskRepository,
    Task,
    TaskRepository,
)


RepoFactory = Callable[[], TaskRepository]


def _sqlite_repo() -> Generator[TaskRepository, None, None]:
    connection = sqlite3.connect(":memory:")
    try:
        yield SQLiteTaskRepository(connection)
    finally:
        connection.close()


@pytest.fixture(params=[InMemoryTaskRepository, _sqlite_repo])
def repo(request: pytest.FixtureRequest) -> Generator[TaskRepository, None, None]:
    factory: RepoFactory | Callable[[], Generator[TaskRepository, None, None]] = request.param
    if factory is _sqlite_repo:
        yield from factory()
    else:
        yield factory()


def test_add_task_trims_title_and_returns_incremented_ids(repo: TaskRepository) -> None:
    first = repo.add("  write doc  ")
    second = repo.add("file bugs")

    assert isinstance(first, Task)
    assert isinstance(second, Task)
    assert first.title == "write doc"
    assert second.title == "file bugs"
    assert first.done is False
    assert second.done is False
    assert second.id > first.id


def test_add_task_ignores_blank_input(repo: TaskRepository) -> None:
    result = repo.add("   ")
    assert result is None
    assert repo.list_tasks() == []


def test_toggle_task_flips_done_state(repo: TaskRepository) -> None:
    added = repo.add("ship")
    assert added is not None
    flipped = repo.toggle(added.id)
    assert flipped.done is True
    reverted = repo.toggle(added.id)
    assert reverted.done is False


def test_remove_task_drops_only_target(repo: TaskRepository) -> None:
    one = repo.add("ship")
    two = repo.add("test")
    three = repo.add("deploy")

    repo.remove(two.id)

    remaining_ids = [task.id for task in repo.list_tasks()]
    assert remaining_ids == [one.id, three.id]


def test_tasks_are_returned_in_position_order(repo: TaskRepository) -> None:
    first = repo.add("ship")
    second = repo.add("test")
    third = repo.add("deploy")

    assert first is not None and second is not None and third is not None

    titles = [task.title for task in repo.list_tasks()]
    assert titles == ["ship", "test", "deploy"]

    positions = [task.position for task in repo.list_tasks()]
    assert positions == [0, 1, 2]


def test_reorder_updates_positions(repo: TaskRepository) -> None:
    first = repo.add("ship")
    second = repo.add("test")
    third = repo.add("deploy")

    assert first is not None and second is not None and third is not None

    repo.reorder([third.id, first.id, second.id])

    tasks = repo.list_tasks()
    assert [task.title for task in tasks] == ["deploy", "ship", "test"]
    assert [task.position for task in tasks] == [0, 1, 2]


def test_completion_stats_counts_done_tasks(repo: TaskRepository) -> None:
    first = repo.add("ship")
    second = repo.add("test")
    assert first is not None and second is not None

    repo.toggle(first.id)

    done, total = repo.completion_stats()
    assert done == 1
    assert total == 2
