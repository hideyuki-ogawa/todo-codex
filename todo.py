from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from sqlite3 import Connection, Row
from typing import Protocol


@dataclass(frozen=True, slots=True)
class Task:
    id: int
    title: str
    done: bool = False


TaskId = int
TaskList = list[Task]


class TaskRepository(Protocol):
    def list_tasks(self) -> TaskList: ...

    def add(self, title: str) -> Task | None: ...

    def toggle(self, task_id: TaskId) -> Task: ...

    def remove(self, task_id: TaskId) -> None: ...

    def completion_stats(self) -> tuple[int, int]: ...


def _normalise_title(title: str) -> str:
    return title.strip()


class InMemoryTaskRepository:
    def __init__(self) -> None:
        self._tasks: TaskList = []
        self._next_id: TaskId = 0

    def list_tasks(self) -> TaskList:
        return list(self._tasks)

    def add(self, title: str) -> Task | None:
        normalised = _normalise_title(title)
        if not normalised:
            return None

        task = Task(id=self._next_id, title=normalised)
        self._tasks.append(task)
        self._next_id += 1
        return task

    def toggle(self, task_id: TaskId) -> Task:
        for index, task in enumerate(self._tasks):
            if task.id == task_id:
                updated = replace(task, done=not task.done)
                self._tasks[index] = updated
                return updated
        raise ValueError(f"Task with id {task_id} not found")

    def remove(self, task_id: TaskId) -> None:
        self._tasks = [task for task in self._tasks if task.id != task_id]

    def completion_stats(self) -> tuple[int, int]:
        total = len(self._tasks)
        done = sum(task.done for task in self._tasks)
        return int(done), total


class SQLiteTaskRepository:
    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._connection.row_factory = sqlite3.Row
        self._initialise()

    def list_tasks(self) -> TaskList:
        cursor = self._connection.execute(
            "SELECT id, title, done FROM tasks ORDER BY id"
        )
        rows = cursor.fetchall()
        return [self._row_to_task(row) for row in rows]

    def add(self, title: str) -> Task | None:
        normalised = _normalise_title(title)
        if not normalised:
            return None

        cursor = self._connection.execute(
            "INSERT INTO tasks (title, done) VALUES (?, 0)",
            (normalised,),
        )
        self._connection.commit()
        task_id = cursor.lastrowid
        return Task(id=task_id, title=normalised, done=False)

    def toggle(self, task_id: TaskId) -> Task:
        row = self._connection.execute(
            "SELECT id, title, done FROM tasks WHERE id = ?",
            (task_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"Task with id {task_id} not found")

        new_done = 0 if row["done"] else 1
        self._connection.execute(
            "UPDATE tasks SET done = ? WHERE id = ?",
            (new_done, task_id),
        )
        self._connection.commit()
        return Task(id=row["id"], title=row["title"], done=bool(new_done))

    def remove(self, task_id: TaskId) -> None:
        self._connection.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        self._connection.commit()

    def completion_stats(self) -> tuple[int, int]:
        row = self._connection.execute(
            "SELECT COUNT(*) AS total, COALESCE(SUM(done), 0) AS done FROM tasks"
        ).fetchone()
        total = int(row["total"])
        done = int(row["done"])
        return done, total

    def _initialise(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                done INTEGER NOT NULL DEFAULT 0 CHECK(done IN (0, 1))
            )
            """
        )
        self._connection.commit()

    @staticmethod
    def _row_to_task(row: Row) -> Task:
        return Task(id=row["id"], title=row["title"], done=bool(row["done"]))
