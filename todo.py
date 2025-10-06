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
    position: int = 0


TaskId = int
TaskList = list[Task]


class TaskRepository(Protocol):
    def list_tasks(self) -> TaskList: ...

    def add(self, title: str) -> Task | None: ...

    def toggle(self, task_id: TaskId) -> Task: ...

    def remove(self, task_id: TaskId) -> None: ...

    def completion_stats(self) -> tuple[int, int]: ...

    def reorder(self, ordered_ids: list[TaskId]) -> TaskList: ...


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

        task = Task(
            id=self._next_id,
            title=normalised,
            position=len(self._tasks),
        )
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
        self._reindex_positions()

    def completion_stats(self) -> tuple[int, int]:
        total = len(self._tasks)
        done = sum(task.done for task in self._tasks)
        return int(done), total

    def reorder(self, ordered_ids: list[TaskId]) -> TaskList:
        if len(ordered_ids) != len(self._tasks):
            raise ValueError("ordered_ids must include every task exactly once")

        id_to_task = {task.id: task for task in self._tasks}
        if set(ordered_ids) != set(id_to_task):
            raise ValueError("ordered_ids must include every task exactly once")

        self._tasks = [
            replace(id_to_task[task_id], position=index)
            for index, task_id in enumerate(ordered_ids)
        ]
        return self.list_tasks()

    def _reindex_positions(self) -> None:
        self._tasks = [
            replace(task, position=index) for index, task in enumerate(self._tasks)
        ]


class SQLiteTaskRepository:
    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._connection.row_factory = sqlite3.Row
        self._initialise()

    def list_tasks(self) -> TaskList:
        cursor = self._connection.execute(
            "SELECT id, title, done, position FROM tasks ORDER BY position, id"
        )
        rows = cursor.fetchall()
        return [self._row_to_task(row) for row in rows]

    def add(self, title: str) -> Task | None:
        normalised = _normalise_title(title)
        if not normalised:
            return None

        next_position_row = self._connection.execute(
            "SELECT COALESCE(MAX(position), -1) + 1 AS next_position FROM tasks"
        ).fetchone()
        next_position = int(next_position_row["next_position"])

        cursor = self._connection.execute(
            "INSERT INTO tasks (title, done, position) VALUES (?, 0, ?)",
            (normalised, next_position),
        )
        self._connection.commit()
        task_id = cursor.lastrowid
        return Task(id=task_id, title=normalised, done=False, position=next_position)

    def toggle(self, task_id: TaskId) -> Task:
        row = self._connection.execute(
            "SELECT id, title, done, position FROM tasks WHERE id = ?",
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
        return Task(
            id=row["id"],
            title=row["title"],
            done=bool(new_done),
            position=row["position"],
        )

    def remove(self, task_id: TaskId) -> None:
        self._connection.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        self._connection.commit()
        self._normalise_positions()

    def completion_stats(self) -> tuple[int, int]:
        row = self._connection.execute(
            "SELECT COUNT(*) AS total, COALESCE(SUM(done), 0) AS done FROM tasks"
        ).fetchone()
        total = int(row["total"])
        done = int(row["done"])
        return done, total

    def reorder(self, ordered_ids: list[TaskId]) -> TaskList:
        existing_rows = self._connection.execute(
            "SELECT id, position FROM tasks ORDER BY position, id"
        ).fetchall()
        existing_ids = [row["id"] for row in existing_rows]

        if len(existing_ids) != len(ordered_ids):
            raise ValueError("ordered_ids must include every task exactly once")

        if set(existing_ids) != set(ordered_ids):
            raise ValueError("ordered_ids must include every task exactly once")

        max_position = max((int(row["position"]) for row in existing_rows), default=-1)
        offset_base = max_position + 1
        temp_updates = [
            (offset_base + index, task_id)
            for index, task_id in enumerate(ordered_ids)
        ]
        final_updates = [
            (index, task_id) for index, task_id in enumerate(ordered_ids)
        ]
        self._connection.executemany(
            "UPDATE tasks SET position = ? WHERE id = ?",
            temp_updates,
        )
        self._connection.executemany(
            "UPDATE tasks SET position = ? WHERE id = ?",
            final_updates,
        )
        self._connection.commit()
        return self.list_tasks()

    def _initialise(self) -> None:
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                done INTEGER NOT NULL DEFAULT 0 CHECK(done IN (0, 1)),
                position INTEGER NOT NULL UNIQUE
            )
            """
        )
        self._connection.commit()
        self._ensure_position_column()
        self._normalise_positions()

    @staticmethod
    def _row_to_task(row: Row) -> Task:
        return Task(
            id=row["id"],
            title=row["title"],
            done=bool(row["done"]),
            position=int(row["position"]),
        )

    def _ensure_position_column(self) -> None:
        columns = self._connection.execute("PRAGMA table_info(tasks)").fetchall()
        has_position = any(column["name"] == "position" for column in columns)
        if not has_position:
            self._connection.execute("ALTER TABLE tasks ADD COLUMN position INTEGER")
            self._connection.commit()
            self._connection.execute("UPDATE tasks SET position = id")
            self._connection.commit()
        self._connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_position ON tasks(position)"
        )
        self._connection.commit()

    def _normalise_positions(self) -> None:
        rows = self._connection.execute(
            "SELECT id, position FROM tasks ORDER BY position, id"
        ).fetchall()
        ids = [row["id"] for row in rows]
        if not ids:
            return

        max_position = max((int(row["position"]) for row in rows), default=-1)
        offset_base = max_position + 1
        temp_updates = [
            (offset_base + index, task_id) for index, task_id in enumerate(ids)
        ]
        final_updates = [(index, task_id) for index, task_id in enumerate(ids)]
        self._connection.executemany(
            "UPDATE tasks SET position = ? WHERE id = ?",
            temp_updates,
        )
        self._connection.executemany(
            "UPDATE tasks SET position = ? WHERE id = ?",
            final_updates,
        )
        self._connection.commit()
