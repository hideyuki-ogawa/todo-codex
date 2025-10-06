from __future__ import annotations

import sqlite3
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

from todo import TaskId, TaskList, TaskRepository, SQLiteTaskRepository


st.set_page_config(page_title="Todo Tracker", layout="centered")

DATABASE_PATH = Path("todo.db")

task_reorder_component = components.declare_component(
    "task_reorder", path=str(Path(__file__).parent / "components" / "task_reorder")
)


def render_reorder(tasks: TaskList) -> list[int] | None:
    items = [
        {"id": task.id, "title": task.title, "done": task.done}
        for task in tasks
    ]
    return task_reorder_component(items=items, key="task-reorder", default=None)


@st.cache_resource
def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DATABASE_PATH, check_same_thread=False)


@st.cache_resource
def get_repository() -> TaskRepository:
    return SQLiteTaskRepository(get_connection())


def handle_add(task_title: str) -> None:
    repository = get_repository()
    repository.add(task_title)


def handle_toggle(task_id: int) -> None:
    repository = get_repository()
    repository.toggle(TaskId(task_id))


def handle_remove(task_id: int) -> None:
    repository = get_repository()
    repository.remove(TaskId(task_id))
    st.session_state.pop(f"task-{task_id}", None)


st.title("Streamlit Todo App")
st.caption("Track tasks in your browser. Data persists locally in SQLite.")

with st.form("task-form", clear_on_submit=True):
    new_task = st.text_input(
        "What needs to be done?",
        key="new_task",
        placeholder="Type a task and press Enter",
    )
    submitted = st.form_submit_button("Add task", use_container_width=True)
    if submitted:
        handle_add(new_task)

repository = get_repository()
current_tasks: TaskList = repository.list_tasks()

if not current_tasks:
    st.success("All clear! Add your first task above.")
else:
    done_count, total_count = repository.completion_stats()
    st.write(f"**{done_count} / {total_count} tasks completed**")

    st.caption("Drag to rearrange tasks and update their priority order.")

    reordered_ids = render_reorder(current_tasks)
    if reordered_ids is not None and len(reordered_ids) == len(current_tasks):
        current_order = [task.id for task in current_tasks]
        if reordered_ids != current_order:
            repository.reorder(reordered_ids)
            st.experimental_rerun()

    for task in current_tasks:
        cols = st.columns([0.85, 0.15])
        with cols[0]:
            st.checkbox(
                label=task.title,
                value=task.done,
                key=f"task-{task.id}",
                on_change=handle_toggle,
                args=(task.id,),
            )
        with cols[1]:
            st.button(
                "Remove",
                key=f"remove-{task.id}",
                use_container_width=True,
                on_click=handle_remove,
                args=(task.id,),
            )
