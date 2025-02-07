import os
import datetime

# Global list to store tasks
tasks = []

# Possible statuses for a task
STATUSES = ["Open", "Blocked", "In Progress", "Review", "Done"]

def handle_file(filename="tasks.txt"):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, filename)
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                f.write("")  # Create an empty file
        return file_path
    except Exception as e:
        print(f"Error handling file: {e}")
        return None

def load_tasks(filename="tasks.txt"):
    if os.path.exists(filename):
        with open(filename, "r") as file:
            for line in file:
                parts = line.strip().split("|")
                if len(parts) == 4:
                    task_name, status, created_at, status_updated_at = parts
                    tasks.append({
                        "task": task_name,
                        "status": status,
                        "created_at": created_at,
                        "status_updated_at": status_updated_at
                    })

def save_tasks(filename="tasks.txt"):
    with open(filename, "w") as file:
        for task in tasks:
            file.write(f"{task['task']}|{task['status']}|{task['created_at']}|{task['status_updated_at']}\n")

def display_menu():
    print("\n--- Tasks List Menu ---")
    print("1. View tasks")
    print("2. Add a new task")
    print("3. Edit a task")
    print("4. Change task status")
    print("5. Remove a task")
    print("6. Exit")

def view_tasks():
    if not tasks:
        print("No tasks found!")
    else:
        print("\nYour Tasks:")
        for index, task in enumerate(tasks, start=1):
            print(f"{index}. {task['task']} [{task['status']}]\n   Created: {task['created_at']}\n   Last Status Change: {task['status_updated_at']}")

def add_task():
    task_name = input("Enter a new task: ").strip()
    if task_name:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tasks.append({"task": task_name, "status": "Open", "created_at": now, "status_updated_at": now})
        print(f"Task '{task_name}' added successfully with status 'Open'.")

def update_task_status():
    view_tasks()
    if not tasks:
        print("No tasks available to update status.")
        return
    try:
        index = int(input("Enter the task number to update status: ")) - 1
        if 0 <= index < len(tasks):
            print("\nAvailable statuses:")
            for i, status in enumerate(STATUSES, start=1):
                print(f"{i}. {status}")
            status_choice = int(input("Choose a status number: ")) - 1
            if 0 <= status_choice < len(STATUSES):
                tasks[index]["status"] = STATUSES[status_choice]
                tasks[index]["status_updated_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"Task '{tasks[index]['task']}' status updated.")
    except ValueError:
        print("Please enter a valid number.")

def remove_task():
    view_tasks()
    if not tasks:
        return
    try:
        index = int(input("Enter the task number to remove: ")) - 1
        if 0 <= index < len(tasks):
            removed = tasks.pop(index)
            print(f"Task '{removed['task']}' removed successfully!")
    except ValueError:
        print("Please enter a valid number.")

def main():
    load_tasks()
    while True:
        display_menu()
        choice = input("Choose an option (1-6): ").strip()
        if choice == "1":
            view_tasks()
        elif choice == "2":
            add_task()
        elif choice == "3":
            print("Editing task functionality not implemented yet.")
        elif choice == "4":
            update_task_status()
        elif choice == "5":
            remove_task()
        elif choice == "6":
            save_tasks()
            print("Tasks saved, Goodbye!")
            break
if __name__ == "__main__":
    main()
