use std::time::Duration;
use std::{io, thread};

use crossterm::event::{read, KeyCode};
use crossterm::{execute, terminal};
use log::{debug, error, LevelFilter};
use merkle_trie_clock::models::{RowParam, ValueType};

use crate::global_syncer::TodoSyncer;
use crate::models::{TodoParam, TODO_TABLE};

mod global_syncer;
mod models;

const GROUP_ID: &str = "todo-app";

fn main() {
    // 初始化日志系统
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let node_name = TodoSyncer::global().lock().unwrap().node_name().to_string();
    let mut stdout = io::stdout();

    // Fire sync event
    thread::spawn(|| loop {
        {
            let mut s = TodoSyncer::global().lock().unwrap();
            {
                s.debug();
            }

            match s.sync(GROUP_ID, vec![], None) {
                Ok(messages) => {
                    debug!("Applied messages: {:#?}", messages);
                }
                Err(e) => {
                    error!("Failed to sync message: {}", e);
                }
            };
        }
        thread::sleep(Duration::from_secs(3));
    });

    loop {
        // 清屏
        execute!(stdout, terminal::Clear(terminal::ClearType::All)).unwrap();

        println!("Welcome, node name: {}", node_name);
        println!("-----------------------------------------");
        println!("1: List all todos");
        println!("2: Add todo");
        println!("3: Update todo");
        println!("4: Delete todo");
        println!("0: Exit");
        println!();

        match read_key() {
            KeyCode::Char('1') => show_tasks(),
            KeyCode::Char('2') => add_task(),
            KeyCode::Char('3') => update_task(),
            KeyCode::Char('4') => remove_task(),
            KeyCode::Char('0') => break,
            _ => println!("Invalid option!"),
        }

        // 等待任意键继续
        println!("\nPress ENTER to continue...");
        let _ = read_key();
        let _ = read_key();
    }
}

fn show_tasks() {
    let storage = TodoSyncer::global().lock().unwrap();
    let todos = storage.storage().items();
    todos
        .iter()
        .filter(|(_, v)| v.tombstone == 0)
        .for_each(|kv| {
            println!("Todo: {:?}", kv.1);
        })
}

fn add_task() {
    println!("Enter the todo item: [content, type]");
    let mut new_item = String::new();
    io::stdin()
        .read_line(&mut new_item)
        .expect("Failed to read line");

    let mut parts = new_item.split_whitespace();
    let (content, todo_type) = (parts.next(), parts.next());
    {
        let mut s = TodoSyncer::global().lock().unwrap();
        let res = s.insert(
            GROUP_ID,
            TODO_TABLE,
            vec![
                RowParam {
                    id: None,
                    column: TodoParam::Content.to_string(),
                    value_type: ValueType::String,
                    value: content.unwrap().to_string(),
                },
                RowParam {
                    id: None,
                    column: TodoParam::TodoType.to_string(),
                    value_type: ValueType::String,
                    value: todo_type.unwrap().to_string(),
                },
            ],
        );
        println!("\nInsert result: {:?}", res);
    }
}

fn update_task() {
    println!("Enter the todo item: [id, content, type]");
    let mut new_item = String::new();
    io::stdin()
        .read_line(&mut new_item)
        .expect("Failed to read line");

    let mut parts = new_item.split_whitespace();
    let (id, content, todo_type) = (parts.next(), parts.next(), parts.next());
    {
        let id = id.unwrap().to_string();
        let mut s = TodoSyncer::global().lock().unwrap();
        let res = s.update(
            GROUP_ID,
            TODO_TABLE,
            vec![
                RowParam {
                    id: Some(id.clone()),
                    column: TodoParam::Content.to_string(),
                    value_type: ValueType::String,
                    value: content.unwrap().to_string(),
                },
                RowParam {
                    id: Some(id),
                    column: TodoParam::TodoType.to_string(),
                    value_type: ValueType::String,
                    value: todo_type.unwrap().to_string(),
                },
            ],
        );
        println!("\nInsert result: {:?}", res);
    }
}

fn remove_task() {
    println!("Enter the index of the todo item to delete:");
    let mut index_input = String::new();
    io::stdin()
        .read_line(&mut index_input)
        .expect("Failed to read line");
    let index_input = index_input.trim();
    {
        let mut s = TodoSyncer::global().lock().unwrap();
        s.delete(GROUP_ID, TODO_TABLE, index_input).unwrap();
    }
    println!("\nDelete task: {}", index_input);
}

fn read_key() -> KeyCode {
    loop {
        if let Ok(crossterm::event::Event::Key(event)) = read() {
            return event.code;
        }
    }
}
