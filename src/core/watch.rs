use std::time::{Duration, SystemTime};

pub struct StopWatch {
    pub current_task: Option<(String, SystemTime, Duration, bool)>,
    pub tasks: Vec<(String, SystemTime, Duration, bool)>,
}

impl StopWatch {
    pub fn new() -> Self {
        Self {
            current_task: None,
            tasks: Vec::new(),
        }
    }

    pub fn start(&mut self, name: &str) {
        self.current_task = Some((
            name.to_owned(),
            SystemTime::now(),
            Duration::new(0, 0),
            false,
        ));
    }

    pub fn stop(&mut self) {
        if let Some(task) = self.current_task.as_mut() {
            if task.3 {
                return;
            }

            task.3 = true;
            task.2 = SystemTime::now().duration_since(task.1).unwrap();

            self.tasks.push(task.to_owned());
        }
    }

    pub fn get_total_tasks_secs(&self) -> f32 {
        self.tasks.iter().map(|task| task.2.as_secs_f32()).sum()
    }

    pub fn get_total_tasks_mills(&self) -> u128 {
        self.tasks.iter().map(|task| task.2.as_millis()).sum()
    }

    pub fn get_all_tasks_mills(&self) -> Vec<(String, u128)> {
        self.tasks
            .iter()
            .map(|task| (task.0.to_owned(), task.2.as_millis()))
            .collect()
    }

    pub fn get_all_tasks_secs(&self) -> Vec<(String, f32)> {
        self.tasks
            .iter()
            .map(|task| (task.0.to_owned(), task.2.as_secs_f32()))
            .collect()
    }

    pub fn get_last_task_mills(&self) -> u128 {
        if let Some(task) = self.current_task.as_ref() {
            if task.3 {
                return task.2.as_millis();
            } else {
                0
            }
        } else {
            0
        }
    }

    pub fn print_last_task_mills(&self) {
        if let Some(task) = self.current_task.as_ref() {
            let time = self.get_last_task_mills();

            println!("{} 任务耗时: {}ms", task.0, time);
        }
    }

    pub fn print_last_task_secs(&self) {
        if let Some(task) = self.current_task.as_ref() {
            let time = self.get_total_tasks_secs();

            println!("{} 任务耗时: {}s", task.0, time);
        }
    }

    pub fn print_all_task_secs(&self) {
        for (name, sec) in self.get_all_tasks_secs() {
            println!("{} 任务耗时: {}s", name, sec);
        }
    }

    pub fn print_all_task_mils(&self) {
        for (name, mils) in self.get_all_tasks_mills() {
            println!("{} 任务耗时: {}ms", name, mils);
        }
    }
}
