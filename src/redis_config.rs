use crate::RedisCommand;
use std::collections::HashMap;

pub struct RedisConfig {
    pub config: HashMap<String, String>,
}

impl RedisConfig {
    pub fn parse_argument(mut args: Vec<String>) -> Self {
        args.remove(0);
        let mut config = HashMap::new();
        let mut args_iter = args.iter().peekable();
        let flags = ["--dir", "--dbfilename"];

        while let Some(arg) = args_iter.next() {
            if flags.contains(&arg.as_str()) {
                match args_iter.peek() {
                    Some(&next_args) if flags.contains(&next_args.as_str()) => {
                        panic!("Missing value for the flag");
                    }
                    Some(&next_args) => {
                        //let mut string_final = next_args.clone();
                        //
                        //if arg.as_str() == "--dir" && !next_args.ends_with("/") {
                        //    string_final.push('/');
                        //}

                        config.insert(arg.clone(), next_args.clone());
                    }

                    None => {
                        panic!("Missing value for the flag");
                    }
                }
            }
        }
        println!("{:?}", config);
        return Self { config };
    }

    pub fn set_config(&mut self, command: &RedisCommand) {}

    pub fn get_config(&self, command: &RedisCommand) -> String {
        // config get dir

        let key = command.str_cmd[2].clone(); // get the key

        let value = match self.config.get(&format!("--{}", &key)) {
            Some(val) => val.clone(),
            None => "".to_string(),
        };

        let lst_str = vec![key, value];
        let base_str = format!("*{}\r\n", lst_str.len());

        let formatted_item = lst_str
            .iter()
            .map(|item| format!("${}\r\n{}\r\n", item.len(), item))
            .collect::<String>();

        return base_str + &formatted_item;
    }
}
