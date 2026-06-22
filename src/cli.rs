use std::env;
use std::path::Path;

pub enum CliAction {
    RunWithConfig(String),
    ExitAfterHelp,
}

pub fn resolve_config_arg(bin_name: &str, local_default: &str, example_default: &str) -> CliAction {
    match env::args().nth(1) {
        Some(arg) if arg == "-h" || arg == "--help" => {
            print_help(bin_name, local_default, example_default);
            CliAction::ExitAfterHelp
        }
        Some(arg) => CliAction::RunWithConfig(arg),
        None => {
            if Path::new(local_default).exists() {
                CliAction::RunWithConfig(local_default.to_string())
            } else {
                CliAction::RunWithConfig(example_default.to_string())
            }
        }
    }
}

fn print_help(bin_name: &str, local_default: &str, example_default: &str) {
    println!("Usage: {bin_name} [CONFIG_PATH]");
    println!();
    println!("If CONFIG_PATH is omitted, the program tries:");
    println!("  1. ./{local_default}");
    println!("  2. ./{example_default}");
}
