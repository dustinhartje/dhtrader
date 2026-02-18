#!/usr/bin/env python3
"""Environment management utilities for MongoDB configuration.

This module provides functions to detect, switch, and display the currently
active MongoDB environment. It's designed to be portable and can be used
across multiple projects.

Environment files follow the pattern: mongo.env.<env>
The environment name is the last segment of the filename.
"""

import os
import sys
import argparse


def get_script_dir(depth=0):
    """Get the directory of the calling script.

    Args:
        depth: Stack depth. 0 = caller of this function,
               1 = caller's caller, etc.
    """
    frame = sys._getframe(depth + 1)
    return os.path.dirname(os.path.abspath(frame.f_globals['__file__']))


def list_environment_files(script_dir=None):
    """List available environment files.

    Args:
        script_dir: Directory containing mongo.env files. If None, uses the
                    current working directory.

    Returns:
        dict: Mapping of environment name to file path
    """
    if script_dir is None:
        script_dir = os.getcwd()

    env_files = {}
    try:
        for name in sorted(os.listdir(script_dir)):
            if not name.startswith('mongo.env.'):
                continue
            path = os.path.join(script_dir, name)
            if not os.path.isfile(path):
                continue
            env_name = name.split('.')[-1]
            env_files[env_name] = path
    except Exception:
        pass

    return env_files


def available_environments(script_dir=None):
    """Return a sorted list of available environment names."""
    return sorted(list_environment_files(script_dir).keys())


def detect_environment(script_dir=None):
    """Detect which environment is currently active.

    Args:
        script_dir: Directory containing mongo.env files. If None, uses the
                    current working directory.

    Returns:
        str: Environment name, or 'unknown'
    """
    if script_dir is None:
        script_dir = os.getcwd()

    mongo_env_path = os.path.join(script_dir, 'mongo.env')
    env_files = list_environment_files(script_dir)

    if not os.path.exists(mongo_env_path):
        return 'unknown'

    try:
        with open(mongo_env_path, 'r') as f:
            current_content = f.read()

        for env_name, path in env_files.items():
            try:
                with open(path, 'r') as f:
                    if current_content == f.read():
                        return env_name
            except Exception:
                continue
    except Exception:
        pass

    return 'unknown'


def read_environment(script_dir=None):
    """Read the contents of the current mongo.env file.

    Args:
        script_dir: Directory containing mongo.env. If None, uses current
                    working directory.

    Returns:
        str: Contents of mongo.env, or empty string if file not found
    """
    if script_dir is None:
        script_dir = os.getcwd()

    mongo_env_path = os.path.join(script_dir, 'mongo.env')
    try:
        with open(mongo_env_path, 'r') as f:
            return f.read()
    except Exception:
        return ''


def display_environment(env, script_dir=None):
    """Display details of the current environment.

    Args:
        env: Environment name or 'unknown'
        script_dir: Directory containing mongo.env files
    """
    print("=" * 42)
    print(f"Current MongoDB Environment: {env}")
    print("=" * 42)
    content = read_environment(script_dir)
    print(content, end='')
    print("=" * 42)


def switch_environment(env, script_dir=None):
    """Switch to a different environment.

    Args:
        env: Target environment name
        script_dir: Directory containing mongo.env files. If None, uses current
                    working directory.

    Returns:
        bool: True if successful, False otherwise
    """
    if script_dir is None:
        script_dir = os.getcwd()
    env_files = list_environment_files(script_dir)
    source = env_files.get(env)
    if source is None:
        print(f"Error: Invalid environment '{env}'")
        return False

    dest = os.path.join(script_dir, 'mongo.env')

    try:
        with open(source, 'r') as f:
            content = f.read()
        with open(dest, 'w') as f:
            f.write(content)
        return True
    except Exception:
        print(f"Error: Failed to switch to '{env}' environment")
        return False


def check_environment_and_prompt(script_dir=None):
    """Check current environment and prompt if not production.

    Args:
        script_dir: Directory containing mongo.env files. If None, uses the
                    directory of the script calling this function.

    Returns:
        bool: True if user confirms or environment is prod, False if aborted
    """
    if script_dir is None:
        script_dir = get_script_dir(depth=1)

    current_env = detect_environment(script_dir)

    # Always allow prod without prompting
    if current_env == 'prod':
        return True

    print(f"\nWarning: Running in '{current_env}' environment")

    prompt_fn = None
    try:
        from dhtrader.dhutil import prompt_yn
        prompt_fn = prompt_yn
    except Exception:
        prompt_fn = None

    if prompt_fn is not None:
        try:
            response = prompt_fn("Continue with current environment?")
        except EOFError:
            response = False
    else:
        try:
            response_str = input("Continue with current environment? (Y/N): ")
            response = response_str.strip().lower() in ['y', 'yes']
        except EOFError:
            response = False

    if not response:
        print("Aborted.")
        sys.exit(1)

    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Manage MongoDB environment configuration',
        epilog=('%(prog)s [ENV|detect|display|list]\n'
                'ENV     switch to environment in mongo.env.ENV\n'
                'detect  print active environment name\n'
                'display show active environment and contents\n'
                'list    list available environments'),
        formatter_class=argparse.RawTextHelpFormatter,
        usage=argparse.SUPPRESS,
    )
    parser.add_argument('action',
                        nargs='?',
                        default=None,
                        help=('Environment to switch to or action '
                              '(detect, display, list)'))

    args = parser.parse_args()
    script_dir = os.getcwd()

    # Default: display for human review
    if args.action is None or args.action == 'display':
        current_env = detect_environment(script_dir)
        display_environment(current_env, script_dir)
    # Identify and print the detected environment name for automation
    elif args.action == 'detect':
        current_env = detect_environment(script_dir)
        print(current_env)
    # Display available environments
    elif args.action == 'list':
        envs = available_environments(script_dir)
        if envs:
            print("\n".join(envs))
        else:
            print("No environments found")
    # Switch to a specific mongo environment
    else:
        if switch_environment(args.action, script_dir):
            print(f"Switched to '{args.action}' environment")
            display_environment(args.action, script_dir)
        else:
            print(f"Error: Failed to switch to '{args.action}' environment")
            envs = available_environments(script_dir)
            if envs:
                print("Available environments:")
                print("\n".join(envs))
            sys.exit(1)
