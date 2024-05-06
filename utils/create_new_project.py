import os

def create_repo_structure(project_name, path=None):
    # Set project directory path
    if path:
        project_dir = os.path.join(path, project_name)
    else:
        project_dir = os.path.join(os.getcwd(), project_name)
    
    # Create project directory if it doesn't exist
    if not os.path.exists(project_dir):
        os.makedirs(project_dir)

        # Create subdirectories
        subdirectories = {
            'queries': ['views', 'materialized'],
            'dashboards': [],
            'docs': []
        }
        for directory, subdirs in subdirectories.items():
            dir_path = os.path.join(project_dir, directory)
            os.makedirs(dir_path)
            for subdir in subdirs:
                subdir_path = os.path.join(dir_path, subdir)
                os.makedirs(subdir_path)

        # Create README.md
        readme_path = os.path.join(project_dir, 'README.md')
        with open(readme_path, 'w') as f:
            f.write(f'# {project_name}\n\nThis is the main README file for the {project_name} project.')
    else:
        print(f"Directory '{project_name}' already exists at {project_dir}. Nothing was created.")

if __name__ == "__main__":
    project_name = input("Enter project name: ")
    path = input("Enter directory path (leave empty for current directory): ").strip()
    if path:
        create_repo_structure(project_name, path)
    else:
        create_repo_structure(project_name)
