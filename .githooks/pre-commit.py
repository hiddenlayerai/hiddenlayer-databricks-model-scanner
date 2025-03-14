
import subprocess
import anthropic
import sys
import os
import re

def get_version_from_file(file_path):
    """Read and extract the version from the Go file."""
    try:
        with open(file_path, 'r') as file:
            content = file.read()
        
        version_match = re.search(r'const\s+Version\s*=\s*"([^"]+)"', content)
        if version_match:
            return version_match.group(1), content
        else:
            raise ValueError("Version pattern not found in the file")
    
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{file_path}' not found.")
    except Exception as e:
        raise Exception(f"Error reading file: {e}")


def update_version_in_file(file_path, new_version):
    """Update the version in the Go file."""
    try:
        # Get current version and file content
        current_version, content = get_version_from_file(file_path)
        
        # Replace version in content
        updated_content = re.sub(
            r'(const\s+Version\s*=\s*)"[^"]+"', 
            f'\\1"{new_version}"', 
            content
        )
        
        # Write updated content back to file
        with open(file_path, 'w') as file:
            file.write(updated_content)
        
        return current_version, new_version
    
    except Exception as e:
        raise Exception(f"Error updating version: {e}")


def main():

    file_path = "internal/utils/version.go"
    version, _ = get_version_from_file(file_path)
    print(f"Current version: {version}")

    diff_process = subprocess.run(
            ["git", "diff", "--staged"],
            capture_output=True, text=True
        )

    diff_output = diff_process.stdout
    print(f"Diff: {diff_output}")

    meta_prompt=f"Given the previous diff: section and that the current version is {version}, output the new recommended semver version bump, output only the new version number in the semver format. If the diff is empty or diff contains edits only to the version.go file you should recommend the current version as the new version. Otherwise, the new version can never be the same as the old version."


    client = anthropic.Anthropic(
        # defaults to os.environ.get("ANTHROPIC_API_KEY")
    )

    message = client.messages.create(
        model="claude-3-7-sonnet-20250219",
        max_tokens=20000,
        temperature=1,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": f"diff: {diff_output} Instructions: {meta_prompt}"
                    }
                ]
            }
        ]
    )

    print(f"recommended version: {message.content[0].text}")

    old_ver, new_ver = update_version_in_file(file_path, message.content[0].text)


    




if __name__ == "__main__":
    sys.exit(main())